// Package syncrun is the weekly one-shot: discover parts on Drive via rclone,
// fetch one at a time, ingest, clean up, report (PLAN.md 3.2).
package syncrun

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/ingest"
	"gphoto-sync/internal/rclone"
	"gphoto-sync/internal/store"
	"gphoto-sync/internal/telegram"
)

// trashFolder is the sub-folder of the Takeout folder used to stage deletions.
const trashFolder = ".gphoto-sync-trash"

// Exit codes (PLAN.md 3.6).
const (
	ExitOK         = 0
	ExitFatal      = 1
	ExitPartial    = 2
	ExitOverdue    = 3
	ExitUnresolved = 4
)

// Worst combines exit codes with the precedence 1, 4, 2, 3.
func Worst(a, b int) int {
	rank := map[int]int{ExitOK: 0, ExitOverdue: 1, ExitPartial: 2, ExitUnresolved: 3, ExitFatal: 4}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

// AccountReport is the outcome for one account.
type AccountReport struct {
	Name           string
	PartsSeen      int
	PartsSettling  int
	PartsFetched   int
	Results        []*ingest.Result
	RemoteDeleted  int
	NewestExport   string
	Overdue        bool
	LooksComplete  []string
	Pruned         []string
	Unresolved     []store.FailedEntry
	Errors         []string
	Exit           int
	DuplicateNames []string
}

// Report is the outcome of a whole run.
type Report struct {
	Started  time.Time
	Finished time.Time
	Accounts []*AccountReport
	Exit     int
	Fatal    string
}

// Runner holds the dependencies.
type Runner struct {
	Cfg *config.Config
	St  *store.Store
	Log *slog.Logger
	Ing *ingest.Ingester
	RC  *rclone.Client
	Bot *telegram.Bot // nil when disabled
	Now func() time.Time
}

// Run processes the given accounts (all configured ones when empty).
func (r *Runner) Run(ctx context.Context, accounts []string, dryRun bool) *Report {
	if r.Now == nil {
		r.Now = time.Now
	}
	rep := &Report{Started: r.Now()}
	if len(accounts) == 0 {
		accounts = r.Cfg.AccountNames()
	}
	if r.Bot != nil && r.Cfg.Telegram.DrainUpdatesOnSync && !dryRun {
		if err := r.Bot.Drain(ctx); err != nil {
			r.Log.Warn("telegram drain failed", "err", err)
		}
	}
	for _, name := range accounts {
		ar := r.runAccount(ctx, name, dryRun)
		rep.Accounts = append(rep.Accounts, ar)
		rep.Exit = Worst(rep.Exit, ar.Exit)
		if ctx.Err() != nil {
			rep.Fatal = "interrupted"
			rep.Exit = ExitFatal
			break
		}
	}
	rep.Finished = r.Now()
	return rep
}

func (r *Runner) runAccount(ctx context.Context, name string, dryRun bool) *AccountReport {
	ar := &AccountReport{Name: name}
	acc, ok := r.Cfg.Accounts[name]
	if !ok {
		ar.fail(ExitFatal, "account not configured")
		return ar
	}
	var acct *store.Account
	var err error
	if dryRun {
		// read-only: never create the account row
		acct, err = r.St.GetAccount(ctx, name)
		if acct == nil && err == nil {
			acct = &store.Account{Name: name}
		}
	} else {
		acct, err = r.St.EnsureAccount(ctx, name, acc.RcloneRemote)
	}
	if err != nil {
		ar.fail(ExitFatal, err.Error())
		return ar
	}
	remoteName, _ := rclone.SplitRemote(acc.RcloneRemote)
	files, err := r.RC.List(ctx, acc.RcloneRemote)
	if err != nil {
		ar.fail(ExitFatal, "list remote: "+err.Error())
		return ar
	}
	// keep takeout parts; refuse duplicate names
	byName := map[string]int{}
	var parts []rclone.RemoteFile
	for _, f := range files {
		if ok, _ := path.Match(r.Cfg.Takeout.Pattern, f.Name); !ok {
			continue
		}
		if _, _, ok := ingest.ParsePartName(f.Name); !ok {
			r.Log.Warn("ignoring file that is not a takeout part", "name", f.Name)
			continue
		}
		byName[f.Name]++
		parts = append(parts, f)
	}
	var usable []rclone.RemoteFile
	for _, f := range parts {
		if byName[f.Name] > 1 {
			ar.DuplicateNames = append(ar.DuplicateNames, f.Name)
			continue
		}
		usable = append(usable, f)
	}
	if len(ar.DuplicateNames) > 0 {
		sort.Strings(ar.DuplicateNames)
		ar.Errors = append(ar.Errors, fmt.Sprintf("duplicate names on Drive, not touched: %s", strings.Join(ar.DuplicateNames, ", ")))
		ar.Exit = Worst(ar.Exit, ExitPartial)
	}
	sort.Slice(usable, func(i, j int) bool {
		ei, pi, _ := ingest.ParsePartName(usable[i].Name)
		ej, pj, _ := ingest.ParsePartName(usable[j].Name)
		if ei != ej {
			return ingest.ExportOlder(ei, ej)
		}
		return pi < pj
	})
	ar.PartsSeen = len(usable)
	settle := r.Cfg.SettleFor(name)
	inbox := filepath.Join(r.Cfg.Takeout.InboxDir, name)

	for _, f := range usable {
		if ctx.Err() != nil {
			break
		}
		exportID, partNo, _ := ingest.ParsePartName(f.Name)
		if ar.NewestExport == "" || ingest.ExportOlder(ar.NewestExport, exportID) {
			ar.NewestExport = exportID
		}
		arch, err := r.St.GetArchiveByRemoteID(ctx, f.ID)
		if err != nil {
			ar.fail(ExitFatal, err.Error())
			return ar
		}
		if arch == nil {
			arch, err = r.St.GetArchive(ctx, acct.ID, exportID, partNo)
			if err != nil {
				ar.fail(ExitFatal, err.Error())
				return ar
			}
		}
		if arch != nil && arch.Status == store.ArchiveIngested {
			if r.Cfg.Takeout.DeleteRemoteAfterIngest && !dryRun {
				if r.deleteRemote(ctx, ar, acc.RcloneRemote, f, arch) {
					ar.RemoteDeleted++
				}
			}
			continue
		}
		if arch != nil && arch.Status == store.ArchiveWithErrors {
			continue // reported via unresolved entries below
		}
		if r.Now().Sub(f.ModTime) < settle {
			ar.PartsSettling++
			r.Log.Info("part still settling", "name", f.Name, "age", r.Now().Sub(f.ModTime).Round(time.Minute))
			continue
		}
		if dryRun {
			r.Log.Info("would fetch", "name", f.Name, "size", f.Size)
			continue
		}
		if err := r.St.TouchExport(ctx, acct.ID, exportID, partNo); err != nil {
			ar.fail(ExitFatal, err.Error())
			return ar
		}
		local := filepath.Join(inbox, f.Name)
		arch, err = r.St.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: f.ID, Name: f.Name, ExportID: exportID, PartNo: partNo,
			Size: f.Size, MD5: f.MD5(), Status: store.ArchiveFetching, LocalPath: local})
		if err != nil {
			ar.fail(ExitFatal, err.Error())
			return ar
		}
		if err := r.fetch(ctx, remoteName, f, local); err != nil {
			_ = r.St.SetArchiveStatus(ctx, arch.ID, store.ArchiveFailed, "")
			ar.Errors = append(ar.Errors, fmt.Sprintf("%s: fetch: %v", f.Name, err))
			ar.Exit = Worst(ar.Exit, ExitPartial)
			_ = r.St.LogError(ctx, acct.ID, f.Name, "fetch: "+err.Error())
			continue
		}
		ar.PartsFetched++
		src, err := ingest.OpenZip(local)
		if err != nil {
			_ = r.St.SetArchiveStatus(ctx, arch.ID, store.ArchiveFailed, "")
			ar.Errors = append(ar.Errors, fmt.Sprintf("%s: %v", f.Name, err))
			ar.Exit = Worst(ar.Exit, ExitPartial)
			continue
		}
		res, err := r.Ing.IngestPart(ctx, src, ingest.PartMeta{Account: name, RcloneRemote: acc.RcloneRemote, ExportID: exportID, PartNo: partNo,
			Name: f.Name, LocalPath: local, RemoteID: f.ID, Size: f.Size, MD5: f.MD5()}, ingest.Options{})
		src.Close()
		if err != nil {
			ar.Errors = append(ar.Errors, fmt.Sprintf("%s: ingest: %v", f.Name, err))
			ar.Exit = Worst(ar.Exit, ExitPartial)
			continue
		}
		ar.Results = append(ar.Results, res)
		switch res.Status {
		case store.ArchivePartial:
			ar.Exit = Worst(ar.Exit, ExitPartial)
		case store.ArchiveWithErrors:
			ar.Exit = Worst(ar.Exit, ExitUnresolved)
		case store.ArchiveIngested:
			if !r.Cfg.Takeout.KeepArchives {
				if err := os.Remove(local); err != nil && !errors.Is(err, os.ErrNotExist) {
					r.Log.Warn("could not remove local part", "path", local, "err", err)
				}
			}
			if r.Cfg.Takeout.DeleteRemoteAfterIngest {
				if a2, _ := r.St.GetArchiveByRemoteID(ctx, f.ID); a2 != nil && r.deleteRemote(ctx, ar, acc.RcloneRemote, f, a2) {
					ar.RemoteDeleted++
				}
			}
		}
	}

	// album pruning for exports whose complete listing is confirmed
	if !dryRun {
		if ids, err := r.Ing.PrunableExports(ctx, name); err == nil {
			for _, id := range ids {
				pres, err := r.Ing.Prune(ctx, name, id, false)
				if err != nil {
					ar.Errors = append(ar.Errors, fmt.Sprintf("prune %s: %v", id, err))
					ar.Exit = Worst(ar.Exit, ExitPartial)
					continue
				}
				if pres.Skipped == "" {
					ar.Pruned = append(ar.Pruned, fmt.Sprintf("%s: %d links removed", id, pres.Removed))
				}
			}
		}
	}

	// unresolved entries, overdue schedule, completeness nudges
	if acct.ID == 0 {
		return ar // dry run on an account that was never ingested
	}
	if failed, err := r.St.ListFailedEntries(ctx, acct.ID, []string{store.EntryGivenUp}); err == nil && len(failed) > 0 {
		ar.Unresolved = failed
		ar.Exit = Worst(ar.Exit, ExitUnresolved)
	}
	if exps, err := r.St.ListExports(ctx, acct.ID); err == nil {
		for _, e := range exps {
			if ar.NewestExport == "" || ingest.ExportOlder(ar.NewestExport, e.ExportID) {
				ar.NewestExport = e.ExportID
			}
			if e.ConfirmedParts == 0 && e.LastPartSeenAt != "" && r.Now().Sub(store.ParseTime(e.LastPartSeenAt)) > r.Cfg.Takeout.ExportCompleteAfter && r.allPartsIngested(ctx, acct.ID, e) {
				ar.LooksComplete = append(ar.LooksComplete, fmt.Sprintf("%s (%d parts seen)", e.ExportID, e.HighestPart))
			}
		}
	}
	if ar.NewestExport != "" {
		if ts, err := time.Parse("20060102T150405Z", strings.SplitN(ar.NewestExport, "-", 2)[0]); err == nil {
			if r.Now().Sub(ts) > r.Cfg.Takeout.ExpectedInterval+r.Cfg.Takeout.Grace {
				ar.Overdue = true
				ar.Exit = Worst(ar.Exit, ExitOverdue)
			}
		}
	}
	return ar
}

func (ar *AccountReport) fail(code int, msg string) {
	ar.Errors = append(ar.Errors, msg)
	ar.Exit = Worst(ar.Exit, code)
}

func (r *Runner) allPartsIngested(ctx context.Context, accountID int64, e store.Export) bool {
	archs, err := r.St.ListArchives(ctx, accountID)
	if err != nil {
		return false
	}
	n := 0
	for _, a := range archs {
		if a.ExportID != e.ExportID {
			continue
		}
		if a.Status != store.ArchiveIngested {
			return false
		}
		n++
	}
	return n > 0 && n == e.HighestPart
}

func fileMD5(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// fetch downloads a part by Drive id into local, unless a complete copy is already there.
func (r *Runner) fetch(ctx context.Context, remoteName string, f rclone.RemoteFile, local string) error {
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return err
	}
	if fi, err := os.Stat(local); err == nil && fi.Size() == f.Size && f.MD5() != "" {
		// a leftover from an interrupted run is reused only when it is byte-identical
		if sum, err := fileMD5(local); err == nil && sum == f.MD5() {
			r.Log.Info("local copy already complete and verified", "name", f.Name)
			return nil
		}
		r.Log.Warn("local copy does not match the remote, re-downloading", "name", f.Name)
	}
	free, err := fsops.FreeBytes(filepath.Dir(local))
	if err == nil && free < uint64(float64(f.Size)*1.1) {
		return fmt.Errorf("not enough free space in inbox: need %.1f GB, have %.1f GB", float64(f.Size)*1.1/1e9, float64(free)/1e9)
	}
	_ = os.Remove(local)
	r.Log.Info("fetching part", "name", f.Name, "size_gb", fmt.Sprintf("%.1f", float64(f.Size)/1e9))
	if err := r.RC.CopyID(ctx, remoteName, f.ID, local); err != nil {
		return err
	}
	fi, err := os.Stat(local)
	if err != nil {
		return err
	}
	if fi.Size() != f.Size {
		return fmt.Errorf("size mismatch after download: %d vs %d", fi.Size(), f.Size)
	}
	if want := f.MD5(); want != "" {
		got, err := fileMD5(local)
		if err != nil {
			return err
		}
		if got != want {
			_ = os.Remove(local)
			return fmt.Errorf("md5 mismatch after download: %s vs %s", got, want)
		}
	}
	return nil
}

// deleteRemote trashes an ingested part on Drive after re-checking that the
// name is unique and the id/md5 still match.
func (r *Runner) deleteRemote(ctx context.Context, ar *AccountReport, remote string, f rclone.RemoteFile, arch *store.Archive) bool {
	if arch.Errors > 0 {
		return false
	}
	files, err := r.RC.List(ctx, remote)
	if err != nil {
		ar.Errors = append(ar.Errors, "re-list before delete: "+err.Error())
		return false
	}
	n := 0
	var match *rclone.RemoteFile
	for i := range files {
		if files[i].Name == f.Name {
			n++
			match = &files[i]
		}
	}
	if n != 1 || match.ID != arch.RemoteID || (arch.MD5 != "" && match.MD5() != "" && match.MD5() != arch.MD5) {
		ar.Errors = append(ar.Errors, fmt.Sprintf("%s: not deleted remotely (name count %d, id/md5 mismatch)", f.Name, n))
		return false
	}
	// move the exact file id into a folder only this program writes to, then
	// delete that unique path: no name-based deletion in the shared folder
	remoteName, _ := rclone.SplitRemote(remote)
	trashPath := remote + "/" + trashFolder + "/" + f.Name + "." + arch.RemoteID
	if err := r.RC.MoveID(ctx, remoteName, arch.RemoteID, trashPath); err != nil {
		ar.Errors = append(ar.Errors, fmt.Sprintf("%s: remote move: %v", f.Name, err))
		return false
	}
	if err := r.RC.DeleteFile(ctx, trashPath); err != nil {
		ar.Errors = append(ar.Errors, fmt.Sprintf("%s: remote delete: %v", f.Name, err))
		return false
	}
	_ = r.St.SetArchiveRemoteDeleted(ctx, arch.ID)
	r.Log.Info("trashed ingested part on Drive", "name", f.Name)
	return true
}

// ---- reporting ----------------------------------------------------------------

// Outcome names a report for notify_on filtering.
func Outcome(code int) string {
	switch code {
	case ExitOK:
		return "success"
	case ExitPartial:
		return "partial"
	case ExitOverdue:
		return "overdue"
	case ExitUnresolved:
		return "unresolved"
	}
	return "failure"
}

// HTML renders the report for Telegram.
func (rep *Report) HTML() string {
	esc := telegram.Esc
	var b strings.Builder
	fmt.Fprintf(&b, "<b>gphoto-sync</b> %s · exit %d (%s) · %s\n", esc(rep.Finished.Format("2006-01-02 15:04")), rep.Exit, Outcome(rep.Exit), esc(rep.Finished.Sub(rep.Started).Round(time.Second).String()))
	if rep.Fatal != "" {
		fmt.Fprintf(&b, "<b>fatal:</b> %s\n", esc(rep.Fatal))
	}
	for _, a := range rep.Accounts {
		fmt.Fprintf(&b, "\n<b>%s</b>: parts seen %d, settling %d, fetched %d, trashed on Drive %d\n", esc(a.Name), a.PartsSeen, a.PartsSettling, a.PartsFetched, a.RemoteDeleted)
		for _, r := range a.Results {
			fmt.Fprintf(&b, "• %s: %s — added %d, duplicates %d, changed %d, promoted %d, links %d, failed %d, pending sidecars %d, %s\n",
				esc(r.Part), esc(r.Status), r.Added, r.Duplicates, r.Changed, r.Promoted, r.Links, r.Failed+r.GivenUp, r.PendingSidecars, esc(r.Duration.Round(time.Second).String()))
			for i, e := range r.Errors {
				if i >= 5 {
					fmt.Fprintf(&b, "   … %d more\n", len(r.Errors)-5)
					break
				}
				fmt.Fprintf(&b, "   ! %s\n", esc(e))
			}
		}
		if a.NewestExport != "" {
			fmt.Fprintf(&b, "newest export: %s", esc(a.NewestExport))
			if a.Overdue {
				b.WriteString(" — <b>OVERDUE</b>: no new export for too long, check the Takeout schedule")
			}
			b.WriteString("\n")
		}
		for _, p := range a.Pruned {
			fmt.Fprintf(&b, "album pruning applied for export %s\n", esc(p))
		}
		for _, lc := range a.LooksComplete {
			fmt.Fprintf(&b, "export %s looks complete; confirm with: gphoto-sync exports confirm --account %s --export %s --parts N\n", esc(lc), esc(a.Name), esc(strings.Fields(lc)[0]))
		}
		if len(a.Unresolved) > 0 {
			fmt.Fprintf(&b, "<b>%d entries given up</b> (run: gphoto-sync retry --account %s):\n", len(a.Unresolved), esc(a.Name))
			for i, u := range a.Unresolved {
				if i >= 5 {
					fmt.Fprintf(&b, "   … %d more\n", len(a.Unresolved)-5)
					break
				}
				fmt.Fprintf(&b, "   %s: %s\n", esc(u.Entry), esc(u.LastErr))
			}
		}
		for _, e := range a.Errors {
			fmt.Fprintf(&b, "! %s\n", esc(e))
		}
	}
	return b.String()
}

// Text renders the report for the terminal.
func (rep *Report) Text() string {
	s := rep.HTML()
	for _, tag := range []string{"<b>", "</b>"} {
		s = strings.ReplaceAll(s, tag, "")
	}
	return strings.NewReplacer("&lt;", "<", "&gt;", ">", "&amp;", "&", "&#39;", "'", "&#34;", `"`).Replace(s)
}
