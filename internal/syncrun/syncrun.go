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
	"strconv"
	"strings"
	"time"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/ingest"
	"gphoto-sync/internal/rclone"
	"gphoto-sync/internal/store"
	"gphoto-sync/internal/takeout"
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
	Notes          []string // noteworthy, non-failing events (replaced parts, unverifiable matches)
	Exit           int
	DuplicateNames []string
	Changed        bool // the run changed the library: a part was ingested or the totals moved
}

// Report is the outcome of a whole run.
type Report struct {
	Started      time.Time
	Finished     time.Time
	Accounts     []*AccountReport
	Exit         int
	Fatal        string
	Library      []*Library // what every account holds on disk; filled when the run changed something
	LibraryTotal *Library   // every account together, when there are several
}

// Library is what a library holds on disk: one account's, or every
// account's together.
type Library struct {
	Name   string // account name, or "all accounts"
	Photos int    // items that are not videos, edited copies included
	Videos int    // items with a video extension, motion-photo companions included
	Albums int
	Links  int
	Bytes  int64
}

func (l *Library) line() string {
	return fmt.Sprintf("%s, %s, %s, %s on disk", plural(l.Photos, "photo"), plural(l.Videos, "video"), plural(l.Albums, "album"), HumanSize(l.Bytes))
}

// Remote is the part of the rclone client the runner uses (*rclone.Client
// implements it; tests substitute a fake).
type Remote interface {
	List(ctx context.Context, remote string) ([]rclone.RemoteFile, error)
	CopyID(ctx context.Context, remoteName, id, dest string) error
	MoveID(ctx context.Context, remoteName, id, destRemotePath string) error
	DeleteFile(ctx context.Context, remotePath string) error
}

// Runner holds the dependencies.
type Runner struct {
	Cfg *config.Config
	St  *store.Store
	Log *slog.Logger
	Ing *ingest.Ingester
	RC  Remote
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
	if !dryRun && ctx.Err() == nil && rep.changed() {
		r.summarize(ctx, rep)
	}
	rep.Finished = r.Now()
	return rep
}

// runAccount syncs one account and tells whether the library changed.
func (r *Runner) runAccount(ctx context.Context, name string, dryRun bool) *AccountReport {
	var before *Library
	if !dryRun {
		before = r.snapshot(ctx, name)
	}
	ar := r.syncAccount(ctx, name, dryRun)
	ar.Changed = len(ar.Results) > 0
	if before != nil && ctx.Err() == nil {
		if after := r.snapshot(ctx, name); after != nil && *after != *before {
			ar.Changed = true
		}
	}
	return ar
}

// snapshot reads what an account holds on disk; an account never ingested
// holds nothing. A failed read is logged and returns nil.
func (r *Runner) snapshot(ctx context.Context, name string) *Library {
	acct, err := r.St.GetAccount(ctx, name)
	if err == nil && acct == nil {
		return &Library{Name: name}
	}
	var l *Library
	if err == nil {
		l, err = r.library(ctx, name, []int64{acct.ID})
	}
	if err != nil {
		r.Log.Warn("library totals", "account", name, "err", err)
		return nil
	}
	return l
}

// library reads the totals of the given accounts together. Album links made
// as copies take their bytes on disk again.
func (r *Runner) library(ctx context.Context, name string, accountIDs []int64) (*Library, error) {
	t, err := r.St.LibraryTotals(ctx, accountIDs)
	if err != nil {
		return nil, err
	}
	l := &Library{Name: name, Albums: t.Albums, Links: t.Links, Bytes: t.Bytes}
	if r.Cfg.Layout.LinkMode == "copy" {
		l.Bytes += t.LinkBytes
	}
	for ext, n := range t.ByExt {
		if takeout.IsVideoName("x" + ext) {
			l.Videos += n
		} else {
			l.Photos += n
		}
	}
	return l, nil
}

// changed tells whether any account's library changed in this run.
func (rep *Report) changed() bool {
	for _, a := range rep.Accounts {
		if a.Changed {
			return true
		}
	}
	return false
}

// summarize fills in what every configured account holds on disk (nothing
// yet for one never ingested), and the total over the configured accounts
// when there are several. A failed read leaves the report without the
// summary.
func (r *Runner) summarize(ctx context.Context, rep *Report) {
	var libs []*Library
	var ids []int64
	for _, name := range r.Cfg.AccountNames() {
		acct, err := r.St.GetAccount(ctx, name)
		if err == nil && acct == nil {
			libs = append(libs, &Library{Name: name})
			continue
		}
		var l *Library
		if err == nil {
			l, err = r.library(ctx, name, []int64{acct.ID})
		}
		if err != nil {
			r.Log.Warn("library totals", "account", name, "err", err)
			return
		}
		libs = append(libs, l)
		ids = append(ids, acct.ID)
	}
	var total *Library
	if len(libs) > 1 {
		var err error
		if total, err = r.library(ctx, "all accounts", ids); err != nil {
			r.Log.Warn("library totals", "account", "all", "err", err)
			return
		}
	}
	rep.Library, rep.LibraryTotal = libs, total
}

func (r *Runner) syncAccount(ctx context.Context, name string, dryRun bool) *AccountReport {
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
	// every part Drive lists for an export is evidence against its completeness
	// until this run confirms it ingested (a duplicate name, a part still
	// settling, a replacement not yet ingested, a fetch or ingest failure, a
	// part with errors all keep the export open; see ResolvePending)
	outstanding := map[string]int{}
	for _, f := range parts {
		e, _, _ := ingest.ParsePartName(f.Name)
		outstanding[e]++
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
		arch, err := r.St.GetArchiveByRemoteID(ctx, acct.ID, f.ID)
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
		if arch == nil || arch.RemoteID != f.ID {
			// the same Drive file visible from two accounts (a shared folder) is
			// neither this account's ingest nor a file this account may delete
			other, err := r.St.ArchiveByRemoteIDAnyAccount(ctx, f.ID)
			if err != nil {
				ar.fail(ExitFatal, err.Error())
				return ar
			}
			if other != nil && other.AccountID != acct.ID {
				ar.problem(fmt.Sprintf("%s (Drive id %s) is recorded by another account's archive: shared folder? left untouched", f.Name, f.ID))
				continue
			}
		}
		// a recorded part is skipped only when the file on Drive is the one
		// that was recorded: a re-upload under a new id or a change in place
		// carries content the earlier ingest never saw
		replaced := false
		awaitingRetry := false
		if arch != nil && arch.Status == store.ArchiveWithErrors {
			// `retry` turned given-up entries back into failed ones
			if counts, err := r.St.CountEntries(ctx, arch.ID); err == nil && counts[store.EntryFailed] > 0 {
				awaitingRetry = true
			}
		}
		switch d, why := decide(arch, f, awaitingRetry); d {
		case dispReplace:
			replaced = true
			ar.Notes = append(ar.Notes, fmt.Sprintf("%s: %s: ingesting again from scratch", f.Name, why))
			r.Log.Warn("part on Drive is not the recorded one", "name", f.Name, "why", why, "status", arch.Status)
		case dispSkipUnverified:
			ar.Notes = append(ar.Notes, fmt.Sprintf("%s was ingested from another copy whose hash is unknown: left on Drive", f.Name))
			outstanding[exportID]--
			continue
		case dispSkipIngested:
			outstanding[exportID]--
			if arch.RemoteID != f.ID && !dryRun {
				// the same content under a new id: the old file's deletion, if still
				// owed (staged by a run whose delete failed), must be finished first,
				// or its id would be forgotten with the file left in staging
				if !r.settleOldRemote(ctx, ar, acc.RcloneRemote, arch, files) {
					ar.Notes = append(ar.Notes, fmt.Sprintf("%s: an earlier copy of this part still awaits deletion; the new copy waits for it", f.Name))
					continue
				}
				if err := r.St.SetArchiveRemote(ctx, arch.ID, f.ID, f.Size, f.MD5()); err != nil {
					ar.fail(ExitFatal, err.Error())
					return ar
				}
				arch.RemoteID, arch.MD5, arch.RemoteDeletedAt = f.ID, f.MD5(), ""
			}
			if r.Cfg.Takeout.DeleteRemoteAfterIngest && !dryRun {
				if r.deleteRemote(ctx, ar, acc.RcloneRemote, f, arch) {
					ar.RemoteDeleted++
				}
			}
			continue
		case dispSkipErrors:
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
		// whenever the row is about to record another Drive file (a resumed or
		// replaced part re-uploaded under a new id), a deletion still owed for
		// the old file (staged by a run whose delete failed) is finished first,
		// or its id would be forgotten with the file left in staging
		if arch != nil && arch.RemoteID != f.ID && !r.settleOldRemote(ctx, ar, acc.RcloneRemote, arch, files) {
			ar.Notes = append(ar.Notes, fmt.Sprintf("%s: the earlier copy of this part still awaits deletion; the new copy waits for it", f.Name))
			continue
		}
		if replaced {
			// the entry bookkeeping and occurrences belong to the old content; the
			// new file is ingested from scratch (unchanged entries are idempotent)
			// and the export's album listing is pruned again afterwards
			if err := r.St.ResetPartForReingest(ctx, arch.ID, acct.ID, exportID, partNo); err != nil {
				ar.fail(ExitFatal, err.Error())
				return ar
			}
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
			outstanding[exportID]--
			if !r.Cfg.Takeout.KeepArchives {
				if err := os.Remove(local); err != nil && !errors.Is(err, os.ErrNotExist) {
					r.Log.Warn("could not remove local part", "path", local, "err", err)
				}
			}
			if r.Cfg.Takeout.DeleteRemoteAfterIngest {
				if a2, _ := r.St.GetArchiveByRemoteID(ctx, acct.ID, f.ID); a2 != nil && r.deleteRemote(ctx, ar, acc.RcloneRemote, f, a2) {
					ar.RemoteDeleted++
				}
			}
		}
	}

	// parts staged for deletion by an earlier run whose delete step failed are
	// no longer in the Takeout folder listing; finish them from the staging
	// folder (files already staged are finished even with deletion turned off)
	if !dryRun && acct.ID != 0 {
		r.finishStagedDeletes(ctx, ar, acct.ID, acc.RcloneRemote, files)
	}

	// sidecars still waiting in album folders may describe photos that
	// arrived since (or, after an upgrade, photos ingested earlier)
	if !dryRun && acct.ID != 0 {
		if exps, err := r.St.ExportsWithPending(ctx, acct.ID); err == nil {
			for _, e := range exps {
				// name collisions with known photos are decided for good only once
				// the export is complete, and nothing Drive lists for it is left
				// unconfirmed by this run
				complete, err := r.Ing.ExportComplete(ctx, acct.ID, e, r.Cfg.Takeout.ExportCompleteAfter, r.Now())
				if err != nil {
					ar.problem(fmt.Sprintf("completeness of %s: %v", e, err))
					continue
				}
				complete = complete && outstanding[e] == 0
				links, promoted, err := r.Ing.ResolvePending(ctx, acct, e, complete)
				if err != nil {
					ar.problem(fmt.Sprintf("resolve pending sidecars of %s: %v", e, err))
					continue
				}
				if links+promoted > 0 {
					ar.Notes = append(ar.Notes, fmt.Sprintf("%s: %d album links and %d promotions from sidecars that found their photo", e, links, promoted))
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

// problem records a non-fatal failure that keeps the run from reporting success.
func (ar *AccountReport) problem(msg string) {
	ar.Errors = append(ar.Errors, msg)
	ar.Exit = Worst(ar.Exit, ExitPartial)
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
	// rclone writes <name>.<random>.partial and renames at the end; an
	// interrupted run leaves that behind, 50 GB of it
	if leftovers, err := filepath.Glob(local + ".*.partial"); err == nil {
		for _, p := range leftovers {
			_ = os.Remove(p)
		}
	}
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

// disposition is what to do with a listed part given its archive row.
type disposition int

const (
	dispNew            disposition = iota // nothing recorded: fetch and ingest
	dispResume                            // the recorded file, not finished: continue where it stopped
	dispSkipIngested                      // the recorded file, ingested: only remote deletion may remain
	dispSkipUnverified                    // ingested from a copy that cannot be verified: leave it alone
	dispSkipErrors                        // ingested with given-up entries: reported, retried via `retry`
	dispReplace                           // another file, or one of unprovable identity: start over
)

// decide tells whether a listed file resumes, replaces or is skipped for the
// archive recorded under its name. Finished entries are only ever reused for
// a file proven to be the recorded one; for an unproven file nothing is
// trusted: an unfinished part starts over, a finished one is left alone. A
// part ingested with given-up entries is skipped unless some entries await a
// retry again (awaitingRetry), in which case it resumes.
func decide(arch *store.Archive, f rclone.RemoteFile, awaitingRetry bool) (disposition, string) {
	if arch == nil {
		return dispNew, ""
	}
	why, verified := compareArchive(arch, f)
	switch {
	case why != "":
		return dispReplace, why
	case !verified && arch.Status == store.ArchiveIngested:
		return dispSkipUnverified, ""
	case !verified:
		return dispReplace, "cannot be proven to be the file ingested before (no hash recorded)"
	case arch.Status == store.ArchiveIngested:
		return dispSkipIngested, ""
	case arch.Status == store.ArchiveWithErrors && !awaitingRetry:
		return dispSkipErrors, ""
	}
	return dispResume, ""
}

// compareArchive tells whether the file on Drive is the one recorded for a
// part. why is non-empty when they are known to differ; verified is true when
// the match was established by hash, or by Drive id and size when no hash is
// recorded.
func compareArchive(arch *store.Archive, f rclone.RemoteFile) (why string, verified bool) {
	if arch.MD5 != "" && f.MD5() != "" {
		if arch.MD5 != f.MD5() {
			return fmt.Sprintf("md5 %s on Drive, %s ingested", f.MD5(), arch.MD5), true
		}
		return "", true
	}
	if arch.Size > 0 && f.Size > 0 && arch.Size != f.Size {
		return fmt.Sprintf("size %d on Drive, %d ingested", f.Size, arch.Size), true
	}
	return "", arch.RemoteID == f.ID && arch.Size > 0 && f.Size > 0
}

// deleteRemote trashes an ingested part on Drive after re-checking that the
// name is unique and the id/md5 still match.
func (r *Runner) deleteRemote(ctx context.Context, ar *AccountReport, remote string, f rclone.RemoteFile, arch *store.Archive) bool {
	if arch.Errors > 0 {
		return false
	}
	files, err := r.RC.List(ctx, remote)
	if err != nil {
		ar.problem("re-list before delete: " + err.Error())
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
	if n != 1 {
		ar.problem(fmt.Sprintf("%s: not deleted remotely: %d files carry this name", f.Name, n))
		return false
	}
	if why := notDeletable(arch, *match); why != "" {
		ar.problem(fmt.Sprintf("%s: not deleted remotely: %s", f.Name, why))
		return false
	}
	// move the exact file id into a folder only this program writes to, then
	// delete that unique path: no name-based deletion in the shared folder
	remoteName, _ := rclone.SplitRemote(remote)
	trashPath := remote + "/" + trashFolder + "/" + f.Name + "." + arch.RemoteID
	if err := r.RC.MoveID(ctx, remoteName, arch.RemoteID, trashPath); err != nil {
		ar.problem(fmt.Sprintf("%s: remote move: %v", f.Name, err))
		return false
	}
	// evidence that a file of this part sits in staging: a failed delete is
	// finished by a later run, and nothing may forget the id until then. The
	// record is part of the operation: without it the delete is not attempted
	// (the row stays in the awaiting set a later run inspects)
	if err := r.St.SetArchiveStaged(ctx, arch.ID); err != nil {
		ar.problem(fmt.Sprintf("%s: record staged deletion: %v", f.Name, err))
		return false
	}
	if err := r.RC.DeleteFile(ctx, trashPath); err != nil {
		// the file sits in the staging folder under its id; the next run finds it there
		ar.problem(fmt.Sprintf("%s: remote delete (staged, retried next run): %v", f.Name, err))
		return false
	}
	_ = r.St.SetArchiveRemoteDeleted(ctx, arch.ID)
	r.Log.Info("trashed ingested part on Drive", "name", f.Name)
	return true
}

// notDeletable tells why a file on Drive may not be deleted for an ingested
// archive: it must carry the recorded id and, when a hash was recorded, the
// listing must report that same hash (a file changed after it was ingested, or
// one whose hash Drive does not report yet, stays for a later retry); without
// a recorded hash the sizes must agree.
func notDeletable(arch *store.Archive, f rclone.RemoteFile) string {
	switch {
	case f.ID != arch.RemoteID:
		return fmt.Sprintf("Drive id %s, ingested %s", f.ID, arch.RemoteID)
	case arch.MD5 != "" && f.MD5() == "":
		return "Drive reports no md5 to compare with the ingested file"
	case arch.MD5 != "" && f.MD5() != arch.MD5:
		return fmt.Sprintf("md5 %s on Drive, %s ingested", f.MD5(), arch.MD5)
	case arch.MD5 == "" && arch.Size > 0 && f.Size != arch.Size:
		return fmt.Sprintf("size %d on Drive, %d ingested", f.Size, arch.Size)
	}
	return ""
}

// mergeArchives appends the rows of b not already in a (by id).
func mergeArchives(a, b []*store.Archive) []*store.Archive {
	seen := map[int64]bool{}
	for _, x := range a {
		seen[x.ID] = true
	}
	for _, x := range b {
		if !seen[x.ID] {
			seen[x.ID] = true
			a = append(a, x)
		}
	}
	return a
}

// listStaged lists the staging folder; one that was never created is empty.
func (r *Runner) listStaged(ctx context.Context, remote string) ([]rclone.RemoteFile, error) {
	staged, err := r.RC.List(ctx, remote+"/"+trashFolder)
	if err != nil && strings.Contains(err.Error(), "directory not found") {
		return nil, nil
	}
	return staged, err
}

// deleteStaged finishes the deletion owed for an archive's Drive file that is
// no longer in the Takeout folder: deleted from the staging folder when it is
// there, recorded as deleted when it is in neither place. It reports whether
// the archive owes nothing any more.
func (r *Runner) deleteStaged(ctx context.Context, ar *AccountReport, remote string, a *store.Archive, staged []rclone.RemoteFile) bool {
	var match *rclone.RemoteFile
	for i := range staged {
		if staged[i].ID == a.RemoteID {
			match = &staged[i]
		}
	}
	if match == nil {
		_ = r.St.SetArchiveRemoteDeleted(ctx, a.ID)
		ar.Notes = append(ar.Notes, fmt.Sprintf("%s is no longer on Drive: recorded as deleted", a.Name))
		return true
	}
	// a file is only ever staged after a clean ingest of that very copy, so
	// errors a later local ingest recorded for the part do not protect it
	if why := notDeletable(a, *match); why != "" {
		ar.problem(fmt.Sprintf("%s: staged file left alone: %s", a.Name, why))
		return false
	}
	if err := r.RC.DeleteFile(ctx, remote+"/"+trashFolder+"/"+match.Name); err != nil {
		ar.problem(fmt.Sprintf("%s: remote delete from staging: %v", a.Name, err))
		return false
	}
	_ = r.St.SetArchiveRemoteDeleted(ctx, a.ID)
	ar.RemoteDeleted++
	r.Log.Info("deleted staged part on Drive", "name", a.Name)
	return true
}

// settleOldRemote reports whether an archive owes nothing for the Drive file
// it currently records, finishing a staged deletion when it can. A file still
// in the Takeout folder is handled on its own turn of the listing, so the
// archive is not free to be matched to another file yet.
func (r *Runner) settleOldRemote(ctx context.Context, ar *AccountReport, remote string, arch *store.Archive, root []rclone.RemoteFile) bool {
	if arch.RemoteID == "" || arch.RemoteDeletedAt != "" {
		return true
	}
	for _, f := range root {
		if f.ID == arch.RemoteID {
			return false
		}
	}
	staged, err := r.listStaged(ctx, remote)
	if err != nil {
		ar.problem("list staging folder: " + err.Error())
		return false
	}
	return r.deleteStaged(ctx, ar, remote, arch, staged)
}

// finishStagedDeletes deletes parts that an earlier run moved into the
// staging folder without managing to delete them. A part that is in neither
// the Takeout folder nor the staging folder is gone and recorded as deleted.
func (r *Runner) finishStagedDeletes(ctx context.Context, ar *AccountReport, acctID int64, remote string, root []rclone.RemoteFile) {
	// files already in our staging folder are always finished, whatever the
	// part's status became since (a later local ingest may have changed it);
	// with deletion on, every ingested part still owing one is handled too
	waiting, err := r.St.ArchivesStaged(ctx, acctID)
	if err != nil {
		ar.problem("list staged parts: " + err.Error())
		return
	}
	if r.Cfg.Takeout.DeleteRemoteAfterIngest {
		if more, err := r.St.ArchivesAwaitingRemoteDelete(ctx, acctID); err == nil {
			waiting = mergeArchives(waiting, more)
		} else {
			ar.problem("list parts awaiting remote delete: " + err.Error())
		}
	}
	if len(waiting) == 0 {
		return
	}
	inRoot := map[string]bool{}
	for _, f := range root {
		inRoot[f.ID] = true
	}
	var staged []rclone.RemoteFile
	listed := false
	for _, a := range waiting {
		if inRoot[a.RemoteID] {
			continue // still in the Takeout folder: the main loop handles it
		}
		if !listed {
			listed = true
			if staged, err = r.listStaged(ctx, remote); err != nil {
				ar.problem("list staging folder: " + err.Error())
				return
			}
		}
		r.deleteStaged(ctx, ar, remote, a, staged)
	}
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
			if r.Versioned > 0 {
				fmt.Fprintf(&b, "   older export: %d files kept as versions, current content untouched\n", r.Versioned)
			}
			if r.SidecarLinks > 0 {
				fmt.Fprintf(&b, "   %d album links from sidecar-only album entries\n", r.SidecarLinks)
			}
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
		for _, n := range a.Notes {
			fmt.Fprintf(&b, "· %s\n", esc(n))
		}
		for _, e := range a.Errors {
			fmt.Fprintf(&b, "! %s\n", esc(e))
		}
	}
	if len(rep.Library) > 0 {
		b.WriteString("\n<b>Library</b>\n")
		for _, l := range rep.Library {
			fmt.Fprintf(&b, "%s: %s\n", esc(l.Name), l.line())
		}
		if rep.LibraryTotal != nil {
			fmt.Fprintf(&b, "<b>%s</b>: %s\n", esc(rep.LibraryTotal.Name), rep.LibraryTotal.line())
		}
	}
	return b.String()
}

// HumanSize formats a byte count: GB for libraries and parts, smaller units
// for stray files such as Takeout's archive_browser.html zip.
func HumanSize(n int64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.1f GB", float64(n)/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1f MB", float64(n)/1e6)
	default:
		return fmt.Sprintf("%.0f KB", float64(n)/1e3)
	}
}

// commas renders 43591 as 43,591.
func commas(n int) string {
	s := strconv.Itoa(n)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

// plural renders "1 photo", "2 photos", "43,591 photos".
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return commas(n) + " " + noun + "s"
}

// Text renders the report for the terminal.
func (rep *Report) Text() string {
	s := rep.HTML()
	for _, tag := range []string{"<b>", "</b>"} {
		s = strings.ReplaceAll(s, tag, "")
	}
	return strings.NewReplacer("&lt;", "<", "&gt;", ">", "&amp;", "&", "&#39;", "'", "&#34;", `"`).Replace(s)
}
