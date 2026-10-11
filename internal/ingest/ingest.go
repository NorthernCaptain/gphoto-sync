// Package ingest turns Takeout parts into the library/albums tree and the
// SQLite state, idempotently. See PLAN.md section 3.2 for the rules.
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/media"
	"gphoto-sync/internal/store"
	"gphoto-sync/internal/takeout"
)

// PartMeta identifies one part of one export.
type PartMeta struct {
	Account      string
	RcloneRemote string
	ExportID     string // "<timestamp>-<seq>"
	PartNo       int
	Name         string
	LocalPath    string
	RemoteID     string
	Size         int64
	MD5          string
}

var partNameRe = regexp.MustCompile(`^takeout-(\d{8}T\d{6}Z)-(\d+)-(\d{3})\.(zip|tgz)$`)

// ParsePartName parses "takeout-20261006T035113Z-1-001.zip".
func ParsePartName(name string) (exportID string, partNo int, ok bool) {
	m := partNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", 0, false
	}
	n, _ := strconv.Atoi(m[3])
	return m[1] + "-" + m[2], n, true
}

// partDiffers reports whether a part file is known to differ from the one an
// archive row was recorded for: by MD5 when both are known, else by size.
func partDiffers(prev *store.Archive, part PartMeta) bool {
	if prev.MD5 != "" && part.MD5 != "" {
		return prev.MD5 != part.MD5
	}
	return prev.Size > 0 && part.Size > 0 && prev.Size != part.Size
}

// ExportOlder reports whether export id a is older than b (timestamp, then sequence).
func ExportOlder(a, b string) bool {
	ta, sa := splitExportID(a)
	tb, sb := splitExportID(b)
	if ta != tb {
		return ta < tb
	}
	return sa < sb
}

func splitExportID(id string) (string, int) {
	i := strings.LastIndex(id, "-")
	if i < 0 {
		return id, 0
	}
	n, _ := strconv.Atoi(id[i+1:])
	return id[:i], n
}

// Options tune one ingest run.
type Options struct {
	MaxEntries int  // stop after this many media entries (0 = all)
	DryRun     bool // classify and report only
	FullAlbums bool // mark the export authoritative (requires Parts)
	Parts      int  // confirmed part count of the export
	Replace    bool // the file replaces an earlier ingest of this part: forget its bookkeeping first
}

// Result summarises one part.
type Result struct {
	Part             string
	Status           string
	Entries          int
	Media            int
	Added            int
	Duplicates       int
	Changed          int
	Versioned        int  // differing bytes from an older export, kept as versions
	Replaced         bool // an earlier ingest of this part was forgotten before this one
	Unlinked         int  // album links removed because no ingested part lists them any more
	SidecarsOrphaned int  // stored sidecars this part supplied before but no longer carries
	SidecarLinks     int  // album links made from sidecars whose photo lives in another folder
	Promoted         int
	Merged           int
	Links            int
	Failed           int
	GivenUp          int
	Skipped          int
	Unresolved       int
	PendingSidecars  int
	Albums           int
	Duration         time.Duration
	Errors           []string
}

// Ingester holds the dependencies of ingest runs.
type Ingester struct {
	cfg *config.Config
	st  *store.Store
	log *slog.Logger
	loc *time.Location

	journalCount atomic.Int64 // journal entries begun; used to decide whether a temp file may be removed
}

// New creates an Ingester.
func New(cfg *config.Config, st *store.Store, log *slog.Logger) *Ingester {
	return &Ingester{cfg: cfg, st: st, log: log, loc: cfg.Location()}
}

// rulesFor returns the Takeout names of an account's Google language: each
// account is classified with the names of its own language only, so an album
// of one account cannot pass for a system folder of another language.
func (ing *Ingester) rulesFor(account string) takeout.Rules {
	rules, err := takeout.ForLanguage(ing.cfg.LanguageFor(account))
	if err != nil {
		// Validate refuses this; a config built by hand without it gets English
		ing.log.Warn("takeout languages", "account", account, "err", err)
		return takeout.DefaultRules
	}
	return rules
}

type folderCtx struct {
	name     string
	kind     takeout.FolderKind
	year     int
	index    *takeout.Index
	meta     *takeout.AlbumMeta
	album    *store.Album
	hasMedia bool
	// lower(stem) -> names of media already ingested in this folder of the
	// export, loaded once on first use (the producer and the commit loop both
	// ask for it)
	occOnce  sync.Once
	occStems map[string][]string
	occErr   error
}

// sidecarOnlyPrefix marks the occurrence recorded for an album entry that
// Takeout delivered as a sidecar only (the photo lives in another folder):
// evidence of membership kept apart from any media file of that name.
const sidecarOnlyPrefix = "sidecar:"

func isSidecarOnlyEntry(name string) bool { return strings.HasPrefix(name, sidecarOnlyPrefix) }

// stemCandidates lists every media file of the folder sharing a stem: those
// in this part and those ingested from earlier parts of the export, so a
// sidecar named after a title is never handed to a second, unrelated file.
func (pr *partRun) stemCandidates(fc *folderCtx, key string) ([]string, error) {
	fc.occOnce.Do(func() {
		stems := map[string][]string{}
		occs, err := pr.ing.st.OccurrencesInFolder(pr.ctx, pr.acct.ID, pr.part.ExportID, fc.name)
		if err != nil {
			fc.occErr = err
			return
		}
		for _, o := range occs {
			if o.ItemID == 0 || o.State == store.OccReset || o.State == store.OccRemoved || isSidecarOnlyEntry(o.EntryName) {
				continue
			}
			root, _ := takeout.SplitExt(o.EntryName)
			stems[strings.ToLower(root)] = append(stems[strings.ToLower(root)], o.EntryName)
		}
		fc.occStems = stems
	})
	if fc.occErr != nil {
		return nil, fc.occErr
	}
	seen := map[string]bool{}
	var out []string
	for _, n := range append(append([]string{}, fc.index.StemCandidates(key)...), fc.occStems[strings.ToLower(key)]...) {
		if !seen[strings.ToLower(n)] {
			seen[strings.ToLower(n)] = true
			out = append(out, n)
		}
	}
	return out, nil
}

type job struct {
	e     SourceEntry
	ce    takeout.Entry
	fc    *folderCtx
	match *takeout.Match
	// pending is the media key of a sidecar taken from pending_sidecars. The row
	// is kept: the same sidecar also serves the edited and motion companions of
	// the media, which may arrive in later parts (status counts only rows whose
	// media is still missing).
	pending string
}

type extracted struct {
	job
	tmpRel   string // temp file holding the bytes; empty when they already live in the library
	sha      string
	size     int64
	exifTime time.Time
	exifSrc  string
	err      error
	exportID string // export that supplied the bytes, when not this part's
}

type partRun struct {
	rules      takeout.Rules // the account's Takeout names
	recognized int           // entries under a known product folder
	ing        *Ingester
	ctx        context.Context
	src        Source
	part       PartMeta
	opt        Options
	acct       *store.Account
	arch       *store.Archive
	folders    map[string]*folderCtx
	res        Result
	tmpDir     string

	prunedListing       string // newest complete listing applied to the account (lazy)
	prunedListingLoaded bool
	pendingRejected     map[string]bool // "<folder>/<key>" of stored sidecars a commit refused (another photo's)
}

// appliedListing returns the newest complete album listing that applies to an
// album: the one recorded for the album, else (for an album created after the
// last pruning) the newest one applied to the account.
func (pr *partRun) appliedListing(album *store.Album) (string, error) {
	applied, err := pr.ing.st.GetAlbumFull(pr.ctx, album.ID)
	if err != nil {
		return "", err
	}
	if !pr.prunedListingLoaded {
		if pr.prunedListing, err = pr.ing.st.NewestPrunedExport(pr.ctx, pr.acct.ID); err != nil {
			return "", err
		}
		pr.prunedListingLoaded = true
	}
	if pr.prunedListing != "" && (applied == "" || ExportOlder(applied, pr.prunedListing)) {
		return pr.prunedListing, nil
	}
	return applied, nil
}

// IngestPart processes one part. It is safe to call again for the same part:
// finished entries are skipped and every file operation is idempotent.
func (ing *Ingester) IngestPart(ctx context.Context, src Source, part PartMeta, opt Options) (*Result, error) {
	start := time.Now()
	pr := &partRun{ing: ing, ctx: ctx, src: src, part: part, opt: opt, folders: map[string]*folderCtx{}, rules: ing.rulesFor(part.Account)}
	pr.res.Part = part.Name
	pr.tmpDir = path.Join(part.Account, ".tmp")

	var acct *store.Account
	var err error
	if opt.DryRun {
		// read-only: never create the account row
		if acct, err = ing.st.GetAccount(ctx, part.Account); err == nil && acct == nil {
			acct = &store.Account{Name: part.Account}
		}
	} else {
		acct, err = ing.st.EnsureAccount(ctx, part.Account, part.RcloneRemote)
	}
	if err != nil {
		return nil, err
	}
	pr.acct = acct

	// pass A: classify, read descriptors and sidecars
	var mediaEntries []job
	var badEntries []failedEntry // descriptor/sidecar entries that could not be read or parsed
	var goodDescriptors []string // descriptor/sidecar entries parsed fine (to clear earlier failures)
	for _, e := range src.Entries() {
		ce, ok := takeout.Classify(e.Name, e.Size, pr.rules)
		if !ok {
			continue
		}
		pr.res.Entries++
		if ce.Folder != "" {
			pr.recognized++ // under Takeout/<product>/<folder>/ (ignored top-level files aside)
		}
		switch ce.Type {
		case takeout.TypeIgnore:
			pr.res.Skipped++
			continue
		case takeout.TypeAlbumMeta:
			raw, err := pr.readSmall(e, 4<<20)
			if err == nil {
				var m *takeout.AlbumMeta
				if m, err = takeout.ParseAlbumMeta(raw); err == nil {
					pr.folder(ce).meta = m
				}
			}
			if err != nil {
				badEntries = append(badEntries, failedEntry{name: e.Name, err: err})
			} else {
				goodDescriptors = append(goodDescriptors, e.Name)
			}
		case takeout.TypeSidecar:
			if (ce.FolderKind == takeout.FolderTrash && !ing.cfg.Takeout.IncludeTrash) || (ce.FolderKind == takeout.FolderArchive && !ing.cfg.Takeout.IncludeArchive) {
				pr.res.Skipped++ // its media is skipped too: nothing to wait for
				continue
			}
			raw, err := pr.readSmall(e, 8<<20)
			if err == nil {
				var sc *takeout.Sidecar
				if sc, err = takeout.ParseSidecar(raw); err == nil {
					pr.folder(ce).index.Add(ce.Base, sc)
				}
			}
			if err != nil {
				badEntries = append(badEntries, failedEntry{name: e.Name, err: err})
			} else {
				goodDescriptors = append(goodDescriptors, e.Name)
			}
		case takeout.TypeMedia:
			if ce.FolderKind == takeout.FolderTrash && !ing.cfg.Takeout.IncludeTrash {
				pr.res.Skipped++
				continue
			}
			if ce.FolderKind == takeout.FolderArchive && !ing.cfg.Takeout.IncludeArchive {
				pr.res.Skipped++
				continue
			}
			if _, edited := pr.rules.IsEdited(ce.Base); edited && ing.cfg.Layout.EditedVersions == "prefer_original" {
				pr.res.Skipped++
				continue
			}
			fc := pr.folder(ce)
			fc.hasMedia = true
			fc.index.AddMedia(ce.Base)
			mediaEntries = append(mediaEntries, job{e: e, ce: ce, fc: fc})
			pr.res.Media++
		}
	}
	// a part whose entries are all outside the layout this run understands is
	// a configuration problem (a Takeout in another language names its folders
	// differently), never an empty part: fail before recording anything, so
	// the next run resumes from the local copy once the names are configured
	if pr.recognized == 0 {
		all := src.Entries()
		if len(all) == 0 {
			return nil, errors.New("the archive has no entries")
		}
		return nil, fmt.Errorf("none of the %d entries is under Takeout/<product>/<folder>/ with a known product folder (first entry %q): set this account's Google language in accounts.%s.takeout.language", len(all), all[0].Name, part.Account)
	}
	// year folders first (library items), then archive, then albums; by offset within each
	rank := func(k takeout.FolderKind) int {
		switch k {
		case takeout.FolderYear:
			return 0
		case takeout.FolderArchive:
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(mediaEntries, func(i, j int) bool {
		ri, rj := rank(mediaEntries[i].ce.FolderKind), rank(mediaEntries[j].ce.FolderKind)
		if ri != rj {
			return ri < rj
		}
		return mediaEntries[i].e.Offset < mediaEntries[j].e.Offset
	})
	truncated := false
	if opt.MaxEntries > 0 && len(mediaEntries) > opt.MaxEntries {
		mediaEntries = mediaEntries[:opt.MaxEntries]
		truncated = true
	}
	for _, fc := range pr.folders {
		if fc.kind == takeout.FolderAlbum && (fc.meta != nil || fc.hasMedia) {
			pr.res.Albums++
		}
	}
	if opt.DryRun {
		pr.res.Status = "dry-run"
		pr.res.Duration = time.Since(start)
		return &pr.res, nil
	}

	if err := ing.st.TouchExport(ctx, acct.ID, part.ExportID, part.PartNo); err != nil {
		return nil, err
	}
	if opt.FullAlbums && opt.Parts > 0 {
		if err := ing.st.SetExportConfirmedParts(ctx, acct.ID, part.ExportID, opt.Parts, true); err != nil {
			return nil, err
		}
	}
	// a part ingested before under this name: when the file is known to differ
	// (hash, else size) or the caller says so, its bookkeeping belongs to the
	// old content and is forgotten; a file of unknown identity resumes
	if prev, err := ing.st.GetArchive(ctx, acct.ID, part.ExportID, part.PartNo); err != nil {
		return nil, err
	} else if prev != nil && (opt.Replace || partDiffers(prev, part)) {
		local := part.RemoteID == "" && part.MD5 == ""
		if local && prev.StagedAt != "" && prev.RemoteDeletedAt == "" {
			// a copy of this part sits in the Drive staging folder (moved by a run
			// whose delete failed); forgetting its id here would strand it, and
			// only sync can finish that deletion
			return nil, fmt.Errorf("part %s has a copy staged for deletion on Drive (id %s) that is not deleted yet: run `gphoto-sync sync` first, then replace it", part.Name, prev.RemoteID)
		}
		ing.log.Warn("part differs from the earlier ingest, starting over", "part", part.Name, "forced", opt.Replace)
		if err := ing.st.ResetPartForReingest(ctx, prev.ID, acct.ID, part.ExportID, part.PartNo); err != nil {
			return nil, err
		}
		if local {
			// a local file of unknown identity: the row must not keep claiming the
			// Drive file it was fetched from, or a later sync could trash that file
			if err := ing.st.ForgetArchiveSource(ctx, prev.ID); err != nil {
				return nil, err
			}
		}
		pr.res.Replaced = true
	}
	arch, err := ing.st.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: part.RemoteID, Name: part.Name, ExportID: part.ExportID,
		PartNo: part.PartNo, Size: part.Size, MD5: part.MD5, Status: store.ArchiveIngesting, LocalPath: part.LocalPath})
	if err != nil {
		return nil, err
	}
	pr.arch = arch
	done, err := ing.st.DoneEntries(ctx, arch.ID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(ing.abs(pr.tmpDir), 0o755); err != nil {
		return nil, err
	}
	// descriptor failures keep the part out of the "ingested" state so it is
	// retried and never deleted remotely
	for _, b := range badEntries {
		pr.failNamed(b.name, b.err)
	}
	prevFailed, err := ing.st.EntryNamesByStatus(ctx, arch.ID, store.EntryFailed, store.EntryGivenUp)
	if err != nil {
		return nil, err
	}
	for _, name := range goodDescriptors {
		if prevFailed[name] {
			_ = ing.st.MarkEntry(ctx, arch.ID, name, store.EntryDone, 0, "")
		}
	}
	if n, err := ing.st.CountExports(ctx, acct.ID); err == nil && n == 1 {
		// an account's first export is the full one and therefore authoritative
		_ = ing.st.SetExportAuthoritative(ctx, acct.ID, part.ExportID)
	}

	// sidecars named after a title are settled against every media file of the
	// folder sharing the stem, including those ingested from earlier parts
	for _, fc := range pr.folders {
		if fc.index.HasTitleKeys() {
			occs, err := ing.st.OccurrencesInFolder(ctx, acct.ID, part.ExportID, fc.name)
			if err != nil {
				return nil, err
			}
			for _, o := range occs {
				if o.ItemID != 0 && o.State != store.OccReset && o.State != store.OccRemoved && !isSidecarOnlyEntry(o.EntryName) {
					fc.index.AddMedia(o.EntryName)
				}
			}
		}
	}

	// albums and late sidecars (media ingested from earlier parts, or from an
	// earlier run of this part whose sidecar could not be read then)
	doneMedia := map[string]bool{} // "<folder>/<base>" of media entries finished by an earlier run
	for _, j := range mediaEntries {
		if done[j.e.Name] {
			doneMedia[j.fc.name+"/"+j.ce.Base] = true
		}
	}
	for _, fc := range pr.folders {
		if fc.kind == takeout.FolderAlbum && (fc.meta != nil || fc.hasMedia) {
			// without metadata in this part, the album this export already
			// attached the folder to wins over a guess by folder name
			var a *store.Album
			if fc.meta == nil {
				if id, err := ing.st.ExportFolderAlbum(ctx, acct.ID, part.ExportID, fc.name); err != nil {
					return nil, err
				} else if id != 0 {
					if a, err = ing.st.GetAlbum(ctx, id); err != nil {
						return nil, err
					}
				}
			}
			if a == nil {
				prevID, err := ing.st.ExportFolderAlbum(ctx, acct.ID, part.ExportID, fc.name)
				if err != nil {
					return nil, err
				}
				if a, err = pr.ensureAlbum(fc); err != nil {
					return nil, fmt.Errorf("album %q: %w", fc.name, err)
				}
				// the record and the move of what an earlier part attached to an
				// album guessed by name change together; the links of the moved
				// items are made (or, after a failure here, made at
				// reconciliation from the occurrences), the stale ones reconciled away
				var moved []int64
				err = ing.st.Tx(ctx, func(tx *store.Store) error {
					if err := tx.SetExportFolderAlbum(ctx, acct.ID, part.ExportID, fc.name, a.ID); err != nil {
						return err
					}
					if prevID != 0 && prevID != a.ID {
						if moved, err = tx.MoveFolderOccurrences(ctx, acct.ID, part.ExportID, fc.name, prevID, a.ID); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
				if len(moved) > 0 {
					if err := pr.linkItemsInto(a, moved); err != nil {
						return nil, err
					}
				}
			}
			fc.album = a
		}
		if err := pr.applyLateSidecars(fc, doneMedia, prevFailed); err != nil {
			return nil, err
		}
	}

	// pass B: extract in parallel, commit sequentially in the sorted order
	// (year folders first), whatever order the workers finish in
	type indexed struct {
		idx int
		x   extracted
	}
	jobs := make(chan indexed)
	results := make(chan indexed, ing.cfg.Workers*2)
	var wg sync.WaitGroup
	for i := 0; i < ing.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				results <- indexed{idx: j.idx, x: pr.extract(j.x.job)}
			}
		}()
	}
	go func() {
		defer close(jobs)
		idx := 0
		for _, j := range mediaEntries {
			// resolve for every entry, including finished ones, so their
			// sidecars count as used and are not stored as pending again
			pr.resolveJob(&j)
			if done[j.e.Name] {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			select {
			case jobs <- indexed{idx: idx, x: extracted{job: j}}:
			case <-ctx.Done():
				return
			}
			idx++
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	buffered := map[int]extracted{}
	next := 0
	for r := range results {
		buffered[r.idx] = r.x
		for {
			x, ok := buffered[next]
			if !ok {
				break
			}
			delete(buffered, next)
			next++
			if err := pr.commit(x); err != nil {
				pr.fail(x.job, err)
				continue
			}
			_ = ing.st.MarkEntry(ctx, arch.ID, x.e.Name, store.EntryDone, 0, "")
			if x.job.pending != "" && !pr.pendingRejected[x.fc.name+"/"+x.job.pending] {
				_ = ing.st.MarkPendingResolved(ctx, acct.ID, part.ExportID, x.fc.name, x.job.pending)
			}
			if m := x.match; m != nil && m.Heuristic && takeout.IsTitleKey(m.Key) && x.job.pending == "" && !pr.pendingRejected[x.fc.name+"/"+m.Key] {
				// a title-named sidecar of this part consumed by its media is kept,
				// as consumed, for the companion a later part may bring (a still
				// after its lone video, a video after its still)
				if err := ing.st.PutPendingSidecar(ctx, acct.ID, part.ExportID, pr.sidecarPart(m.Sidecar), x.fc.name, m.Key, m.Sidecar.Raw, m.Sidecar.GoogleID()); err != nil {
					pr.fail(x.job, err)
					continue
				}
				_ = ing.st.MarkPendingResolved(ctx, acct.ID, part.ExportID, x.fc.name, m.Key)
			}
		}
	}
	for _, x := range buffered { // only when the context was cancelled mid-way
		if x.tmpRel != "" {
			_ = fsops.Unlink(ing.abs(x.tmpRel))
		}
	}
	if ctx.Err() != nil {
		return &pr.res, ctx.Err()
	}

	// sidecars that found no media in this part wait for later parts
	for _, fc := range pr.folders {
		for _, u := range fc.index.Unused() {
			// media already ingested from another part of this export: nothing to
			// wait for, unless that file is another photo than the one the sidecar
			// names (then the sidecar is an album entry of the named photo). An
			// occurrence still waiting for its part's replacement is as good as
			// absent: the replacement may have moved the media to a later part,
			// which must find this sidecar
			if occ, err := ing.st.FindOccurrence(ctx, acct.ID, part.ExportID, fc.name, u.Key); err == nil && occ != nil && occ.ItemID != 0 && occ.State != store.OccReset && occ.State != store.OccRemoved {
				gid := u.SC.GoogleID()
				if gid == "" {
					continue
				}
				if it, err := ing.st.GetItem(ctx, occ.ItemID); err != nil {
					return nil, err
				} else if it == nil || it.GoogleID == gid {
					continue
				}
			}
			if err := ing.st.PutPendingSidecar(ctx, acct.ID, part.ExportID, part.PartNo, fc.name, u.Key, u.SC.Raw, u.SC.GoogleID()); err != nil {
				return nil, err
			}
			pr.res.PendingSidecars++
		}
	}

	counts, err := ing.st.CountEntries(ctx, arch.ID)
	if err != nil {
		return nil, err
	}
	pr.res.Failed = counts[store.EntryFailed]
	pr.res.GivenUp = counts[store.EntryGivenUp]
	status := store.ArchiveIngested
	switch {
	case truncated:
		status = store.ArchivePartial // --max-entries stopped early; a later run continues
	case pr.res.Failed > 0:
		status = store.ArchivePartial
	case pr.res.GivenUp > 0:
		status = store.ArchiveWithErrors
	}
	pr.res.Status = status
	if status == store.ArchiveIngested {
		// memberships are derived from active occurrences; a replaced part (or a
		// resumed ingest of one) may have left direct links nothing supports.
		// This runs before the part is marked ingested so that a failure here
		// leaves it resumable and the reconciliation is tried again
		n, err := ing.ReconcileMemberships(ctx, acct, arch.ID)
		if err != nil {
			return nil, fmt.Errorf("reconcile memberships: %w", err)
		}
		pr.res.Unlinked = n
		if err := ing.st.ClearResetState(ctx, acct.ID, part.ExportID, part.PartNo); err != nil {
			return nil, err
		}
		orphaned, err := ing.st.ReleaseUnconfirmedSidecars(ctx, acct.ID, part.ExportID, part.PartNo)
		if err != nil {
			return nil, err
		}
		if orphaned > 0 {
			ing.log.Warn("sidecars this part supplied before are not in its replacement; their metadata stays but with unknown provenance", "part", part.Name, "count", orphaned)
			pr.res.SidecarsOrphaned = int(orphaned)
		}
	}
	if err := ing.st.FinishArchive(ctx, arch.ID, status, pr.res.Media, counts[store.EntryDone], pr.res.Duplicates, pr.res.Failed+pr.res.GivenUp, pr.recognized); err != nil {
		return nil, err
	}
	// sidecars still waiting may now describe photos this part brought
	links, promoted, err := ing.ResolvePending(ctx, acct, part.ExportID, false)
	if err != nil {
		return nil, fmt.Errorf("resolve pending sidecars: %w", err)
	}
	pr.res.SidecarLinks, pr.res.Promoted = pr.res.SidecarLinks+links, pr.res.Promoted+promoted
	_ = os.Remove(ing.abs(pr.tmpDir))
	pr.res.Duration = time.Since(start)
	return &pr.res, nil
}

func (pr *partRun) folder(ce takeout.Entry) *folderCtx {
	fc, ok := pr.folders[ce.Folder]
	if !ok {
		fc = &folderCtx{name: ce.Folder, kind: ce.FolderKind, year: ce.Year, index: takeout.NewIndex(pr.rules)}
		pr.folders[ce.Folder] = fc
	}
	return fc
}

func (pr *partRun) readSmall(e SourceEntry, limit int64) ([]byte, error) {
	rc, err := pr.src.Open(e)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(io.LimitReader(rc, limit))
}

type failedEntry struct {
	name string
	err  error
}

func (pr *partRun) fail(j job, err error) { pr.failNamed(j.e.Name, err) }

// failNamed records a failed entry; after max_entry_attempts it is given up.
func (pr *partRun) failNamed(name string, err error) {
	ctx, st := pr.ctx, pr.ing.st
	attempts := 1
	if prev, _ := st.GetEntry(ctx, pr.arch.ID, name); prev != nil {
		attempts = prev.Attempts + 1
	}
	status := store.EntryFailed
	if attempts >= pr.ing.cfg.Takeout.MaxEntryAttempts {
		status = store.EntryGivenUp
	}
	pr.ing.log.Error("entry failed", "entry", name, "attempt", attempts, "status", status, "err", err)
	_ = st.MarkEntry(ctx, pr.arch.ID, name, status, attempts, err.Error())
	_ = st.LogError(ctx, pr.acct.ID, pr.part.Name+"!"+name, err.Error())
	if len(pr.res.Errors) < 50 {
		pr.res.Errors = append(pr.res.Errors, fmt.Sprintf("%s: %v", path.Base(name), err))
	}
}

// resolveSidecar finds the sidecar for a media entry in this part's index or
// in sidecars stored by earlier parts of the same export.
func (pr *partRun) resolveSidecar(j job) *takeout.Match {
	if m, ok := j.fc.index.Lookup(j.ce.Base); ok {
		return &m
	}
	for _, k := range takeout.LookupKeys(j.ce.Base, pr.rules) {
		p, err := pr.ing.st.GetPendingSidecar(pr.ctx, pr.acct.ID, pr.part.ExportID, j.fc.name, k.Key)
		if err != nil || p == nil {
			continue
		}
		sc, err := takeout.ParseSidecar(p.Raw)
		if err != nil {
			continue
		}
		sc.SourcePart = p.PartNo
		variant, still := k.Variant, k.Still
		byStem := takeout.IsStemKey(j.ce.Base, k.Key)
		if byStem {
			// a sidecar named after a title without extension: only for a lone
			// file or a still with its video, never for two unrelated files
			// (counting media of earlier parts of the export in the folder)
			cands, err := pr.stemCandidates(j.fc, k.Key)
			if err != nil {
				continue
			}
			var ok bool
			if variant, still, ok = takeout.StemRole(j.ce.Base, cands, pr.rules); !ok {
				continue
			}
		}
		return &takeout.Match{Sidecar: sc, Variant: variant, Still: still, Key: k.Key, Heuristic: byStem, Settled: pr.settled(j.fc.name, k.Key, *p)}
	}
	// a sidecar stored under a truncated media name by an earlier part: the
	// prefix narrows the candidates, the sidecar's own title (the untruncated
	// original name) must equal the media name, and that must be unique
	if cands, err := pr.ing.st.PendingSidecarsByPrefix(pr.ctx, pr.acct.ID, pr.part.ExportID, j.fc.name, j.ce.Base); err == nil && len(cands) > 0 {
		var hit *takeout.Match
		n := 0
		for key, p := range cands {
			sc, err := takeout.ParseSidecar(p.Raw)
			if err != nil || !strings.EqualFold(sc.Title, j.ce.Base) {
				continue
			}
			sc.SourcePart = p.PartNo
			n++
			variant, still := takeout.VariantOriginal, j.ce.Base
			if orig, edited := pr.rules.IsEdited(j.ce.Base); edited {
				variant, still = takeout.VariantEdited, orig
			}
			hit = &takeout.Match{Sidecar: sc, Variant: variant, Still: still, Key: key, Heuristic: true, Settled: pr.settled(j.fc.name, key, p)}
		}
		if n == 1 {
			return hit
		}
	}
	return nil
}

// settled reports whether a stored sidecar was already consumed: flagged so,
// or recorded as an album membership of the photo it names (the flag is
// cleared when a part is replaced; the membership's occurrence is evidence
// that survives).
func (pr *partRun) settled(folder, key string, p store.PendingSidecar) bool {
	if p.Resolved {
		return true
	}
	o, err := pr.ing.st.FindOccurrence(pr.ctx, pr.acct.ID, pr.part.ExportID, folder, sidecarOnlyPrefix+key)
	return err == nil && o != nil && o.State != store.OccReset && o.State != store.OccRemoved
}

// resolveJob resolves the sidecar and remembers whether it came from the
// pending table so the row can be deleted once the entry is committed.
func (pr *partRun) resolveJob(j *job) {
	if m, ok := j.fc.index.Lookup(j.ce.Base); ok {
		j.match = &m
		return
	}
	j.match = pr.resolveSidecar(*j)
	if j.match != nil {
		j.pending = j.match.Key
	}
}

func (pr *partRun) extract(j job) extracted {
	x := extracted{job: j}
	rc, err := pr.src.Open(j.e)
	if err != nil {
		x.err = err
		return x
	}
	defer rc.Close()
	tmpRel := path.Join(pr.tmpDir, fmt.Sprintf("%d-%s", j.e.Offset, fsops.SanitizeName(j.ce.Base, 120)))
	f, err := os.Create(pr.ing.abs(tmpRel))
	if err != nil {
		x.err = err
		return x
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), rc)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(pr.ing.abs(tmpRel))
		x.err = err
		return x
	}
	x.tmpRel, x.size, x.sha = tmpRel, n, hex.EncodeToString(h.Sum(nil))
	x.exifTime, x.exifSrc = media.CaptureTime(pr.ing.abs(tmpRel))
	return x
}

// ---- identity, names, paths ----------------------------------------------

func hash8(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:4])
}

func (pr *partRun) fallbackKey(fc *folderCtx, base string) string {
	return "occ:" + pr.part.ExportID + "#" + fc.name + "#" + base
}

// deriveName picks the on-disk name: the sidecar title for originals, the
// title with "-edited" for edited versions, the entry's own name otherwise.
func deriveName(base string, sc *takeout.Sidecar, variant string) string {
	name := base
	if sc != nil && sc.Title != "" {
		switch variant {
		case takeout.VariantOriginal:
			name = sc.Title
		case takeout.VariantEdited:
			r, ext := takeout.SplitExt(sc.Title)
			name = r + "-edited" + ext
		}
	}
	name = fsops.SanitizeName(name, 200)
	if filepath.Ext(name) == "" {
		if _, ext := takeout.SplitExt(base); ext != "" {
			name += ext
		}
	}
	return name
}

func (pr *partRun) resolveTime(sc *takeout.Sidecar, exifT time.Time, exifSrc string, year int) (time.Time, string) {
	if t, ok := sc.Taken(); ok {
		return t, "sidecar"
	}
	if !exifT.IsZero() {
		if exifSrc == media.SrcExif {
			// EXIF wall clock: anchor in the bucket zone
			y, mo, d := exifT.Date()
			h, mi, s := exifT.Clock()
			return time.Date(y, mo, d, h, mi, s, exifT.Nanosecond(), pr.ing.loc), media.SrcExif
		}
		return exifT, exifSrc
	}
	if year > 0 {
		return time.Date(year, 1, 1, 0, 0, 0, 0, pr.ing.loc), "folder"
	}
	return time.Time{}, ""
}

func (pr *partRun) libraryDir(taken time.Time, src string) string {
	year, month := "0000", "00"
	if !taken.IsZero() {
		t := taken.In(pr.ing.loc)
		year = fmt.Sprintf("%04d", t.Year())
		if src != "folder" {
			month = fmt.Sprintf("%02d", int(t.Month()))
		}
	}
	return path.Join(pr.acct.Name, "library", year, month)
}

func suffixed(name, suffix string) string {
	r, ext := takeout.SplitExt(name)
	return r + "~" + suffix + ext
}

// libraryPath returns a free library path for an item, applying the identity
// suffix on collisions with other items. For an existing item (exceptItem,
// ownPath) its current path is kept whenever it is a candidate. When sha and
// size are given, a path occupied on disk by an unrecorded file is used only
// if that file has exactly this content (adopted after a crash); otherwise it
// is skipped.
func (pr *partRun) libraryPath(dir, name, key string, exceptItem int64, ownPath, sha string, size int64) (p string, adopted bool, err error) {
	cands := []string{path.Join(dir, name), path.Join(dir, suffixed(name, hash8(key))), path.Join(dir, suffixed(name, hash8(key)+hash8(key+"#2")))}
	for _, p := range cands {
		if ownPath != "" && p == ownPath {
			return p, false, nil
		}
		taken, err := pr.ing.st.LibPathTaken(pr.ctx, p, exceptItem)
		if err != nil {
			return "", false, err
		}
		if taken {
			continue
		}
		if vt, _ := pr.ing.st.VersionPathTaken(pr.ctx, p); vt {
			continue
		}
		if fi, err := os.Lstat(pr.ing.abs(p)); err == nil {
			if sha == "" || fi.Size() != size || fileSHA(pr.ing.abs(p)) != sha {
				continue // unrelated file on disk: never overwrite, never adopt
			}
			return p, true, nil
		}
		return p, false, nil
	}
	return "", false, fmt.Errorf("no free path for %s in %s", name, dir)
}

func (pr *partRun) versionPath(libPath, sha string) (string, error) {
	cands := []string{suffixed(libPath, "v"+sha[:8]), suffixed(libPath, "v"+sha[:16])}
	for _, p := range cands {
		if vt, err := pr.ing.st.VersionPathTaken(pr.ctx, p); err != nil {
			return "", err
		} else if vt {
			continue
		}
		if lt, _ := pr.ing.st.LibPathTaken(pr.ctx, p, 0); lt {
			continue
		}
		if _, err := os.Lstat(pr.ing.abs(p)); err == nil {
			continue
		}
		return p, nil
	}
	return "", fmt.Errorf("no free version path for %s", libPath)
}

func (pr *partRun) linkStep(src, dst string) Step {
	switch pr.ing.cfg.Layout.LinkMode {
	case "symlink":
		return Step{Op: "symlink", Src: src, Dst: dst}
	case "copy":
		return Step{Op: "copy", Src: src, Dst: dst}
	}
	return Step{Op: "link", Src: src, Dst: dst}
}

func sidecarPath(libPath string) string { return libPath + ".json" }

// movePath picks the destination for an existing item whose name or capture
// time changed. Its current path is kept when it still fits; a destination
// already holding the item's own bytes is adopted (a crash between the
// journaled rename and its database commit leaves the file there after journal
// replay while the item still records the old path), so the rename step then
// finds its work done and the database catches up.
func (pr *partRun) movePath(item *store.Item, blob *store.Blob, dir, name, key string) (string, error) {
	sha, size := "", int64(0)
	if blob != nil {
		sha, size = blob.SHA256, blob.Size
	}
	p, adopted, err := pr.libraryPath(dir, name, key, item.ID, item.LibPath, sha, size)
	if err != nil {
		return "", err
	}
	if adopted {
		pr.ing.log.Warn("destination already holds the item's bytes, adopting it", "from", item.LibPath, "to", p)
	}
	return p, nil
}

// symlinkMoveSteps re-points an item's album symlinks when its library path
// moves; hardlinks follow the inode and copies are independent, so only
// symlink mode needs this.
// linkMoveSteps makes an item's album links follow its library file to a new
// path: when the file name changes the links are renamed as well (hardlinks
// and copies keep their inode or bytes), symlinks are re-pointed. It returns
// the file steps and the membership rows whose link_path changes, to be
// written in the same transaction as the item.
func (pr *partRun) linkMoveSteps(item *store.Item, newPath string) ([]Step, []*store.AlbumItem, error) {
	ais, err := pr.ing.st.AlbumItemsByItem(pr.ctx, item.ID)
	if err != nil {
		return nil, nil, err
	}
	var steps []Step
	var moved []*store.AlbumItem
	nameChanged := path.Base(newPath) != path.Base(item.LibPath)
	for _, ai := range ais {
		newLink := ai.LinkPath
		if nameChanged {
			album, err := pr.ing.st.GetAlbum(pr.ctx, ai.AlbumID)
			if err != nil {
				return nil, nil, err
			}
			if album == nil {
				continue
			}
			// the file may already sit at either path (journal replay after a crash)
			if newLink, err = pr.linkPathForName(album, item, path.Base(newPath), item.LibPath, newPath); err != nil {
				return nil, nil, err
			}
		}
		switch {
		case pr.ing.cfg.Layout.LinkMode == "symlink":
			steps = append(steps, Step{Op: "unlink", Dst: ai.LinkPath}, Step{Op: "symlink", Src: newPath, Dst: newLink})
		case newLink != ai.LinkPath:
			steps = append(steps, Step{Op: "rename", Src: ai.LinkPath, Dst: newLink})
		}
		if newLink != ai.LinkPath {
			ai.LinkPath = newLink
			moved = append(moved, ai)
		}
	}
	return steps, moved, nil
}

func (pr *partRun) writeSidecarFile(libPath string, sc *takeout.Sidecar) error {
	if sc == nil || !pr.ing.cfg.Layout.KeepSidecarJSON {
		return nil
	}
	if err := fsops.WriteFileAtomic(pr.ing.abs(sidecarPath(libPath)), sc.Raw, 0o644); err != nil {
		return fmt.Errorf("sidecar write %s: %w", sidecarPath(libPath), err)
	}
	return nil
}

// sidecarPart is the part a sidecar in hand came from: the pending store's
// part when it was taken from there, else this part.
func (pr *partRun) sidecarPart(sc *takeout.Sidecar) int {
	if sc != nil && sc.SourcePart != 0 {
		return sc.SourcePart
	}
	return pr.part.PartNo
}

func (pr *partRun) saveSidecar(tx *store.Store, itemID int64, sc *takeout.Sidecar) error {
	return pr.saveSidecarAs(tx, itemID, sc, pr.part.ExportID, pr.sidecarPart(sc))
}

// saveSidecarAs stores a sidecar with an explicit provenance.
func (pr *partRun) saveSidecarAs(tx *store.Store, itemID int64, sc *takeout.Sidecar, exportID string, partNo int) error {
	if sc == nil {
		return nil
	}
	var lat, lon, alt *float64
	if g := sc.Location(); g != nil {
		lat, lon, alt = &g.Latitude, &g.Longitude, &g.Altitude
	}
	people := ""
	if names := sc.PeopleNames(); len(names) > 0 {
		b, _ := json.Marshal(names)
		people = string(b)
	}
	return tx.UpsertSidecar(pr.ctx, itemID, sc.Raw, sc.SHA256, sc.Description, sc.Favorited, lat, lon, alt, people, exportID, partNo)
}

// ---- commit ----------------------------------------------------------------

func (pr *partRun) commit(x extracted) (err error) {
	if x.err != nil {
		return x.err
	}
	journalBefore := pr.ing.journalCount.Load()
	defer func() {
		// the temp file is removed after success, or after a failure that
		// happened before any journal entry could refer to it; otherwise it
		// stays for journal recovery at the next start
		if x.tmpRel != "" && (err == nil || pr.ing.journalCount.Load() == journalBefore) {
			_ = fsops.Unlink(pr.ing.abs(x.tmpRel))
		}
	}()
	ctx, st, fc := pr.ctx, pr.ing.st, x.fc
	base := x.ce.Base
	var sc *takeout.Sidecar
	variant := takeout.VariantOriginal
	if x.match != nil {
		sc, variant = x.match.Sidecar, x.match.Variant
	}
	googleID := sc.GoogleID()
	name := deriveName(base, sc, variant)
	var key, src string
	if googleID != "" {
		key, src = googleID+"#"+variant, store.IdentityGoogle
	} else {
		key, src = pr.fallbackKey(fc, base), store.IdentityFallback
	}

	if x.match != nil && googleID != "" && (x.match.Heuristic || x.match.Settled) {
		mates, err := pr.stemCandidates(fc, x.match.Key)
		if err != nil {
			return err
		}
		if err := pr.adoptPairStill(fc.name, x.match, base, mates); err != nil {
			return err
		}
		// a sidecar that relates to this file by a name heuristic (a title, a
		// truncated name) or a stored one already consumed as an album membership
		// of the photo it names, while that photo (any variant of it) is already
		// known with other bytes: whether this file is a new version of the photo
		// or an unrelated file sharing the name cannot be told until the rest of
		// the export is seen (ResolvePending decides: the photo delivered
		// elsewhere in the export makes this an album entry, a complete export
		// without it makes this the new version). Until then the file stays
		// nameless and the sidecar is kept
		if known, collides, err := pr.nameCollision(fc.name, x.match, base, x.sha, mates); err != nil {
			return err
		} else if collides {
			pr.ing.log.Info("sidecar names a known photo with other bytes, decided once the export is complete", "entry", x.e.Name, "google_id", googleID, "known", known.LibPath)
			if err := st.PutPendingSidecar(ctx, pr.acct.ID, pr.part.ExportID, pr.sidecarPart(sc), fc.name, x.match.Key, sc.Raw, googleID); err != nil {
				return err
			}
			if err := st.DeferPendingSidecar(ctx, pr.acct.ID, pr.part.ExportID, fc.name, x.match.Key, base, x.match.Variant, x.match.Still); err != nil {
				return err
			}
			if pr.pendingRejected == nil {
				pr.pendingRejected = map[string]bool{}
			}
			pr.pendingRejected[fc.name+"/"+x.match.Key] = true
			x.match, sc, googleID, variant = nil, nil, "", takeout.VariantOriginal
			name = deriveName(base, nil, variant)
			key, src = pr.fallbackKey(fc, base), store.IdentityFallback
		}
	}
	item, err := st.FindItemByKey(ctx, pr.acct.ID, key)
	if err != nil {
		return err
	}
	if item == nil && googleID != "" {
		fbs, err := st.FindFallbackItems(ctx, pr.acct.ID, x.sha, base, variant)
		if err != nil {
			return err
		}
		switch len(fbs) {
		case 1:
			item, err = pr.promote(fbs[0], sc, variant, googleID, key)
			if err != nil {
				return fmt.Errorf("promote %s: %w", base, err)
			}
		case 0:
		default:
			pr.res.Unresolved++
			pr.ing.log.Warn("ambiguous fallback items, not promoting", "entry", x.e.Name, "candidates", len(fbs))
		}
	}
	if item != nil && googleID != "" && variant == takeout.VariantOriginal && item.Variant == takeout.VariantOriginal && !strings.EqualFold(item.Filename, name) {
		_, e1 := takeout.SplitExt(item.Filename)
		_, e2 := takeout.SplitExt(name)
		if !strings.EqualFold(e1, e2) {
			// a second file sharing the still's Google id (e.g. Live Photo video with its own sidecar)
			variant = "alt:" + strings.ToLower(strings.TrimPrefix(e2, "."))
			key = googleID + "#" + variant
			if item, err = st.FindItemByKey(ctx, pr.acct.ID, key); err != nil {
				return err
			}
		}
	}

	if item != nil {
		if err := pr.existing(item, x, sc, variant, name); err != nil {
			return err
		}
	} else {
		item, err = pr.create(x, sc, variant, name, key, src, googleID)
		if err != nil {
			return err
		}
	}

	var albumID int64
	if fc.kind == takeout.FolderAlbum && fc.album != nil {
		albumID = fc.album.ID
	}
	occState := store.OccIngested
	if src == store.IdentityGoogle {
		occState = store.OccResolved
	}
	if _, err := st.UpsertOccurrence(ctx, &store.Occurrence{AccountID: pr.acct.ID, ExportID: pr.part.ExportID, PartNo: pr.part.PartNo,
		Folder: fc.name, EntryName: base, Variant: variant, SHA256: x.sha, AlbumID: albumID, ItemID: item.ID, State: occState}); err != nil {
		return err
	}
	if err := pr.updateArchived(item, fc.kind); err != nil {
		return err
	}
	if albumID != 0 {
		if _, err := pr.ensureDirect(fc.album, item); err != nil {
			return fmt.Errorf("album link: %w", err)
		}
		// whether linked (by newer evidence) or excluded by a newer complete
		// listing, occurrences older than that listing are history, never
		// support for a membership
		if err := pr.retireOccurrences(fc.album, item); err != nil {
			return err
		}
	}
	return pr.ensureGroupLinks(item)
}

// linkItemsInto links items whose occurrences were re-attached to an album.
func (pr *partRun) linkItemsInto(to *store.Album, items []int64) error {
	for _, id := range items {
		item, err := pr.ing.st.GetItem(pr.ctx, id)
		if err != nil || item == nil {
			continue
		}
		if _, err := pr.ensureDirectConfirmed(to, item, pr.part.ExportID); err != nil {
			return fmt.Errorf("re-attaching %s to album %s: %w", item.LibPath, to.Path, err)
		}
		if err := pr.ensureGroupLinks(item); err != nil {
			return err
		}
	}
	pr.ing.log.Info("folder re-attached to the album its metadata names", "album", to.Path, "items", len(items))
	return nil
}

// retireOccurrences deactivates the active occurrences of an (album, item)
// pair that are older than the newest applied complete listing, so that they
// never count as support for a membership (a later replacement of a newer
// part must not find them and keep a stale link).
func (pr *partRun) retireOccurrences(album *store.Album, item *store.Item) error {
	applied, err := pr.appliedListing(album)
	if err != nil || applied == "" {
		return err
	}
	occs, err := pr.ing.st.OccurrencesForMembership(pr.ctx, album.ID, item.ID)
	if err != nil {
		return err
	}
	for _, o := range occs {
		if ExportOlder(o.ExportID, applied) {
			if err := pr.ing.st.DeactivateOccurrence(pr.ctx, o.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (pr *partRun) create(x extracted, sc *takeout.Sidecar, variant, name, key, src, googleID string) (*store.Item, error) {
	ctx, st := pr.ctx, pr.ing.st
	taken, takenSrc := pr.resolveTime(sc, x.exifTime, x.exifSrc, x.fc.year)
	dir := pr.libraryDir(taken, takenSrc)
	libPath, adopted, err := pr.libraryPath(dir, name, key, 0, "", x.sha, x.size)
	if err != nil {
		return nil, err
	}
	if adopted {
		pr.ing.log.Warn("adopting file left by an earlier run", "path", libPath)
	}
	blob, err := st.FindBlob(ctx, x.sha)
	if err != nil {
		return nil, err
	}
	var steps []Step
	newBlob := false
	switch {
	case adopted:
		steps = []Step{{Op: "unlink", Dst: x.tmpRel}}
	case blob != nil && exists(pr.ing.abs(blob.CanonicalPath)):
		// library files always share bytes by hardlink, whatever the album link mode
		steps = []Step{{Op: "link", Src: blob.CanonicalPath, Dst: libPath}, {Op: "unlink", Dst: x.tmpRel}}
	default:
		steps = []Step{{Op: "rename", Src: x.tmpRel, Dst: libPath}}
		newBlob = blob == nil
	}
	groupID := key
	if googleID != "" {
		groupID = googleID + "#" + takeout.VariantOriginal
	}
	it := &store.Item{AccountID: pr.acct.ID, GoogleID: googleID, Variant: variant, IdentityKey: key, IdentitySrc: src, Filename: name,
		Ext: strings.ToLower(filepath.Ext(name)), TakenAt: taken, TakenSrc: takenSrc, LibPath: libPath, GroupID: groupID, BytesExportID: pr.part.ExportID}
	err = pr.ing.journaled(ctx, "create", steps, func(tx *store.Store) error {
		if blob == nil {
			b, err := tx.InsertBlob(ctx, x.sha, x.size, libPath)
			if err != nil {
				return err
			}
			blob = b
		} else if !exists(pr.ing.abs(blob.CanonicalPath)) {
			if err := tx.SetBlobCanonical(ctx, blob.ID, libPath); err != nil {
				return err
			}
		}
		it.BlobID = blob.ID
		id, err := tx.InsertItem(ctx, it)
		if err != nil {
			return err
		}
		it.ID = id
		return pr.saveSidecar(tx, id, sc)
	})
	if err != nil {
		return nil, err
	}
	if newBlob {
		pr.ing.setMtime(libPath, taken)
	}
	if err := pr.writeSidecarFile(libPath, sc); err != nil {
		return nil, err // the item exists; the retry repairs the sidecar file in existing()
	}
	pr.res.Added++
	return it, nil
}

func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

func fileSHA(p string) string {
	f, err := os.Open(p)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

// existing handles another occurrence of a known item: unchanged (just a
// membership), changed sidecar, or changed bytes (supersede). Parts are not
// guaranteed to arrive in export order: content from an export older than the
// one that last supplied the item's bytes or sidecar never replaces them; the
// bytes are kept as a version and the sidecar is ignored.
func (pr *partRun) existing(item *store.Item, x extracted, sc *takeout.Sidecar, variant, name string) error {
	ctx, st := pr.ctx, pr.ing.st
	blob, err := st.GetBlob(ctx, item.BlobID)
	if err != nil {
		return err
	}
	changed, versioned := false, false
	switch {
	case blob != nil && blob.SHA256 != x.sha && pr.olderThan(item.BytesExportID):
		kept, err := pr.keepAsVersion(item, x)
		if err != nil {
			return fmt.Errorf("keep version: %w", err)
		}
		versioned = kept
	case blob != nil && blob.SHA256 != x.sha:
		if err := pr.supersede(item, blob, x); err != nil {
			return fmt.Errorf("supersede: %w", err)
		}
		changed = true
	case item.BytesExportID == "" || ExportOlder(item.BytesExportID, pr.part.ExportID):
		// the same bytes, confirmed by a newer export
		item.BytesExportID = pr.part.ExportID
		if err := st.UpdateItem(ctx, item); err != nil {
			return err
		}
	}
	if sc != nil {
		scChanged, err := pr.applySidecar(item, sc, name)
		if err != nil {
			return err
		}
		changed = changed || scChanged
	}
	switch {
	case changed:
		pr.res.Changed++
	case versioned:
		pr.res.Versioned++
	default:
		pr.res.Duplicates++
	}
	return st.TouchItem(ctx, item.ID)
}

// olderThan reports whether this part's export is older than exportID (an
// unknown exportID never counts as newer).
func (pr *partRun) olderThan(exportID string) bool {
	return exportID != "" && ExportOlder(pr.part.ExportID, exportID)
}

// applySidecar stores a sidecar for an item unless the stored one came from a
// newer export. It reports whether the stored sidecar changed.
func (pr *partRun) applySidecar(item *store.Item, sc *takeout.Sidecar, name string) (bool, error) {
	return pr.applySidecarAs(item, sc, name, pr.part.ExportID, pr.sidecarPart(sc))
}

// applySidecarAs is applySidecar for a sidecar of explicit provenance (an
// empty exportID means unknown: such a sidecar only ever fills a void).
func (pr *partRun) applySidecarAs(item *store.Item, sc *takeout.Sidecar, name, exportID string, partNo int) (bool, error) {
	ctx, st := pr.ctx, pr.ing.st
	cur, err := st.GetSidecar(ctx, item.ID)
	if err != nil {
		return false, err
	}
	switch {
	case cur != nil && cur.SHA256 == sc.SHA256:
		switch {
		case exportID == "":
			// unknown provenance vouches for nothing
		case cur.ExportID == "" || ExportOlder(cur.ExportID, exportID):
			// unknown or older provenance: this export now vouches for it
			if err := st.SetSidecarExport(ctx, item.ID, exportID, partNo); err != nil {
				return false, err
			}
		case cur.ExportID == exportID:
			// the same export carries it: the part in hand is its source (it
			// confirms a part being ingested again, or takes over from one)
			if err := st.SetSidecarExport(ctx, item.ID, exportID, partNo); err != nil {
				return false, err
			}
			// an older export carrying the same sidecar vouches for nothing
		}
	case cur != nil && (exportID == "" || (cur.ExportID != "" && ExportOlder(exportID, cur.ExportID))):
		// an older (or unknown) view of the metadata; the stored one stays
		pr.ing.log.Debug("ignoring sidecar not newer than the stored one", "item", item.LibPath, "export", exportID, "kept", cur.ExportID)
		sc = &takeout.Sidecar{Raw: cur.JSON}
	default:
		if err := pr.updateSidecarAs(item, sc, name, exportID, partNo); err != nil {
			return false, fmt.Errorf("sidecar update: %w", err)
		}
		return true, nil
	}
	if pr.ing.cfg.Layout.KeepSidecarJSON && !exists(pr.ing.abs(sidecarPath(item.LibPath))) {
		// the file beside the item went missing (or a crashed run never wrote it)
		if err := pr.writeSidecarFile(item.LibPath, sc); err != nil {
			return false, err
		}
	}
	return false, nil
}

// keepAsVersion stores bytes that differ from an item's current content
// without replacing it: they come from an export older than the one that
// supplied the current bytes. The version path is a function of the content so
// that a retry after a crash finds the same file. It reports whether a new
// version was recorded.
func (pr *partRun) keepAsVersion(item *store.Item, x extracted) (bool, error) {
	ctx, st := pr.ctx, pr.ing.st
	blob, err := st.FindBlob(ctx, x.sha)
	if err != nil {
		return false, err
	}
	if blob != nil {
		versions, err := st.VersionsByItem(ctx, item.ID)
		if err != nil {
			return false, err
		}
		for _, v := range versions {
			if v.BlobID == blob.ID && exists(pr.ing.abs(v.Path)) {
				return false, nil // these bytes are already kept for this item
			}
		}
	}
	verPath := suffixed(item.LibPath, "v"+x.sha[:8])
	prev, err := st.VersionByPath(ctx, verPath)
	if err != nil {
		return false, err
	}
	if prev != nil && (prev.ItemID != item.ID || blob == nil || prev.BlobID != blob.ID) {
		return false, fmt.Errorf("version path %s belongs to another item or content", verPath)
	}
	placed := false // a crashed earlier attempt already put the bytes there
	if exists(pr.ing.abs(verPath)) {
		if fileSHA(pr.ing.abs(verPath)) != x.sha {
			return false, fmt.Errorf("version path %s exists with unexpected content", verPath)
		}
		placed = true
	}
	var steps []Step
	switch {
	case placed:
		steps = []Step{{Op: "unlink", Dst: x.tmpRel}}
	case blob != nil && exists(pr.ing.abs(blob.CanonicalPath)):
		steps = []Step{{Op: "link", Src: blob.CanonicalPath, Dst: verPath}, {Op: "unlink", Dst: x.tmpRel}}
	default:
		steps = []Step{{Op: "rename", Src: x.tmpRel, Dst: verPath}}
	}
	pr.ing.log.Info("older export carries other bytes, kept as a version", "item", item.LibPath, "version", verPath, "export", pr.part.ExportID, "current", item.BytesExportID)
	err = pr.ing.journaled(ctx, "keep-version", steps, func(tx *store.Store) error {
		if blob == nil {
			b, err := tx.InsertBlob(ctx, x.sha, x.size, verPath)
			if err != nil {
				return err
			}
			blob = b
		} else if !exists(pr.ing.abs(blob.CanonicalPath)) {
			if err := tx.SetBlobCanonical(ctx, blob.ID, verPath); err != nil {
				return err
			}
		}
		if prev == nil {
			return tx.InsertVersion(ctx, item.ID, blob.ID, verPath, pr.part.ExportID)
		}
		return nil
	})
	return err == nil, err
}

func (pr *partRun) supersede(item *store.Item, oldBlob *store.Blob, x extracted) error {
	ctx, st := pr.ctx, pr.ing.st
	// the version path is a pure function of the old content so that a retry
	// after a crash, or a later A->B->A->B history, finds the same name
	verPath := suffixed(item.LibPath, "v"+oldBlob.SHA256[:8])
	prevVer, err := st.VersionByPath(ctx, verPath)
	if err != nil {
		return err
	}
	if prevVer != nil && (prevVer.BlobID != oldBlob.ID || prevVer.ItemID != item.ID) {
		return fmt.Errorf("version path %s belongs to another item or content", verPath)
	}
	alreadyVersioned := prevVer != nil // these bytes were kept before: just drop the current copy
	oldMoved := false                  // a crashed earlier attempt already moved the old bytes aside
	if exists(pr.ing.abs(verPath)) {
		if fileSHA(pr.ing.abs(verPath)) != oldBlob.SHA256 {
			return fmt.Errorf("version path %s exists with unexpected content", verPath)
		}
		oldMoved = true
	}
	libSHA := ""
	if oldMoved || alreadyVersioned {
		libSHA = fileSHA(pr.ing.abs(item.LibPath))
	}
	newPlaced := libSHA == x.sha
	newBlob, err := st.FindBlob(ctx, x.sha)
	if err != nil {
		return err
	}
	ais, err := st.AlbumItemsByItem(ctx, item.ID)
	if err != nil {
		return err
	}
	var steps []Step
	switch {
	case newPlaced:
	case alreadyVersioned && oldMoved:
		steps = append(steps, Step{Op: "unlink", Dst: item.LibPath})
	case !oldMoved:
		// SHA lets journal replay recognise the move as done even after the
		// library path has been filled with the new bytes
		steps = append(steps, Step{Op: "rename", Src: item.LibPath, Dst: verPath, SHA: oldBlob.SHA256})
	}
	switch {
	case newPlaced:
	case newBlob != nil && exists(pr.ing.abs(newBlob.CanonicalPath)):
		steps = append(steps, Step{Op: "link", Src: newBlob.CanonicalPath, Dst: item.LibPath})
	case x.tmpRel == "":
		return fmt.Errorf("no file holds the new bytes %s", x.sha[:8])
	default:
		steps = append(steps, Step{Op: "rename", Src: x.tmpRel, Dst: item.LibPath})
	}
	if x.tmpRel != "" && (newPlaced || (newBlob != nil && exists(pr.ing.abs(newBlob.CanonicalPath)))) {
		steps = append(steps, Step{Op: "unlink", Dst: x.tmpRel}) // the rename case consumes it
	}
	for _, ai := range ais {
		steps = append(steps, Step{Op: "unlink", Dst: ai.LinkPath}, pr.linkStep(item.LibPath, ai.LinkPath))
	}
	pr.ing.log.Info("bytes changed, keeping previous version", "item", item.LibPath, "version", verPath)
	return pr.ing.journaled(ctx, "supersede", steps, func(tx *store.Store) error {
		if oldBlob.CanonicalPath == item.LibPath {
			if err := tx.SetBlobCanonical(ctx, oldBlob.ID, verPath); err != nil {
				return err
			}
		}
		if !alreadyVersioned {
			if err := tx.InsertVersion(ctx, item.ID, oldBlob.ID, verPath, pr.part.ExportID); err != nil {
				return err
			}
		}
		if newBlob == nil {
			b, err := tx.InsertBlob(ctx, x.sha, x.size, item.LibPath)
			if err != nil {
				return err
			}
			newBlob = b
		} else if !exists(pr.ing.abs(newBlob.CanonicalPath)) {
			if err := tx.SetBlobCanonical(ctx, newBlob.ID, item.LibPath); err != nil {
				return err
			}
		}
		item.BlobID, item.BytesExportID = newBlob.ID, pr.part.ExportID
		if x.exportID != "" {
			item.BytesExportID = x.exportID
		}
		return tx.UpdateItem(ctx, item)
	})
}

// updateSidecar stores a changed sidecar and moves the file if its name or
// capture month changed.
func (pr *partRun) updateSidecar(item *store.Item, sc *takeout.Sidecar, name string) error {
	return pr.updateSidecarAs(item, sc, name, pr.part.ExportID, pr.sidecarPart(sc))
}

// updateSidecarAs is updateSidecar with an explicit provenance.
func (pr *partRun) updateSidecarAs(item *store.Item, sc *takeout.Sidecar, name, exportID string, partNo int) error {
	ctx, st := pr.ctx, pr.ing.st
	taken, takenSrc := item.TakenAt, item.TakenSrc
	if t, ok := sc.Taken(); ok {
		taken, takenSrc = t, "sidecar"
	}
	blob, err := st.GetBlob(ctx, item.BlobID)
	if err != nil {
		return err
	}
	newPath := item.LibPath
	if name != item.Filename || !taken.Equal(item.TakenAt) {
		dir := pr.libraryDir(taken, takenSrc)
		p, err := pr.movePath(item, blob, dir, name, item.IdentityKey)
		if err != nil {
			return err
		}
		newPath = p
	}
	var steps []Step
	oldPath := item.LibPath
	var movedLinks []*store.AlbumItem
	if newPath != oldPath {
		steps = append(steps, Step{Op: "rename", Src: oldPath, Dst: newPath}, Step{Op: "unlink", Dst: sidecarPath(oldPath)})
		more, moved, err := pr.linkMoveSteps(item, newPath)
		if err != nil {
			return err
		}
		steps, movedLinks = append(steps, more...), moved
	}
	err = pr.ing.journaled(ctx, "sidecar", steps, func(tx *store.Store) error {
		item.Filename, item.TakenAt, item.TakenSrc, item.LibPath = name, taken, takenSrc, newPath
		if blob != nil && blob.CanonicalPath == oldPath && newPath != oldPath {
			if err := tx.SetBlobCanonical(ctx, blob.ID, newPath); err != nil {
				return err
			}
		}
		if err := tx.UpdateItem(ctx, item); err != nil {
			return err
		}
		for _, ai := range movedLinks {
			if err := tx.UpsertAlbumItem(ctx, ai); err != nil {
				return err
			}
		}
		return pr.saveSidecarAs(tx, item.ID, sc, exportID, partNo)
	})
	if err != nil {
		return err
	}
	return pr.writeSidecarFile(newPath, sc)
}

// ---- promotion and merge ---------------------------------------------------

// applyLateSidecars applies sidecars that arrive after their media was
// committed: media from an earlier part of this export, or media of this part
// finished by an earlier run whose sidecar could not be read then. Only items
// without any stored sidecar are touched (fallback or not), so the index's
// fuzzy rules (title, stem prefix) can never replace metadata or an identity
// that an exact match supplied.
func (pr *partRun) applyLateSidecars(fc *folderCtx, doneMedia, prevFailed map[string]bool) error {
	ctx, st := pr.ctx, pr.ing.st
	occs, err := st.OccurrencesInFolder(ctx, pr.acct.ID, pr.part.ExportID, fc.name)
	if err != nil {
		return err
	}
	for _, o := range occs {
		// inactive occurrences count too: a membership withdrawn by a newer
		// listing says nothing about the item's identity or metadata, and media
		// seen only in that album would otherwise never get its sidecar. Those
		// waiting for their part's replacement are left to that re-ingest, and
		// entries a replacement dropped are not in the export any more
		if o.ItemID == 0 || o.State == store.OccReset || o.State == store.OccRemoved || isSidecarOnlyEntry(o.EntryName) {
			continue
		}
		if o.PartNo == pr.part.PartNo && !doneMedia[fc.name+"/"+o.EntryName] {
			continue // pass B commits this entry together with its sidecar
		}
		item, err := st.GetItem(ctx, o.ItemID)
		if err != nil || item == nil {
			continue
		}
		cur, err := st.GetSidecar(ctx, item.ID)
		if err != nil {
			return err
		}
		var m takeout.Match
		var ok bool
		switch {
		case cur == nil:
			// the lookup also marks the sidecar used, so a re-run never stores it as pending
			m, ok = fc.index.Lookup(o.EntryName)
		case o.PartNo != pr.part.PartNo && (cur.ExportID == "" || (cur.ExportID == pr.part.ExportID && cur.PartNo == pr.part.PartNo)):
			// media in another part whose stored sidecar this part supplied (being
			// ingested again, a replacement) or whose sidecar has no known source
			// (released by an earlier replacement, or migrated): an exact sidecar
			// confirms it, or corrects it
			if m, ok = fc.index.Exact(o.EntryName); ok && m.Sidecar.SHA256 == cur.SHA256 {
				if err := st.SetSidecarExport(ctx, item.ID, pr.part.ExportID, pr.part.PartNo); err != nil {
					return err
				}
				ok = false
			}
		}
		if !ok {
			continue
		}
		if m.Heuristic {
			mates, err := pr.stemCandidates(fc, m.Key)
			if err != nil {
				return err
			}
			if err := pr.adoptPairStill(fc.name, &m, o.EntryName, mates); err != nil {
				return err
			}
			if _, collides, err := pr.nameCollision(fc.name, &m, o.EntryName, pr.itemSHA(item), mates); err != nil {
				return err
			} else if collides {
				// decided by ResolvePending once the export is seen (the index marked
				// the sidecar used, so it is stored here)
				if err := st.PutPendingSidecar(ctx, pr.acct.ID, pr.part.ExportID, pr.sidecarPart(m.Sidecar), fc.name, m.Key, m.Sidecar.Raw, m.Sidecar.GoogleID()); err != nil {
					return err
				}
				if err := st.DeferPendingSidecar(ctx, pr.acct.ID, pr.part.ExportID, fc.name, m.Key, o.EntryName, m.Variant, m.Still); err != nil {
					return err
				}
				continue
			}
		}
		entry := "promotion:" + fc.name + "/" + o.EntryName
		acted, err := pr.applyLateSidecar(item, o, m)
		if err != nil {
			pr.failNamed(entry, fmt.Errorf("late sidecar: %w", err))
			continue
		}
		if acted || prevFailed[entry] {
			_ = st.MarkEntry(ctx, pr.arch.ID, entry, store.EntryDone, 0, "")
		}
		if m.Heuristic && takeout.IsTitleKey(m.Key) {
			// a title-named sidecar applied to media of an earlier part is kept,
			// as consumed, for the companion a later part may bring: a still
			// after its lone video (which then becomes the motion companion), a
			// video after its still
			if err := st.PutPendingSidecar(ctx, pr.acct.ID, pr.part.ExportID, pr.sidecarPart(m.Sidecar), fc.name, m.Key, m.Sidecar.Raw, m.Sidecar.GoogleID()); err != nil {
				return err
			}
			if err := st.MarkPendingResolved(ctx, pr.acct.ID, pr.part.ExportID, fc.name, m.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolvePending goes over an export's stored sidecars that found no media in
// their folder. Takeout writes the JSON of a photo into every album folder
// but the photo itself into one folder only, so such a sidecar in an album
// folder whose Google id names a known photo is an album membership (recorded
// as an occurrence without bytes, like any other); and a sidecar whose media
// is in the folder under a name the index could not relate at the time (a
// title without extension), or that a part related by a heuristic and
// deferred, is applied to that item. A nameless file sharing
// the name of a photo already known with other bytes is an album entry of
// that photo once the photo turns up elsewhere in the export, and the photo's
// new version only once the export is complete (complete) without it. It
// returns the number of album links and promotions made.
//
// A promotion may name the photo an album's stored sidecar waits for, so
// passes repeat while promotions happen.
func (ing *Ingester) ResolvePending(ctx context.Context, acct *store.Account, exportID string, complete bool) (links, promoted int, err error) {
	for pass := 0; pass < 4; pass++ {
		l, p, err := ing.resolvePendingPass(ctx, acct, exportID, complete)
		links, promoted = links+l, promoted+p
		if err != nil || p == 0 {
			return links, promoted, err
		}
	}
	return links, promoted, nil
}

func (ing *Ingester) resolvePendingPass(ctx context.Context, acct *store.Account, exportID string, complete bool) (links, promoted int, err error) {
	rules := ing.rulesFor(acct.Name)
	rows, err := ing.st.PendingSidecarsOfExport(ctx, acct.ID, exportID)
	if err != nil || len(rows) == 0 {
		return 0, 0, err
	}
	byFolder := map[string][]store.PendingRow{}
	for _, r := range rows {
		byFolder[r.Folder] = append(byFolder[r.Folder], r)
	}
	for folder, prs := range byFolder {
		kind := takeout.ClassifyFolder(folder, rules)
		occs, err := ing.st.OccurrencesInFolder(ctx, acct.ID, exportID, folder)
		if err != nil {
			return links, promoted, err
		}
		byEntry := map[string]*store.Occurrence{}
		for _, o := range occs {
			if o.ItemID != 0 && o.State != store.OccReset && o.State != store.OccRemoved && !isSidecarOnlyEntry(o.EntryName) {
				byEntry[strings.ToLower(o.EntryName)] = o
			}
		}
		excluded := (kind == takeout.FolderTrash && !ing.cfg.Takeout.IncludeTrash) || (kind == takeout.FolderArchive && !ing.cfg.Takeout.IncludeArchive)
		if excluded && len(byEntry) == 0 {
			// sidecars of an excluded folder (stored before the exclusion covered
			// sidecars) whose media the export never ingested: nothing waits for
			// them. A folder ingested while it was included keeps its rows: its
			// media may still need them
			for _, r := range prs {
				if err := ing.st.DeletePendingSidecar(ctx, acct.ID, exportID, folder, r.Key); err != nil {
					return links, promoted, err
				}
			}
			continue
		}
		// the album this export attached the folder to (recorded when the folder
		// was seen; a folder name recurs across exports for different albums);
		// for exports ingested before that record existed, the only album with
		// that folder, else none
		var album *store.Album
		albumID, err := ing.st.ExportFolderAlbum(ctx, acct.ID, exportID, folder)
		if err != nil {
			return links, promoted, err
		}
		if albumID != 0 {
			if album, err = ing.st.GetAlbum(ctx, albumID); err != nil {
				return links, promoted, err
			}
		} else if albums, err := ing.st.AlbumsByFolder(ctx, acct.ID, folder); err != nil {
			return links, promoted, err
		} else if len(albums) == 1 {
			// a guess by folder name, recorded as provisional: metadata of this
			// export arriving later names another album and moves the folder
			album = albums[0]
			if err := ing.st.SetExportFolderAlbum(ctx, acct.ID, exportID, folder, album.ID); err != nil {
				return links, promoted, err
			}
		}
		for _, r := range prs {
			sc, err := takeout.ParseSidecar(r.Raw)
			if err != nil {
				continue
			}
			sc.SourcePart = r.PartNo
			pr := &partRun{ing: ing, ctx: ctx, acct: acct, part: PartMeta{Account: acct.Name, ExportID: exportID, PartNo: r.PartNo, Name: "pending:" + r.Key}, folders: map[string]*folderCtx{}, rules: rules}
			// the media the sidecar relates to: the entry a heuristic match of a
			// part was deferred for (recorded with its variant), and media under a
			// name the index could not relate at the time, a title without
			// extension matching the entries' stem for a lone file or a still
			// with its video (StemRole); anything else stays pending
			type candidate struct{ name, variant, still string }
			var cands []candidate
			var stemNames []string
			if takeout.IsTitleKey(r.Key) {
				for _, o := range byEntry {
					if takeout.IsStemKey(o.EntryName, r.Key) {
						stemNames = append(stemNames, o.EntryName)
					}
				}
			}
			if r.Entry != "" && byEntry[strings.ToLower(r.Entry)] != nil {
				c := candidate{r.Entry, r.Variant, r.Still}
				if takeout.IsStemKey(r.Entry, r.Key) {
					// a deferred stem match is judged again against the media sharing
					// the stem now: a second still arrived since makes it ambiguous, a
					// still arrived since makes a deferred video the motion companion
					variant, still, ok := takeout.StemRole(r.Entry, stemNames, rules)
					if !ok {
						c.name = ""
					} else {
						c.variant, c.still = variant, still
					}
				}
				if c.name != "" {
					cands = append(cands, c)
				}
			}
			if len(stemNames) > 0 {
				var stemCands []candidate
				for _, name := range stemNames {
					variant, still, ok := takeout.StemRole(name, stemNames, rules)
					if !ok {
						stemCands = nil
						break
					}
					if !strings.EqualFold(name, r.Entry) {
						stemCands = append(stemCands, candidate{name, variant, still})
					}
				}
				cands = append(cands, stemCands...)
			}
			if len(cands) > 0 {
				// the media may be the photo itself (then its own sidecar covers
				// it), take the sidecar, or be unrelated files that merely share
				// the name: then the sidecar is an album entry of the photo its
				// Google id names, like one without media at all
				consumed, undecided := false, false
				for _, c := range cands {
					name, variant, still := c.name, c.variant, c.still
					occ := byEntry[strings.ToLower(name)]
					item, err := ing.st.GetItem(ctx, occ.ItemID)
					if err != nil || item == nil {
						continue
					}
					if cur, err := ing.st.GetSidecar(ctx, item.ID); err != nil {
						return links, promoted, err
					} else if cur != nil {
						if item.GoogleID != "" && item.GoogleID == sc.GoogleID() {
							consumed = true // the photo itself, already described
							_ = ing.st.MarkPendingResolved(ctx, acct.ID, exportID, folder, r.Key)
						}
						continue
					}
					// a nameless file sharing the name of a photo known with other
					// bytes: an album entry of that photo if the export delivered the
					// photo elsewhere, its new version if the export is complete
					// without it, undecided otherwise
					m := takeout.Match{Sidecar: sc, Variant: variant, Still: still, Key: r.Key}
					if err := pr.adoptPairStill(folder, &m, occ.EntryName, stemNames); err != nil {
						return links, promoted, err
					}
					if known, collides, err := pr.nameCollision(folder, &m, occ.EntryName, pr.itemSHA(item), stemNames); err != nil {
						return links, promoted, err
					} else if collides {
						elsewhere, err := ing.st.PhotoDeliveredElsewhereInExport(ctx, acct.ID, known.GoogleID, exportID, folder, occ.EntryName)
						if err != nil {
							return links, promoted, err
						}
						if elsewhere {
							continue // the membership path below
						}
						if !complete {
							undecided = true
							continue
						}
					}
					acted, err := pr.applyLateSidecar(item, occ, m)
					if err != nil {
						ing.log.Warn("pending sidecar not applied", "folder", folder, "key", r.Key, "err", err)
						continue
					}
					if acted {
						consumed = true
						promoted++
						_ = ing.st.MarkPendingResolved(ctx, acct.ID, exportID, folder, r.Key)
					}
				}
				if consumed || undecided {
					continue
				}
			}
			// an album membership of a photo known by its Google id
			gid := sc.GoogleID()
			if gid == "" {
				continue
			}
			if kind == takeout.FolderYear {
				// Takeout can write a photo's sidecar into its year folder while
				// the file itself sits in an album folder only: the known photo
				// has its sidecar from there, this copy is consumed
				if known, err := ing.st.ItemsByGoogleIDAny(ctx, acct.ID, gid); err != nil {
					return links, promoted, err
				} else if known != nil {
					if elsewhere, err := ing.st.PhotoDeliveredElsewhereInExport(ctx, acct.ID, gid, exportID, folder, r.Key); err != nil {
						return links, promoted, err
					} else if elsewhere {
						_ = ing.st.MarkPendingResolved(ctx, acct.ID, exportID, folder, r.Key)
					}
				}
				continue
			}
			if album == nil {
				continue
			}
			variant := takeout.VariantOriginal
			if _, edited := rules.IsEdited(r.Key); edited {
				variant = takeout.VariantEdited
			}
			item, err := ing.st.FindItemByKey(ctx, acct.ID, gid+"#"+variant)
			if err != nil {
				return links, promoted, err
			}
			if item == nil {
				continue // the photo may still arrive in a later part
			}
			if r.PartNo == 0 {
				// a stored sidecar of unknown source part (predating part tracking):
				// a membership made from it could never be withdrawn with its part
				continue
			}
			// the link first, the occurrence last: until the occurrence exists the
			// row stays pending and a failure here is retried by the next run (a
			// link without occurrence is unsupported and reconciled away)
			linked, err := pr.ensureDirectConfirmed(album, item, exportID)
			if err != nil {
				return links, promoted, fmt.Errorf("album link for %s/%s: %w", folder, r.Key, err)
			}
			if err := pr.ensureGroupLinks(item); err != nil {
				return links, promoted, err
			}
			// its own occurrence, apart from any media file of that name that may
			// turn up in the folder later (another photo would replace it otherwise)
			if _, err := ing.st.UpsertOccurrence(ctx, &store.Occurrence{AccountID: acct.ID, ExportID: exportID, PartNo: r.PartNo, Folder: folder, EntryName: sidecarOnlyPrefix + r.Key,
				Variant: variant, AlbumID: album.ID, ItemID: item.ID, State: store.OccResolved}); err != nil {
				return links, promoted, err
			}
			if err := pr.retireOccurrences(album, item); err != nil {
				return links, promoted, err
			}
			_ = ing.st.MarkPendingResolved(ctx, acct.ID, exportID, folder, r.Key)
			if linked {
				links++
			}
		}
	}
	return links, promoted, nil
}

// nameCollision reports whether a file (base, with bytes sha) that a sidecar
// relates to by name alone collides with the photo the sidecar names because
// that photo is already known with other bytes: the file may then be the
// photo's new version or an unrelated file sharing the name. The variant the
// match assigns is compared first; when only another variant is known, the
// file is the photo's companion, not a collision, if that variant was
// delivered by the same folder of this export under one of the mates (names
// sharing the stem: a still with its video, an original with its edited copy).
func (pr *partRun) nameCollision(folder string, m *takeout.Match, base, sha string, mates []string) (known *store.Item, collides bool, err error) {
	gid := m.Sidecar.GoogleID()
	if gid == "" {
		return nil, false, nil
	}
	if known, err = pr.ing.st.FindItemByKey(pr.ctx, pr.acct.ID, gid+"#"+m.Variant); err != nil {
		return nil, false, err
	} else if known != nil {
		return known, pr.itemSHA(known) != sha, nil
	}
	if known, err = pr.ing.st.ItemsByGoogleIDAny(pr.ctx, pr.acct.ID, gid); err != nil || known == nil {
		return nil, false, err
	}
	names := append([]string{m.Still}, mates...)
	if takeout.SameStem(known.Filename, base) {
		// the known photo was delivered under a name sharing this file's stem
		// (20230621_145830.MP4 beside 20230621_145830.jpg): the companion of
		// the same motion photo, whichever of the two was committed first
		names = append(names, known.Filename)
	}
	mate, err := pr.deliveredAsMate(folder, known, base, names)
	return known, !mate, err
}

// deliveredAsMate reports whether an item was delivered by this folder of
// the export under one of the names (other than base).
func (pr *partRun) deliveredAsMate(folder string, item *store.Item, base string, names []string) (bool, error) {
	for _, n := range names {
		if n == "" || strings.EqualFold(n, base) {
			continue
		}
		o, err := pr.ing.st.FindOccurrence(pr.ctx, pr.acct.ID, pr.part.ExportID, folder, n)
		if err != nil {
			return false, err
		}
		if o != nil && o.ItemID == item.ID && o.State != store.OccReset && o.State != store.OccRemoved {
			return true, nil
		}
	}
	return false, nil
}

// adoptPairStill handles a still related to a title-named sidecar by its
// stem whose photo's original is a lone video of the same folder (the video
// arrived in an earlier part, before its still, and was the original for want
// of one): the video becomes the photo's motion companion so that the still
// can be its original.
func (pr *partRun) adoptPairStill(folder string, m *takeout.Match, base string, mates []string) error {
	gid := m.Sidecar.GoogleID()
	if gid == "" || m.Variant != takeout.VariantOriginal || !takeout.IsTitleKey(m.Key) || !takeout.IsStillName(base) {
		return nil
	}
	st := pr.ing.st
	orig, err := st.FindItemByKey(pr.ctx, pr.acct.ID, gid+"#"+takeout.VariantOriginal)
	if err != nil || orig == nil || !takeout.IsVideoName(orig.Filename) {
		return err
	}
	if mate, err := pr.deliveredAsMate(folder, orig, base, mates); err != nil || !mate {
		return err
	}
	motionKey := gid + "#" + takeout.VariantMotion
	if other, err := st.FindItemByKey(pr.ctx, pr.acct.ID, motionKey); err != nil || other != nil {
		return err // a motion companion exists already; the still is handled as an alt variant
	}
	pr.ing.log.Info("video that preceded its still becomes the motion companion", "video", orig.LibPath, "still", base)
	orig.Variant, orig.IdentityKey, orig.GroupID = takeout.VariantMotion, motionKey, gid+"#"+takeout.VariantOriginal
	if err := st.UpdateItem(pr.ctx, orig); err != nil {
		return err
	}
	return st.SetOccurrencesVariant(pr.ctx, orig.ID, takeout.VariantMotion)
}

// itemSHA returns the hash of an item's current bytes ("" when unknown).
func (pr *partRun) itemSHA(item *store.Item) string {
	b, err := pr.ing.st.GetBlob(pr.ctx, item.BlobID)
	if err != nil || b == nil {
		return ""
	}
	return b.SHA256
}

// applyLateSidecar gives a fallback item the identity its sidecar carries,
// stores the sidecar for an item that has none, or applies a corrected exact
// sidecar from the part that supplied the stored one (re-keying the item when
// the Google id changed). It reports whether it changed anything.
func (pr *partRun) applyLateSidecar(item *store.Item, o *store.Occurrence, m takeout.Match) (bool, error) {
	ctx, st := pr.ctx, pr.ing.st
	if gid := m.Sidecar.GoogleID(); gid != "" && (item.IdentitySrc == store.IdentityFallback || item.GoogleID != gid) {
		variant, key := m.Variant, gid+"#"+m.Variant
		if variant == takeout.VariantOriginal {
			// a second file sharing the still's Google id (e.g. a Live Photo video
			// with a sidecar of its own) is an alt variant, not a duplicate to merge
			other, err := st.FindItemByKey(ctx, pr.acct.ID, key)
			if err != nil {
				return false, err
			}
			if other != nil && other.ID != item.ID && !strings.EqualFold(other.Ext, item.Ext) {
				variant = "alt:" + strings.ToLower(strings.TrimPrefix(item.Ext, "."))
				key = gid + "#" + variant
			}
		}
		if _, err := pr.promote(item, m.Sidecar, variant, gid, key); err != nil {
			return false, fmt.Errorf("promotion: %w", err)
		}
		return true, nil
	}
	cur, err := st.GetSidecar(ctx, item.ID)
	if err != nil {
		return false, err
	}
	if cur != nil && cur.SHA256 == m.Sidecar.SHA256 {
		return false, nil
	}
	if err := pr.updateSidecar(item, m.Sidecar, deriveName(o.EntryName, m.Sidecar, item.Variant)); err != nil {
		return false, fmt.Errorf("sidecar update: %w", err)
	}
	return true, nil
}

// promote gives a fallback item its Google identity, or merges it into the
// item that already has it.
func (pr *partRun) promote(fb *store.Item, sc *takeout.Sidecar, variant, googleID, newKey string) (*store.Item, error) {
	ctx, st := pr.ctx, pr.ing.st
	if target, err := st.FindItemByKey(ctx, pr.acct.ID, newKey); err != nil {
		return nil, err
	} else if target != nil {
		return pr.mergeInto(fb, target, sc, variant)
	}
	// the arriving sidecar establishes the identity; its metadata applies only
	// when the stored sidecar is not from a newer export
	meta, keepStored := sc, false
	if cur, err := st.GetSidecar(ctx, fb.ID); err != nil {
		return nil, err
	} else if cur != nil && pr.olderThan(cur.ExportID) {
		if stored, err := takeout.ParseSidecar(cur.JSON); err == nil {
			meta, keepStored = stored, true
			pr.ing.log.Debug("promotion keeps the newer stored sidecar", "item", fb.LibPath, "stored", cur.ExportID, "arriving", pr.part.ExportID)
		}
	}
	name := deriveName(fb.Filename, meta, variant)
	taken, takenSrc := fb.TakenAt, fb.TakenSrc
	if t, ok := meta.Taken(); ok {
		taken, takenSrc = t, "sidecar"
	}
	blob, err := st.GetBlob(ctx, fb.BlobID)
	if err != nil {
		return nil, err
	}
	oldPath := fb.LibPath
	newPath, err := pr.movePath(fb, blob, pr.libraryDir(taken, takenSrc), name, newKey)
	if err != nil {
		return nil, err
	}
	var steps []Step
	var movedLinks []*store.AlbumItem
	if newPath != oldPath {
		steps = append(steps, Step{Op: "rename", Src: oldPath, Dst: newPath}, Step{Op: "unlink", Dst: sidecarPath(oldPath)})
		more, moved, err := pr.linkMoveSteps(fb, newPath)
		if err != nil {
			return nil, err
		}
		steps, movedLinks = append(steps, more...), moved
	}
	err = pr.ing.journaled(ctx, "promote", steps, func(tx *store.Store) error {
		fb.GoogleID, fb.Variant, fb.IdentityKey, fb.IdentitySrc = googleID, variant, newKey, store.IdentityGoogle
		fb.GroupID = googleID + "#" + takeout.VariantOriginal
		fb.Filename, fb.TakenAt, fb.TakenSrc, fb.LibPath = name, taken, takenSrc, newPath
		if blob != nil && blob.CanonicalPath == oldPath && newPath != oldPath {
			if err := tx.SetBlobCanonical(ctx, blob.ID, newPath); err != nil {
				return err
			}
		}
		if err := tx.UpdateItem(ctx, fb); err != nil {
			return err
		}
		for _, ai := range movedLinks {
			if err := tx.UpsertAlbumItem(ctx, ai); err != nil {
				return err
			}
		}
		if err := tx.MoveOccurrences(ctx, fb.ID, fb.ID); err != nil {
			return err
		}
		if keepStored {
			return nil
		}
		return pr.saveSidecar(tx, fb.ID, sc)
	})
	if err != nil {
		return nil, err
	}
	if err := pr.writeSidecarFile(newPath, meta); err != nil {
		return nil, err
	}
	pr.res.Promoted++
	pr.ing.log.Info("promoted fallback item", "path", newPath, "google_id", googleID)
	return fb, pr.ensureGroupLinks(fb)
}

// mergeInto folds a fallback item into the item that owns its identity. The
// fallback's bytes and the arriving sidecar are first applied to the target
// under the usual export-order rule, so that a newer export's content seen
// before its sidecar becomes current rather than a mere kept version. Each
// step is journaled on its own; a crash between them leaves the fallback item
// in place and the next run repeats the remaining steps.
func (pr *partRun) mergeInto(fb, target *store.Item, sc *takeout.Sidecar, variant string) (*store.Item, error) {
	ctx, st := pr.ctx, pr.ing.st
	fbBlob, err := st.GetBlob(ctx, fb.BlobID)
	if err != nil {
		return nil, err
	}
	tBlob, err := st.GetBlob(ctx, target.BlobID)
	if err != nil {
		return nil, err
	}
	fbExport := fb.BytesExportID
	if fbExport == "" {
		fbExport = pr.part.ExportID
	}
	fbNewer := target.BytesExportID == "" || ExportOlder(target.BytesExportID, fbExport)
	switch {
	case fbBlob == nil || tBlob == nil:
	case fbBlob.SHA256 != tBlob.SHA256 && (fbNewer || fbExport == target.BytesExportID):
		// the fallback carries newer bytes: they become current (hardlinked from
		// the fallback's file), the target's bytes become a kept version
		x := extracted{sha: fbBlob.SHA256, size: fbBlob.Size, exportID: fbExport}
		if err := pr.supersede(target, tBlob, x); err != nil {
			return nil, fmt.Errorf("supersede: %w", err)
		}
		pr.res.Changed++
	case fbBlob.SHA256 == tBlob.SHA256 && fbNewer:
		// the same bytes confirmed by a newer export: an export in between may
		// no longer replace them
		target.BytesExportID = fbExport
		if err := st.UpdateItem(ctx, target); err != nil {
			return nil, err
		}
	}
	// the fallback's own stored sidecar (possibly from a newer export than the
	// arriving one) competes for the target by its provenance, then the
	// arriving sidecar by this part's; the export-order rule decides both
	if fbSc, err := st.GetSidecar(ctx, fb.ID); err != nil {
		return nil, err
	} else if fbSc != nil {
		tSc, err := st.GetSidecar(ctx, target.ID)
		if err != nil {
			return nil, err
		}
		// a fallback sidecar still awaiting its source part's replacement never
		// displaces a target sidecar that export has confirmed (any of its parts)
		stale := fbSc.Reconfirm && tSc != nil && !tSc.Reconfirm && tSc.ExportID == fbSc.ExportID
		if stored, err := takeout.ParseSidecar(fbSc.JSON); err == nil && !stale {
			if _, err := pr.applySidecarAs(target, stored, deriveName(fb.Filename, stored, variant), fbSc.ExportID, fbSc.PartNo); err != nil {
				return nil, err
			}
			// when the awaiting sidecar is the one now on the target, the wait
			// follows it, or the replacement could never release it
			if fbSc.Reconfirm {
				if tSc, err = st.GetSidecar(ctx, target.ID); err != nil {
					return nil, err
				} else if tSc != nil && tSc.SHA256 == fbSc.SHA256 && tSc.ExportID == fbSc.ExportID && tSc.PartNo == fbSc.PartNo {
					if err := st.MarkSidecarReconfirm(ctx, target.ID); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	if sc != nil {
		if _, err := pr.applySidecar(target, sc, deriveName(fb.Filename, sc, variant)); err != nil {
			return nil, err
		}
	}
	if target, err = st.GetItem(ctx, target.ID); err != nil || target == nil {
		return nil, fmt.Errorf("target item %d vanished: %v", fb.ID, err)
	}
	if err := pr.merge(fb, target); err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	return st.GetItem(ctx, target.ID)
}

// merge folds a fallback item into the item that owns the identity.
func (pr *partRun) merge(fb, target *store.Item) error {
	ctx, st := pr.ctx, pr.ing.st
	blob, err := st.GetBlob(ctx, fb.BlobID)
	if err != nil {
		return err
	}
	ais, err := st.AlbumItemsByItem(ctx, fb.ID)
	if err != nil {
		return err
	}
	var steps []Step
	for _, ai := range ais {
		steps = append(steps, Step{Op: "unlink", Dst: ai.LinkPath})
	}
	steps = append(steps, Step{Op: "unlink", Dst: sidecarPath(fb.LibPath)})
	refs, err := st.BlobRefPaths(ctx, blob.ID, fb.LibPath)
	if err != nil {
		return err
	}
	targetBlob, err := st.GetBlob(ctx, target.BlobID)
	if err != nil {
		return err
	}
	// the fallback bytes are kept as a version of the target whenever they
	// differ from the target's current bytes (backup semantics), or when the
	// blob would otherwise lose its only file
	verPath := ""
	alreadyVersioned := false
	if (targetBlob != nil && targetBlob.SHA256 != blob.SHA256) || (blob.CanonicalPath == fb.LibPath && len(refs) == 0) {
		// deterministic name so that a retry after a crash finds the moved file
		verPath = suffixed(target.LibPath, "v"+blob.SHA256[:8])
		if prev, err := st.VersionByPath(ctx, verPath); err != nil {
			return err
		} else if prev != nil {
			if prev.BlobID != blob.ID {
				return fmt.Errorf("version path %s belongs to other content", verPath)
			}
			alreadyVersioned = true
		}
		if exists(pr.ing.abs(verPath)) && fileSHA(pr.ing.abs(verPath)) != blob.SHA256 {
			return fmt.Errorf("version path %s exists with unexpected content", verPath)
		}
		if alreadyVersioned && exists(pr.ing.abs(verPath)) {
			steps = append(steps, Step{Op: "unlink", Dst: fb.LibPath})
		} else {
			steps = append(steps, Step{Op: "rename", Src: fb.LibPath, Dst: verPath})
		}
	} else {
		steps = append(steps, Step{Op: "unlink", Dst: fb.LibPath})
	}
	err = pr.ing.journaled(ctx, "merge", steps, func(tx *store.Store) error {
		if err := tx.MoveOccurrences(ctx, fb.ID, target.ID); err != nil {
			return err
		}
		if err := tx.MoveVersions(ctx, fb.ID, target.ID); err != nil {
			return err
		}
		if err := tx.DeleteAlbumItemsOfItem(ctx, fb.ID); err != nil {
			return err
		}
		if err := tx.DeleteItem(ctx, fb.ID); err != nil {
			return err
		}
		switch {
		case verPath != "":
			if !alreadyVersioned {
				if err := tx.InsertVersion(ctx, target.ID, blob.ID, verPath, pr.part.ExportID); err != nil {
					return err
				}
			}
			if blob.CanonicalPath == fb.LibPath {
				return tx.SetBlobCanonical(ctx, blob.ID, verPath)
			}
		case blob.CanonicalPath == fb.LibPath:
			return tx.SetBlobCanonical(ctx, blob.ID, refs[0])
		}
		return nil
	})
	if err != nil {
		return err
	}
	pr.res.Merged++
	pr.ing.log.Info("merged fallback item into existing", "removed", fb.LibPath, "into", target.LibPath)
	albums, err := st.ActiveAlbumsOfItem(ctx, target.ID)
	if err != nil {
		return err
	}
	for _, aid := range albums {
		a, err := st.GetAlbum(ctx, aid)
		if err != nil || a == nil {
			continue
		}
		// confirm with the newest export that actually contains this membership,
		// not with the current one, so pruning still sees stale albums as stale
		occs, err := st.OccurrencesForMembership(ctx, a.ID, target.ID)
		if err != nil {
			return err
		}
		newest := ""
		for _, o := range occs {
			if newest == "" || ExportOlder(newest, o.ExportID) {
				newest = o.ExportID
			}
		}
		if newest == "" {
			continue
		}
		if _, err := pr.ensureDirectConfirmed(a, target, newest); err != nil {
			return err
		}
		if err := pr.retireOccurrences(a, target); err != nil {
			return err
		}
	}
	return pr.ensureGroupLinks(target)
}

// ---- archived flag ---------------------------------------------------------

func (pr *partRun) updateArchived(item *store.Item, kind takeout.FolderKind) error {
	switch kind {
	case takeout.FolderArchive:
		if item.ArchivedExportID == "" || ExportOlder(item.ArchivedExportID, pr.part.ExportID) {
			item.ArchivedExportID = pr.part.ExportID
			return pr.ing.st.UpdateItem(pr.ctx, item)
		}
	case takeout.FolderYear:
		if item.ArchivedExportID != "" && ExportOlder(item.ArchivedExportID, pr.part.ExportID) {
			item.ArchivedExportID = ""
			return pr.ing.st.UpdateItem(pr.ctx, item)
		}
	}
	return nil
}

// ---- albums ----------------------------------------------------------------

func (pr *partRun) ensureAlbum(fc *folderCtx) (*store.Album, error) {
	ctx, st := pr.ctx, pr.ing.st
	existing, err := st.AlbumsByFolder(ctx, pr.acct.ID, fc.name)
	if err != nil {
		return nil, err
	}
	folderTitle := strings.TrimSpace(fc.name)
	if fc.meta == nil {
		if len(existing) > 0 {
			return existing[0], nil
		}
		a := &store.Album{AccountID: pr.acct.ID, Title: folderTitle, TakeoutFolder: fc.name, Kind: "own"}
		p, err := pr.albumPath(folderTitle, "", fc.name)
		if err != nil {
			return nil, err
		}
		a.Path = p
		id, err := st.InsertAlbum(ctx, a, nil)
		if err != nil {
			return nil, err
		}
		a.ID = id
		return a, nil
	}
	title := fc.meta.Title
	if title == "" {
		title = folderTitle
	}
	date := fc.meta.DateKey()
	kind := "own"
	if fc.meta.Shared {
		kind = "shared"
	}
	for _, a := range existing {
		if a.Title == title && a.AlbumDate == date {
			if !a.HasMeta || a.Kind != kind {
				a.HasMeta, a.Kind = true, kind
				if err := st.UpdateAlbum(ctx, a, fc.meta.Raw); err != nil {
					return nil, err
				}
			}
			return a, nil
		}
	}
	// a provisional album (created from media before its descriptor arrived) adopts the descriptor
	for _, a := range existing {
		if !a.HasMeta {
			a.Title, a.AlbumDate, a.Kind, a.HasMeta = title, date, kind, true
			newPath, err := pr.albumPathExcept(title, date, fc.name, a.ID)
			if err != nil {
				return nil, err
			}
			if newPath != a.Path {
				oldPath := a.Path
				oldExists, newExists := exists(pr.ing.abs(oldPath)), exists(pr.ing.abs(newPath))
				var steps []Step
				switch {
				case !newExists:
					steps = []Step{{Op: "rename", Src: oldPath, Dst: newPath}}
				case newExists && !oldExists && pr.linksLiveUnder(a, newPath):
					// a crashed run already renamed the directory (its links are inside): reconcile the database only
				default:
					pr.ing.log.Warn("album folder name collision, keeping the provisional path", "album", title, "path", oldPath, "wanted", newPath)
					if err := st.UpdateAlbum(ctx, a, fc.meta.Raw); err != nil {
						return nil, err
					}
					return a, nil
				}
				a.Path = newPath
				err = pr.ing.journaled(ctx, "album-rename", steps, func(tx *store.Store) error {
					if err := tx.UpdateAlbum(ctx, a, fc.meta.Raw); err != nil {
						return err
					}
					return tx.RenameLinkPrefix(ctx, a.ID, oldPath+"/", newPath+"/")
				})
				if err != nil {
					return nil, err
				}
				return a, nil
			}
			if err := st.UpdateAlbum(ctx, a, fc.meta.Raw); err != nil {
				return nil, err
			}
			return a, nil
		}
	}
	a := &store.Album{AccountID: pr.acct.ID, Title: title, AlbumDate: date, TakeoutFolder: fc.name, Kind: kind, HasMeta: true}
	p, err := pr.albumPath(title, date, fc.name)
	if err != nil {
		return nil, err
	}
	a.Path = p
	id, err := st.InsertAlbum(ctx, a, fc.meta.Raw)
	if err != nil {
		return nil, err
	}
	a.ID = id
	return a, nil
}

// linksLiveUnder reports whether every recorded link of the album exists
// under dir (used to recognise a directory rename completed by recovery).
func (pr *partRun) linksLiveUnder(a *store.Album, dir string) bool {
	ais, err := pr.ing.st.AlbumItemsByAlbum(pr.ctx, a.ID)
	if err != nil || len(ais) == 0 {
		return false
	}
	for _, ai := range ais {
		if !exists(pr.ing.abs(path.Join(dir, path.Base(ai.LinkPath)))) {
			return false
		}
	}
	return true
}

func (pr *partRun) albumPath(title, date, folder string) (string, error) {
	return pr.albumPathExcept(title, date, folder, 0)
}

// albumPathExcept picks a free album path, allowing album exceptID to keep its own.
func (pr *partRun) albumPathExcept(title, date, folder string, exceptID int64) (string, error) {
	base := path.Join(pr.acct.Name, pr.ing.cfg.Layout.Albums)
	name := fsops.SanitizeName(title, 200)
	cands := []string{path.Join(base, name)}
	if date != "" {
		if t, err := time.Parse(time.RFC3339, date); err == nil {
			cands = append(cands, path.Join(base, fsops.SanitizeName(name+" ("+t.Format("2006-01-02")+")", 200)))
		}
	}
	cands = append(cands, path.Join(base, fsops.SanitizeName(name+"~"+hash8(folder), 200)))
	for _, p := range cands {
		taken, err := pr.ing.st.AlbumPathTakenExcept(pr.ctx, p, exceptID)
		if err != nil {
			return "", err
		}
		if !taken {
			return p, nil
		}
	}
	return "", fmt.Errorf("no free album path for %q", title)
}

func (pr *partRun) linkPathFor(album *store.Album, item *store.Item) (string, error) {
	return pr.linkPathForName(album, item, path.Base(item.LibPath), item.LibPath)
}

// linkPathForName returns the link path for an item under a given file name.
// A path already occupied is the item's own when it shares the inode (or, in
// copy mode, the bytes) of one of the item's library paths.
func (pr *partRun) linkPathForName(album *store.Album, item *store.Item, base string, libPaths ...string) (string, error) {
	cands := []string{path.Join(album.Path, base), path.Join(album.Path, suffixed(base, hash8(item.IdentityKey)))}
	own := func(p string) bool {
		for _, lp := range libPaths {
			if !exists(pr.ing.abs(lp)) {
				continue
			}
			if fsops.SameInode(pr.ing.abs(p), pr.ing.abs(lp)) {
				return true
			}
			// in copy mode a crashed run may have left the finished copy behind: reuse it
			if pr.ing.cfg.Layout.LinkMode == "copy" {
				if same, err := fsops.SameContent(pr.ing.abs(p), pr.ing.abs(lp)); err == nil && same {
					return true
				}
			}
		}
		return false
	}
	for _, p := range cands {
		taken, err := pr.ing.st.LinkPathTaken(pr.ctx, p, album.ID, item.ID)
		if err != nil {
			return "", err
		}
		if taken {
			continue
		}
		if exists(pr.ing.abs(p)) && !own(p) {
			continue
		}
		return p, nil
	}
	return "", fmt.Errorf("no free link path for %s in %s", base, album.Path)
}

// ensureDirect makes item a direct member of album and propagates group links.
func (pr *partRun) ensureDirect(album *store.Album, item *store.Item) (bool, error) {
	return pr.ensureDirectConfirmed(album, item, pr.part.ExportID)
}

// ensureDirectConfirmed is ensureDirect with an explicit confirming export id.
func (pr *partRun) ensureDirectConfirmed(album *store.Album, item *store.Item, exportID string) (linked bool, err error) {
	ctx, st := pr.ctx, pr.ing.st
	ai, err := st.GetAlbumItem(ctx, album.ID, item.ID)
	if err != nil {
		return false, err
	}
	if ai == nil || !ai.Direct {
		// a complete listing newer than this export was applied (to the album, or
		// to the account before this album existed) and does not contain the
		// item (no row, or only a link derived from its still): this occurrence
		// is history, not a direct membership
		if applied, err := pr.appliedListing(album); err != nil {
			return false, err
		} else if applied != "" && ExportOlder(exportID, applied) {
			pr.ing.log.Debug("membership from an older export superseded by a newer listing", "album", album.Path, "item", item.LibPath, "export", exportID, "listing", applied)
			return false, nil
		}
	}
	linkPath := ""
	if ai != nil {
		linkPath = ai.LinkPath
	} else if linkPath, err = pr.linkPathFor(album, item); err != nil {
		return false, err
	}
	row := &store.AlbumItem{AlbumID: album.ID, ItemID: item.ID, LinkPath: linkPath, Direct: true, ConfirmedExportID: exportID, ConfirmedBy: pr.part.Name}
	if ai != nil {
		row.ViaGroup, row.GroupStill = ai.ViaGroup, ai.GroupStill
		if ai.ConfirmedExportID != "" && !ExportOlder(ai.ConfirmedExportID, exportID) {
			row.ConfirmedExportID, row.ConfirmedBy = ai.ConfirmedExportID, ai.ConfirmedBy
		}
	}
	var steps []Step
	if !fsops.SameInode(pr.ing.abs(linkPath), pr.ing.abs(item.LibPath)) {
		steps = []Step{pr.linkStep(item.LibPath, linkPath)}
	}
	if err := pr.ing.journaled(ctx, "album-link", steps, func(tx *store.Store) error { return tx.UpsertAlbumItem(ctx, row) }); err != nil {
		return false, err
	}
	if ai == nil || !ai.Direct {
		pr.res.Links++
	}
	if item.IdentityKey != item.GroupID {
		return true, nil // only the original still carries its companions into an album
	}
	members, err := st.ItemsByGroup(ctx, pr.acct.ID, item.GroupID)
	if err != nil {
		return true, err
	}
	for _, m := range members {
		if m.ID == item.ID {
			continue
		}
		if err := pr.ensureViaGroup(album, m, item); err != nil {
			return true, err
		}
	}
	return true, nil
}

// ensureViaGroup links a companion into an album its still is a direct member of.
func (pr *partRun) ensureViaGroup(album *store.Album, m, still *store.Item) error {
	ctx, st := pr.ctx, pr.ing.st
	ai, err := st.GetAlbumItem(ctx, album.ID, m.ID)
	if err != nil {
		return err
	}
	if ai != nil && ai.ViaGroup && ai.GroupStill == still.ID && fsops.SameInode(pr.ing.abs(ai.LinkPath), pr.ing.abs(m.LibPath)) {
		return nil
	}
	linkPath := ""
	if ai != nil {
		linkPath = ai.LinkPath
	} else {
		if linkPath, err = pr.linkPathFor(album, m); err != nil {
			return err
		}
	}
	row := &store.AlbumItem{AlbumID: album.ID, ItemID: m.ID, LinkPath: linkPath, ViaGroup: true, GroupStill: still.ID}
	if ai != nil {
		row.Direct, row.ConfirmedExportID, row.ConfirmedBy = ai.Direct, ai.ConfirmedExportID, ai.ConfirmedBy
	}
	var steps []Step
	if !fsops.SameInode(pr.ing.abs(linkPath), pr.ing.abs(m.LibPath)) {
		steps = []Step{pr.linkStep(m.LibPath, linkPath)}
	}
	if err := pr.ing.journaled(ctx, "group-link", steps, func(tx *store.Store) error { return tx.UpsertAlbumItem(ctx, row) }); err != nil {
		return err
	}
	pr.res.Links++
	return nil
}

// ensureGroupLinks keeps group links consistent for one item: a still carries
// its companions into every album it is a direct member of; a companion is
// linked into every album its still is a direct member of.
func (pr *partRun) ensureGroupLinks(item *store.Item) error {
	ctx, st := pr.ctx, pr.ing.st
	still := item
	if item.IdentityKey != item.GroupID {
		var err error
		still, err = st.FindItemByKey(ctx, pr.acct.ID, item.GroupID)
		if err != nil || still == nil || still.ID == item.ID {
			return err
		}
	}
	ais, err := st.AlbumItemsByItem(ctx, still.ID)
	if err != nil {
		return err
	}
	var companions []*store.Item
	if still.ID == item.ID {
		members, err := st.ItemsByGroup(ctx, pr.acct.ID, still.GroupID)
		if err != nil {
			return err
		}
		for _, m := range members {
			if m.ID != still.ID {
				companions = append(companions, m)
			}
		}
	} else {
		companions = []*store.Item{item}
	}
	if len(companions) == 0 {
		return nil
	}
	for _, ai := range ais {
		if !ai.Direct {
			continue
		}
		album, err := st.GetAlbum(ctx, ai.AlbumID)
		if err != nil || album == nil {
			continue
		}
		for _, c := range companions {
			if err := pr.ensureViaGroup(album, c, still); err != nil {
				return err
			}
		}
	}
	return nil
}

// ErrNotPart is returned when a file name is not a Takeout part.
var ErrNotPart = errors.New("not a takeout part name")
