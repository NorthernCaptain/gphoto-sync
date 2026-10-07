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
}

// Result summarises one part.
type Result struct {
	Part            string
	Status          string
	Entries         int
	Media           int
	Added           int
	Duplicates      int
	Changed         int
	Promoted        int
	Merged          int
	Links           int
	Failed          int
	GivenUp         int
	Skipped         int
	Unresolved      int
	PendingSidecars int
	Albums          int
	Duration        time.Duration
	Errors          []string
}

// Ingester holds the dependencies of ingest runs.
type Ingester struct {
	cfg   *config.Config
	st    *store.Store
	log   *slog.Logger
	loc   *time.Location
	rules takeout.Rules

	journalCount atomic.Int64 // journal entries begun; used to decide whether a temp file may be removed
}

// New creates an Ingester.
func New(cfg *config.Config, st *store.Store, log *slog.Logger) *Ingester {
	rules := takeout.Rules{ProductFolders: cfg.Takeout.ProductFolders, YearPrefixes: cfg.Takeout.YearFolderPrefixes, ArchiveFolders: cfg.Takeout.ArchiveFolders, TrashFolders: cfg.Takeout.TrashFolders}
	if len(rules.YearPrefixes) == 0 {
		rules = takeout.DefaultRules
	}
	return &Ingester{cfg: cfg, st: st, log: log, loc: cfg.Location(), rules: rules}
}

type folderCtx struct {
	name     string
	kind     takeout.FolderKind
	year     int
	index    *takeout.Index
	meta     *takeout.AlbumMeta
	album    *store.Album
	hasMedia bool
}

type job struct {
	e       SourceEntry
	ce      takeout.Entry
	fc      *folderCtx
	match   *takeout.Match
	pending string // media key of a pending sidecar consumed from an earlier part
}

type extracted struct {
	job
	tmpRel   string
	sha      string
	size     int64
	exifTime time.Time
	exifSrc  string
	err      error
}

type partRun struct {
	ing     *Ingester
	ctx     context.Context
	src     Source
	part    PartMeta
	opt     Options
	acct    *store.Account
	arch    *store.Archive
	folders map[string]*folderCtx
	res     Result
	tmpDir  string
}

// IngestPart processes one part. It is safe to call again for the same part:
// finished entries are skipped and every file operation is idempotent.
func (ing *Ingester) IngestPart(ctx context.Context, src Source, part PartMeta, opt Options) (*Result, error) {
	start := time.Now()
	pr := &partRun{ing: ing, ctx: ctx, src: src, part: part, opt: opt, folders: map[string]*folderCtx{}}
	pr.res.Part = part.Name
	pr.tmpDir = path.Join(part.Account, ".tmp")

	acct, err := ing.st.EnsureAccount(ctx, part.Account, part.RcloneRemote)
	if err != nil {
		return nil, err
	}
	pr.acct = acct

	// pass A: classify, read descriptors and sidecars
	var mediaEntries []job
	var badEntries []failedEntry // descriptor/sidecar entries that could not be read or parsed
	var goodDescriptors []string // descriptor/sidecar entries parsed fine (to clear earlier failures)
	for _, e := range src.Entries() {
		ce, ok := takeout.Classify(e.Name, e.Size, ing.rules)
		if !ok {
			continue
		}
		pr.res.Entries++
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
			if _, edited := takeout.IsEdited(ce.Base); edited && ing.cfg.Layout.EditedVersions == "prefer_original" {
				pr.res.Skipped++
				continue
			}
			fc := pr.folder(ce)
			fc.hasMedia = true
			mediaEntries = append(mediaEntries, job{e: e, ce: ce, fc: fc})
			pr.res.Media++
		}
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
	if prevFailed, err := ing.st.EntryNamesByStatus(ctx, arch.ID, store.EntryFailed, store.EntryGivenUp); err == nil && len(prevFailed) > 0 {
		for _, name := range goodDescriptors {
			if prevFailed[name] {
				_ = ing.st.MarkEntry(ctx, arch.ID, name, store.EntryDone, 0, "")
			}
		}
	}
	if n, err := ing.st.CountExports(ctx, acct.ID); err == nil && n == 1 {
		// an account's first export is the full one and therefore authoritative
		_ = ing.st.SetExportAuthoritative(ctx, acct.ID, part.ExportID)
	}

	// albums and late sidecars (promotion of fallback items from earlier parts)
	for _, fc := range pr.folders {
		if fc.kind == takeout.FolderAlbum && (fc.meta != nil || fc.hasMedia) {
			a, err := pr.ensureAlbum(fc)
			if err != nil {
				return nil, fmt.Errorf("album %q: %w", fc.name, err)
			}
			fc.album = a
		}
		if err := pr.promoteFromEarlierParts(fc); err != nil {
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
			// media already ingested from another part of this export: nothing to wait for
			if occ, err := ing.st.FindOccurrence(ctx, acct.ID, part.ExportID, fc.name, u.Key); err == nil && occ != nil && occ.ItemID != 0 {
				continue
			}
			if err := ing.st.PutPendingSidecar(ctx, acct.ID, part.ExportID, fc.name, u.Key, u.SC.Raw); err != nil {
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
	if err := ing.st.FinishArchive(ctx, arch.ID, status, pr.res.Media, counts[store.EntryDone], pr.res.Duplicates, pr.res.Failed+pr.res.GivenUp); err != nil {
		return nil, err
	}
	_ = os.Remove(ing.abs(pr.tmpDir))
	pr.res.Duration = time.Since(start)
	return &pr.res, nil
}

func (pr *partRun) folder(ce takeout.Entry) *folderCtx {
	fc, ok := pr.folders[ce.Folder]
	if !ok {
		fc = &folderCtx{name: ce.Folder, kind: ce.FolderKind, year: ce.Year, index: takeout.NewIndex()}
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
	for _, k := range takeout.LookupKeys(j.ce.Base) {
		raw, err := pr.ing.st.GetPendingSidecar(pr.ctx, pr.acct.ID, pr.part.ExportID, j.fc.name, k.Key)
		if err != nil || raw == nil {
			continue
		}
		sc, err := takeout.ParseSidecar(raw)
		if err != nil {
			continue
		}
		return &takeout.Match{Sidecar: sc, Variant: k.Variant, Still: k.Still, Key: k.Key}
	}
	// a sidecar stored under a truncated media name by an earlier part: the
	// prefix narrows the candidates, the sidecar's own title (the untruncated
	// original name) must equal the media name, and that must be unique
	if cands, err := pr.ing.st.PendingSidecarsByPrefix(pr.ctx, pr.acct.ID, pr.part.ExportID, j.fc.name, j.ce.Base); err == nil && len(cands) > 0 {
		var hit *takeout.Match
		n := 0
		for key, raw := range cands {
			sc, err := takeout.ParseSidecar(raw)
			if err != nil || !strings.EqualFold(sc.Title, j.ce.Base) {
				continue
			}
			n++
			variant, still := takeout.VariantOriginal, j.ce.Base
			if orig, edited := takeout.IsEdited(j.ce.Base); edited {
				variant, still = takeout.VariantEdited, orig
			}
			hit = &takeout.Match{Sidecar: sc, Variant: variant, Still: still, Key: key}
		}
		if n == 1 {
			return hit
		}
	}
	return nil
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

// libraryPath returns a free library path for a new item, applying the
// identity suffix on collisions with other items. When sha and size are given,
// a path occupied on disk by an unrecorded file is used only if that file has
// exactly this content (adopted after a crash); otherwise it is skipped.
func (pr *partRun) libraryPath(dir, name, key string, exceptItem int64, sha string, size int64) (p string, adopted bool, err error) {
	cands := []string{path.Join(dir, name), path.Join(dir, suffixed(name, hash8(key))), path.Join(dir, suffixed(name, hash8(key)+hash8(key+"#2")))}
	for _, p := range cands {
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

// symlinkMoveSteps re-points an item's album symlinks when its library path
// moves; hardlinks follow the inode and copies are independent, so only
// symlink mode needs this.
func (pr *partRun) symlinkMoveSteps(item *store.Item, newPath string) ([]Step, error) {
	if pr.ing.cfg.Layout.LinkMode != "symlink" {
		return nil, nil
	}
	ais, err := pr.ing.st.AlbumItemsByItem(pr.ctx, item.ID)
	if err != nil {
		return nil, err
	}
	var steps []Step
	for _, ai := range ais {
		steps = append(steps, Step{Op: "unlink", Dst: ai.LinkPath}, Step{Op: "symlink", Src: newPath, Dst: ai.LinkPath})
	}
	return steps, nil
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

func (pr *partRun) saveSidecar(tx *store.Store, itemID int64, sc *takeout.Sidecar) error {
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
	return tx.UpsertSidecar(pr.ctx, itemID, sc.Raw, sc.SHA256, sc.Description, sc.Favorited, lat, lon, alt, people)
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
		if err := pr.ensureDirect(fc.album, item); err != nil {
			return fmt.Errorf("album link: %w", err)
		}
	}
	return pr.ensureGroupLinks(item)
}

func (pr *partRun) create(x extracted, sc *takeout.Sidecar, variant, name, key, src, googleID string) (*store.Item, error) {
	ctx, st := pr.ctx, pr.ing.st
	taken, takenSrc := pr.resolveTime(sc, x.exifTime, x.exifSrc, x.fc.year)
	dir := pr.libraryDir(taken, takenSrc)
	libPath, adopted, err := pr.libraryPath(dir, name, key, 0, x.sha, x.size)
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
		Ext: strings.ToLower(filepath.Ext(name)), TakenAt: taken, TakenSrc: takenSrc, LibPath: libPath, GroupID: groupID}
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
// membership), changed sidecar, or changed bytes (supersede).
func (pr *partRun) existing(item *store.Item, x extracted, sc *takeout.Sidecar, variant, name string) error {
	ctx, st := pr.ctx, pr.ing.st
	blob, err := st.GetBlob(ctx, item.BlobID)
	if err != nil {
		return err
	}
	changed := false
	if blob != nil && blob.SHA256 != x.sha {
		if err := pr.supersede(item, blob, x); err != nil {
			return fmt.Errorf("supersede: %w", err)
		}
		changed = true
	}
	if sc != nil {
		cur, err := st.GetSidecar(ctx, item.ID)
		if err != nil {
			return err
		}
		if cur == nil || cur.SHA256 != sc.SHA256 {
			if err := pr.updateSidecar(item, sc, name); err != nil {
				return fmt.Errorf("sidecar update: %w", err)
			}
			changed = true
		} else if pr.ing.cfg.Layout.KeepSidecarJSON && !exists(pr.ing.abs(sidecarPath(item.LibPath))) {
			// the file beside the item went missing (or a crashed run never wrote it)
			if err := pr.writeSidecarFile(item.LibPath, sc); err != nil {
				return err
			}
		}
	}
	if changed {
		pr.res.Changed++
	} else {
		pr.res.Duplicates++
	}
	return st.TouchItem(ctx, item.ID)
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
		steps = append(steps, Step{Op: "unlink", Dst: x.tmpRel})
	case newBlob != nil && exists(pr.ing.abs(newBlob.CanonicalPath)):
		steps = append(steps, Step{Op: "link", Src: newBlob.CanonicalPath, Dst: item.LibPath}, Step{Op: "unlink", Dst: x.tmpRel})
	default:
		steps = append(steps, Step{Op: "rename", Src: x.tmpRel, Dst: item.LibPath})
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
		item.BlobID = newBlob.ID
		return tx.UpdateItem(ctx, item)
	})
}

// updateSidecar stores a changed sidecar and moves the file if its name or
// capture month changed.
func (pr *partRun) updateSidecar(item *store.Item, sc *takeout.Sidecar, name string) error {
	ctx, st := pr.ctx, pr.ing.st
	taken, takenSrc := item.TakenAt, item.TakenSrc
	if t, ok := sc.Taken(); ok {
		taken, takenSrc = t, "sidecar"
	}
	newPath := item.LibPath
	if name != item.Filename || !taken.Equal(item.TakenAt) {
		dir := pr.libraryDir(taken, takenSrc)
		p, _, err := pr.libraryPath(dir, name, item.IdentityKey, item.ID, "", 0)
		if err != nil {
			return err
		}
		newPath = p
	}
	var steps []Step
	oldPath := item.LibPath
	if newPath != oldPath {
		steps = append(steps, Step{Op: "rename", Src: oldPath, Dst: newPath}, Step{Op: "unlink", Dst: sidecarPath(oldPath)})
		more, err := pr.symlinkMoveSteps(item, newPath)
		if err != nil {
			return err
		}
		steps = append(steps, more...)
	}
	blob, err := st.GetBlob(ctx, item.BlobID)
	if err != nil {
		return err
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
		return pr.saveSidecar(tx, item.ID, sc)
	})
	if err != nil {
		return err
	}
	return pr.writeSidecarFile(newPath, sc)
}

// ---- promotion and merge ---------------------------------------------------

// promoteFromEarlierParts promotes fallback items whose media was ingested
// from an earlier part of this export and whose sidecar arrived now.
func (pr *partRun) promoteFromEarlierParts(fc *folderCtx) error {
	ctx, st := pr.ctx, pr.ing.st
	occs, err := st.OccurrencesInFolder(ctx, pr.acct.ID, pr.part.ExportID, fc.name)
	if err != nil {
		return err
	}
	for _, o := range occs {
		if o.ItemID == 0 || o.PartNo == pr.part.PartNo {
			continue
		}
		// the lookup also marks the sidecar used, so a re-run never stores it as pending
		m, ok := fc.index.Lookup(o.EntryName)
		if !ok {
			continue
		}
		item, err := st.GetItem(ctx, o.ItemID)
		if err != nil || item == nil || item.IdentitySrc != store.IdentityFallback {
			continue
		}
		gid := m.Sidecar.GoogleID()
		if gid == "" {
			continue
		}
		entry := "promotion:" + fc.name + "/" + o.EntryName
		if _, err := pr.promote(item, m.Sidecar, m.Variant, gid, gid+"#"+m.Variant); err != nil {
			pr.failNamed(entry, fmt.Errorf("late promotion: %w", err))
			continue
		}
		_ = st.MarkEntry(ctx, pr.arch.ID, entry, store.EntryDone, 0, "")
	}
	return nil
}

// promote gives a fallback item its Google identity, or merges it into the
// item that already has it.
func (pr *partRun) promote(fb *store.Item, sc *takeout.Sidecar, variant, googleID, newKey string) (*store.Item, error) {
	ctx, st := pr.ctx, pr.ing.st
	if target, err := st.FindItemByKey(ctx, pr.acct.ID, newKey); err != nil {
		return nil, err
	} else if target != nil {
		if err := pr.merge(fb, target); err != nil {
			return nil, fmt.Errorf("merge: %w", err)
		}
		return st.GetItem(ctx, target.ID)
	}
	name := deriveName(fb.Filename, sc, variant)
	taken, takenSrc := fb.TakenAt, fb.TakenSrc
	if t, ok := sc.Taken(); ok {
		taken, takenSrc = t, "sidecar"
	}
	oldPath := fb.LibPath
	newPath, _, err := pr.libraryPath(pr.libraryDir(taken, takenSrc), name, newKey, fb.ID, "", 0)
	if err != nil {
		return nil, err
	}
	var steps []Step
	if newPath != oldPath {
		steps = append(steps, Step{Op: "rename", Src: oldPath, Dst: newPath}, Step{Op: "unlink", Dst: sidecarPath(oldPath)})
		more, err := pr.symlinkMoveSteps(fb, newPath)
		if err != nil {
			return nil, err
		}
		steps = append(steps, more...)
	}
	blob, err := st.GetBlob(ctx, fb.BlobID)
	if err != nil {
		return nil, err
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
		if err := tx.MoveOccurrences(ctx, fb.ID, fb.ID); err != nil {
			return err
		}
		return pr.saveSidecar(tx, fb.ID, sc)
	})
	if err != nil {
		return nil, err
	}
	if err := pr.writeSidecarFile(newPath, sc); err != nil {
		return nil, err
	}
	pr.res.Promoted++
	pr.ing.log.Info("promoted fallback item", "path", newPath, "google_id", googleID)
	return fb, pr.ensureGroupLinks(fb)
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
		if err := pr.ensureDirectConfirmed(a, target, newest); err != nil {
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
	base := path.Base(item.LibPath)
	cands := []string{path.Join(album.Path, base), path.Join(album.Path, suffixed(base, hash8(item.IdentityKey)))}
	for _, p := range cands {
		taken, err := pr.ing.st.LinkPathTaken(pr.ctx, p, album.ID, item.ID)
		if err != nil {
			return "", err
		}
		if taken {
			continue
		}
		if exists(pr.ing.abs(p)) && !fsops.SameInode(pr.ing.abs(p), pr.ing.abs(item.LibPath)) {
			// in copy mode a crashed run may have left the finished copy behind: reuse it
			if pr.ing.cfg.Layout.LinkMode == "copy" {
				if same, err := fsops.SameContent(pr.ing.abs(p), pr.ing.abs(item.LibPath)); err == nil && same {
					return p, nil
				}
			}
			continue
		}
		return p, nil
	}
	return "", fmt.Errorf("no free link path for %s in %s", base, album.Path)
}

// ensureDirect makes item a direct member of album and propagates group links.
func (pr *partRun) ensureDirect(album *store.Album, item *store.Item) error {
	return pr.ensureDirectConfirmed(album, item, pr.part.ExportID)
}

// ensureDirectConfirmed is ensureDirect with an explicit confirming export id.
func (pr *partRun) ensureDirectConfirmed(album *store.Album, item *store.Item, exportID string) error {
	ctx, st := pr.ctx, pr.ing.st
	ai, err := st.GetAlbumItem(ctx, album.ID, item.ID)
	if err != nil {
		return err
	}
	linkPath := ""
	if ai != nil {
		linkPath = ai.LinkPath
	} else {
		if linkPath, err = pr.linkPathFor(album, item); err != nil {
			return err
		}
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
		return err
	}
	if ai == nil || !ai.Direct {
		pr.res.Links++
	}
	if item.IdentityKey != item.GroupID {
		return nil // only the original still carries its companions into an album
	}
	members, err := st.ItemsByGroup(ctx, pr.acct.ID, item.GroupID)
	if err != nil {
		return err
	}
	for _, m := range members {
		if m.ID == item.ID {
			continue
		}
		if err := pr.ensureViaGroup(album, m, item); err != nil {
			return err
		}
	}
	return nil
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
