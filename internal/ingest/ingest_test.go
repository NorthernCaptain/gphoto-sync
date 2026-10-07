package ingest

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/db"
	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/store"
)

type fixture struct {
	t    *testing.T
	root string
	cfg  *config.Config
	st   *store.Store
	ing  *Ingester
	ctx  context.Context
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = filepath.Join(root, "data")
	cfg.StateDir = filepath.Join(root, "state")
	cfg.Workers = 2
	cfg.Timezone = "UTC"
	cfg.Accounts = map[string]config.Account{"leo": {}, "jane": {}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(cfg.StateDir, 0o755)
	os.MkdirAll(cfg.DataDir, 0o755)
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(cfg.StateDir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	st := store.New(d)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &fixture{t: t, root: root, cfg: cfg, st: st, ing: New(cfg, st, log), ctx: ctx}
}

type file struct {
	name string
	data []byte
}

func (f *fixture) zip(name string, files []file) string {
	p := filepath.Join(f.root, name)
	out, err := os.Create(p)
	if err != nil {
		f.t.Fatal(err)
	}
	w := zip.NewWriter(out)
	for _, fl := range files {
		fw, err := w.Create(fl.name)
		if err != nil {
			f.t.Fatal(err)
		}
		fw.Write(fl.data)
	}
	w.Close()
	out.Close()
	return p
}

func (f *fixture) ingest(account, zipPath string, opt Options) *Result {
	f.t.Helper()
	src, err := OpenZip(zipPath)
	if err != nil {
		f.t.Fatal(err)
	}
	defer src.Close()
	eid, pn, ok := ParsePartName(filepath.Base(zipPath))
	if !ok {
		f.t.Fatalf("bad part name %s", zipPath)
	}
	res, err := f.ing.IngestPart(f.ctx, src, PartMeta{Account: account, ExportID: eid, PartNo: pn, Name: filepath.Base(zipPath)}, opt)
	if err != nil {
		f.t.Fatalf("ingest %s: %v", zipPath, err)
	}
	if len(res.Errors) > 0 {
		f.t.Fatalf("ingest %s errors: %v", zipPath, res.Errors)
	}
	return res
}

func (f *fixture) exists(rel string) bool {
	_, err := os.Stat(filepath.Join(f.cfg.DataDir, rel))
	return err == nil
}

func (f *fixture) mustExist(rels ...string) {
	f.t.Helper()
	for _, r := range rels {
		if !f.exists(r) {
			f.t.Errorf("expected %s to exist", r)
		}
	}
}

func (f *fixture) sameInode(a, b string) bool {
	return fsops.SameInode(filepath.Join(f.cfg.DataDir, a), filepath.Join(f.cfg.DataDir, b))
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func sidecar(title, id string, taken int64, extra string) []byte {
	return []byte(fmt.Sprintf(`{"title":%q,"description":"","photoTakenTime":{"timestamp":"%d","formatted":"x"},"geoData":{"latitude":0.0,"longitude":0.0,"altitude":0.0},"url":"https://photos.google.com/photo/%s"%s}`, title, taken, id, extra))
}

const (
	gp    = "Takeout/Google Photos/"
	tJuly = int64(1657473817) // 2022-07-10T17:23:37Z
	tMar  = int64(1648000000) // 2022-03-23
)

func TestIngestEndToEnd(t *testing.T) {
	f := newFixture(t)
	a := randBytes(3000)
	a1 := randBytes(3100) // distinct photo with the same name, Google's (1)
	aEdited := randBytes(2000)
	mpStill := randBytes(2500)
	mpMotion := randBytes(4000)
	b := randBytes(1500) // sidecar arrives in part 2

	part1 := f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/IMG_1.jpg", a},
		{gp + "Photos from 2022/IMG_1.jpg.supplemental-metadata.json", sidecar("IMG_1.jpg", "AAA", tJuly, "")},
		{gp + "Photos from 2022/IMG_1(1).jpg", a1},
		{gp + "Photos from 2022/IMG_1.jpg.supplemental-metadata(1).json", sidecar("IMG_1.jpg", "BBB", tMar, "")},
		{gp + "Photos from 2022/IMG_1-edited.jpg", aEdited},
		{gp + "Photos from 2022/PXL_9.MP.jpg", mpStill},
		{gp + "Photos from 2022/PXL_9.MP", mpMotion},
		{gp + "Photos from 2022/PXL_9.MP.jpg.supplemental-metadata.json", sidecar("PXL_9.MP.jpg", "CCC", tJuly, "")},
		{gp + "Photos from 2021/B.jpg", b}, // no sidecar yet
		// album "Trip " with a copy of IMG_1 and the motion still; descriptor comes in part 2
		{gp + "Trip /IMG_1.jpg", a},
		{gp + "Trip /IMG_1.jpg.supplemental-metadata.json", sidecar("IMG_1.jpg", "AAA", tJuly, "")},
		{gp + "Trip /PXL_9.MP.jpg", mpStill},
		{gp + "Trip /PXL_9.MP.jpg.supplemental-metadata.json", sidecar("PXL_9.MP.jpg", "CCC", tJuly, "")},
		{gp + "Trip /PXL_9.MP", mpMotion},
		// a sidecar whose media never shows up in this part
		{gp + "Photos from 2020/Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
		{gp + "Trash/old.jpg", randBytes(10)},
		{"Takeout/archive_browser.html", []byte("<html>")},
	})
	part2 := f.zip("takeout-20261006T035113Z-1-002.zip", []file{
		{gp + "Trip /metadata.json", []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Photos from 2021/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "DDD", tMar, "")},
		{gp + "Photos from 2020/Z.jpg", randBytes(700)},
	})

	r1 := f.ingest("leo", part1, Options{})
	if r1.Status != store.ArchiveIngested {
		t.Fatalf("part1 status %s", r1.Status)
	}
	// 6 distinct items: IMG_1, IMG_1(1), IMG_1-edited, PXL still, PXL motion, B
	if r1.Added != 6 {
		t.Errorf("added %d want 6 (%+v)", r1.Added, r1)
	}
	f.mustExist(
		"leo/library/2022/07/IMG_1.jpg", "leo/library/2022/07/IMG_1.jpg.json",
		"leo/library/2022/03/IMG_1.jpg", // BBB, different month, same name
		"leo/library/2022/07/IMG_1-edited.jpg",
		"leo/library/2022/07/PXL_9.MP.jpg", "leo/library/2022/07/PXL_9.MP",
		"leo/library/2021/00/B.jpg", // folder year only
		"leo/albums/Trip/IMG_1.jpg", "leo/albums/Trip/PXL_9.MP.jpg", "leo/albums/Trip/PXL_9.MP",
	)
	if f.exists("leo/library/0000/00/old.jpg") || f.exists("leo/albums/Trash") {
		t.Error("trash was ingested")
	}
	if !f.sameInode("leo/library/2022/07/IMG_1.jpg", "leo/albums/Trip/IMG_1.jpg") {
		t.Error("album link is not a hardlink")
	}
	if r1.PendingSidecars != 1 {
		t.Errorf("pending sidecars %d want 1", r1.PendingSidecars)
	}

	// part 2: album descriptor renames the provisional album, B gets promoted, Z arrives for its waiting sidecar
	r2 := f.ingest("leo", part2, Options{})
	if r2.Promoted != 1 || r2.Added != 1 {
		t.Errorf("part2: promoted %d added %d (%+v)", r2.Promoted, r2.Added, r2)
	}
	f.mustExist("leo/albums/Trip to Austin/IMG_1.jpg", "leo/library/2022/03/B.jpg", "leo/library/2022/03/B.jpg.json", "leo/library/2022/07/Z.jpg")
	if f.exists("leo/albums/Trip") || f.exists("leo/library/2021/00/B.jpg") {
		t.Error("old paths still present after rename/promotion")
	}
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "DDD#original")
	if it == nil || it.IdentitySrc != store.IdentityGoogle {
		t.Fatalf("B not promoted: %+v", it)
	}
	motion, _ := f.st.FindItemByKey(f.ctx, acct.ID, "CCC#motion")
	still, _ := f.st.FindItemByKey(f.ctx, acct.ID, "CCC#original")
	if motion == nil || still == nil || motion.GroupID != still.GroupID {
		t.Fatalf("motion pair not grouped: %+v %+v", motion, still)
	}

	// idempotency: nothing changes on a re-run
	r1b := f.ingest("leo", part1, Options{})
	r2b := f.ingest("leo", part2, Options{})
	if r1b.Added+r2b.Added+r1b.Promoted+r2b.Promoted+r1b.Changed+r2b.Changed != 0 {
		t.Errorf("re-run changed things: %+v %+v", r1b, r2b)
	}
	c, _ := f.st.Counts(f.ctx, acct.ID)
	if c["items"] != 7 || c["albums"] != 1 {
		t.Errorf("counts %+v", c)
	}
	// the first export is authoritative; with the part count confirmed, pruning applies the listing
	if err := f.st.SetExportConfirmedParts(f.ctx, acct.ID, "20261006T035113Z-1", 2, false); err != nil {
		t.Fatal(err)
	}
	pres, err := f.ing.Prune(f.ctx, "leo", "20261006T035113Z-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if pres.Skipped != "" || pres.Removed != 0 {
		t.Errorf("prune of the first export: %+v", pres)
	}

	// a later export: IMG_1 gets a description (sidecar change) and new bytes (supersede)
	part3 := f.zip("takeout-20261106T000000Z-2-001.zip", []file{
		{gp + "Photos from 2022/IMG_1.jpg", randBytes(3333)},
		{gp + "Photos from 2022/IMG_1.jpg.supplemental-metadata.json", sidecar("IMG_1.jpg", "AAA", tJuly, `,"description":"re-edited"`)},
	})
	r3 := f.ingest("leo", part3, Options{})
	if r3.Changed != 1 || r3.Added != 0 {
		t.Errorf("part3: %+v", r3)
	}
	entries, _ := filepath.Glob(filepath.Join(f.cfg.DataDir, "leo/library/2022/07/IMG_1~v*.jpg"))
	if len(entries) != 1 {
		t.Errorf("superseded version not kept: %v", entries)
	}
	if !f.sameInode("leo/library/2022/07/IMG_1.jpg", "leo/albums/Trip to Austin/IMG_1.jpg") {
		t.Error("album link not re-pointed after supersede")
	}

	// a later complete export where IMG_1 left the album: the stale link goes, the motion pair stays
	part4 := f.zip("takeout-20261206T000000Z-3-001.zip", []file{
		{gp + "Trip /metadata.json", []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip /PXL_9.MP.jpg", mpStill},
		{gp + "Trip /PXL_9.MP.jpg.supplemental-metadata.json", sidecar("PXL_9.MP.jpg", "CCC", tJuly, "")},
		{gp + "Trip /PXL_9.MP", mpMotion},
	})
	f.ingest("leo", part4, Options{FullAlbums: true, Parts: 1})
	pres, err = f.ing.Prune(f.ctx, "leo", "20261206T000000Z-3", false)
	if err != nil {
		t.Fatal(err)
	}
	if pres.Removed != 1 || f.exists("leo/albums/Trip to Austin/IMG_1.jpg") {
		t.Errorf("prune did not remove the stale link: %+v", pres)
	}
	f.mustExist("leo/albums/Trip to Austin/PXL_9.MP.jpg", "leo/albums/Trip to Austin/PXL_9.MP", "leo/library/2022/07/IMG_1.jpg")

	// second account with the same bytes: one blob, two items
	jane := f.zip("takeout-20261007T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/IMG_1.jpg", a},
		{gp + "Photos from 2022/IMG_1.jpg.supplemental-metadata.json", sidecar("IMG_1.jpg", "JJJ", tJuly, "")},
	})
	rj := f.ingest("jane", jane, Options{})
	if rj.Added != 1 {
		t.Errorf("jane: %+v", rj)
	}
	f.mustExist("jane/library/2022/07/IMG_1.jpg")
	blob, _ := f.st.FindBlob(f.ctx, fileSHA(filepath.Join(f.cfg.DataDir, "jane/library/2022/07/IMG_1.jpg")))
	if blob == nil {
		t.Fatal("blob missing")
	}
	if !fsops.SameInode(filepath.Join(f.cfg.DataDir, blob.CanonicalPath), filepath.Join(f.cfg.DataDir, "jane/library/2022/07/IMG_1.jpg")) {
		t.Error("cross-account dedup did not hardlink")
	}

	// verify is clean
	v, err := f.ing.Verify(f.ctx, "leo", true)
	if err != nil {
		t.Fatal(err)
	}
	if v.MissingFiles+v.HashMismatches+v.MissingLinks+v.BadLinks+v.MissingCanonical+v.MissingVersions+v.VersionMismatches != 0 {
		t.Errorf("verify problems: %v", v.Problems)
	}
	if v.Versions != 1 {
		t.Errorf("verify saw %d kept versions, want 1", v.Versions)
	}
}

func TestResumeAfterFailure(t *testing.T) {
	f := newFixture(t)
	p := f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Photos from 2022/C.jpg", randBytes(100)},
		{gp + "Photos from 2022/C.jpg.supplemental-metadata.json", sidecar("C.jpg", "CCC", tJuly, "")},
	})
	r := f.ingest("leo", p, Options{MaxEntries: 1})
	if r.Added != 1 {
		t.Fatalf("first pass added %d", r.Added)
	}
	r = f.ingest("leo", p, Options{})
	if r.Added != 1 || r.Duplicates != 0 {
		t.Errorf("resume: %+v", r)
	}
	// simulate a crash after the file move but before the DB commit: journal replay must be harmless
	id, _ := f.st.JournalBegin(f.ctx, "create", `[{"op":"rename","src":"leo/.tmp/x","dst":"leo/library/2022/07/A.jpg"}]`)
	_ = id
	if err := f.ing.RecoverJournal(f.ctx); err != nil {
		t.Fatal(err)
	}
	if open, _ := f.st.JournalOpen(f.ctx); len(open) != 0 {
		t.Error("journal not drained")
	}
	f.mustExist("leo/library/2022/07/A.jpg", "leo/library/2022/07/C.jpg")
}

func TestParsePartName(t *testing.T) {
	eid, n, ok := ParsePartName("takeout-20261006T035113Z-1-004.zip")
	if !ok || eid != "20261006T035113Z-1" || n != 4 {
		t.Errorf("got %s %d %v", eid, n, ok)
	}
	if _, _, ok := ParsePartName("foo.zip"); ok {
		t.Error("accepted bad name")
	}
	if !ExportOlder("20261006T035113Z-1", "20261006T035113Z-2") || ExportOlder("20261106T000000Z-1", "20261006T035113Z-9") {
		t.Error("export ordering wrong")
	}
}

func sha8(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:8]
}

func (f *fixture) read(rel string) []byte {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.cfg.DataDir, rel))
	if err != nil {
		f.t.Fatal(err)
	}
	return b
}

// A part of an older export that arrives after a newer export was ingested
// must not replace the newer bytes or sidecar; its bytes are kept as a version.
func TestOlderExportKeepsNewerContent(t *testing.T) {
	f := newFixture(t)
	newer, older, newest := randBytes(120), randBytes(130), randBytes(140)
	lib := "leo/library/2022/07/A.jpg"
	f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", newer},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"favorited":true`)},
	}), Options{})

	old := f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", older},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	})
	r := f.ingest("leo", old, Options{})
	if r.Versioned != 1 || r.Changed != 0 || r.Duplicates != 0 || r.Added != 0 {
		t.Fatalf("older export: %+v", r)
	}
	if !bytes.Equal(f.read(lib), newer) {
		t.Error("older export replaced the newer bytes")
	}
	if !bytes.Contains(f.read(lib+".json"), []byte(`"favorited":true`)) {
		t.Error("older export replaced the newer sidecar")
	}
	ver := "leo/library/2022/07/A~v" + sha8(older) + ".jpg"
	f.mustExist(ver)
	if !bytes.Equal(f.read(ver), older) {
		t.Error("kept version has wrong content")
	}
	// the same older part again skips its finished entries
	r = f.ingest("leo", old, Options{})
	if r.Versioned != 0 || r.Changed != 0 || r.Added != 0 {
		t.Errorf("older export re-run: %+v", r)
	}
	// another part of the older export carrying the same file: already kept
	r = f.ingest("leo", f.zip("takeout-20261006T000000Z-1-002.zip", []file{
		{gp + "Photos from 2022/A.jpg", older},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Versioned != 0 || r.Changed != 0 || r.Duplicates != 1 {
		t.Errorf("older export, second part: %+v", r)
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
	// a newer export still supersedes, keeping the previous bytes
	r = f.ingest("leo", f.zip("takeout-20261206T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", newest},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"favorited":true`)},
	}), Options{})
	if r.Changed != 1 {
		t.Fatalf("newest export: %+v", r)
	}
	if !bytes.Equal(f.read(lib), newest) {
		t.Error("newest export did not replace the bytes")
	}
	f.mustExist("leo/library/2022/07/A~v" + sha8(newer) + ".jpg")
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A sidecar that could not be read when its media was committed is applied
// when the same part is ingested again with the sidecar readable.
func TestRepairedSidecarAppliedOnRetry(t *testing.T) {
	f := newFixture(t)
	name := "takeout-20261006T035113Z-1-001.zip"
	a, b := randBytes(100), randBytes(110)
	p := f.zip(name, []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", []byte("{not json")},
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", []byte("{not json")},
	})
	src, err := OpenZip(p)
	if err != nil {
		t.Fatal(err)
	}
	meta := PartMeta{Account: "leo", ExportID: "20261006T035113Z-1", PartNo: 1, Name: name}
	res, err := f.ing.IngestPart(f.ctx, src, meta, Options{})
	src.Close()
	if err != nil || res.Added != 2 || res.Failed != 2 || res.Status != store.ArchivePartial {
		t.Fatalf("first run: %+v %v", res, err)
	}
	f.mustExist("leo/library/2022/00/A.jpg", "leo/library/2022/00/B.jpg")

	// the retry reads both sidecars fine; B's carries no Google id
	p = f.zip(name, []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", []byte(fmt.Sprintf(`{"title":"B.jpg","photoTakenTime":{"timestamp":"%d"}}`, tMar))},
	})
	r := f.ingest("leo", p, Options{})
	if r.Status != store.ArchiveIngested || r.Failed != 0 || r.Promoted != 1 || r.Added != 0 {
		t.Fatalf("retry: %+v", r)
	}
	f.mustExist("leo/library/2022/07/A.jpg", "leo/library/2022/07/A.jpg.json", "leo/library/2022/03/B.jpg", "leo/library/2022/03/B.jpg.json")
	if f.exists("leo/library/2022/00/A.jpg") || f.exists("leo/library/2022/00/B.jpg") {
		t.Error("files left at the folder-year path")
	}
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original"); it == nil {
		t.Error("A.jpg did not get its Google identity")
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("%d pending sidecars after retry", n)
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
	// a third run changes nothing
	r = f.ingest("leo", p, Options{})
	if r.Promoted != 0 || r.Changed != 0 || r.Added != 0 || r.Duplicates != 0 {
		t.Errorf("third run: %+v", r)
	}
}

// A part whose file on Drive was replaced is ingested from scratch after
// ResetPartForReingest: entries missing from the replacement lose their album
// membership when the export is pruned again.
func TestReplacedPartIsReconciled(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	name := "takeout-" + export + "-001.zip"
	a, b := randBytes(100), randBytes(110)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("leo", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "leo", export, false); err != nil || pres.Skipped != "" || pres.Removed != 0 {
		t.Fatalf("first prune: %+v %v", pres, err)
	}
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/albums/Trip to Austin/B.jpg")
	if ids, _ := f.ing.PrunableExports(f.ctx, "leo"); len(ids) != 0 {
		t.Fatalf("export still prunable after pruning: %v", ids)
	}

	// the replacement no longer lists B in the album
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	arch, _ := f.st.GetArchive(f.ctx, acct.ID, export, 1)
	if err := f.st.ResetPartForReingest(f.ctx, arch.ID, acct.ID, export, 1); err != nil {
		t.Fatal(err)
	}
	r := f.ingest("leo", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Status != store.ArchiveIngested || r.Duplicates != 1 || r.Added != 0 || r.Unlinked != 1 {
		t.Fatalf("re-ingest: %+v", r)
	}
	if f.exists("leo/albums/Trip to Austin/B.jpg") {
		t.Error("stale album link of the replaced part survived the re-ingest")
	}
	// the listing is applied again; nothing is left for it to remove
	ids, _ := f.ing.PrunableExports(f.ctx, "leo")
	if len(ids) != 1 || ids[0] != export {
		t.Fatalf("export not prunable again: %v", ids)
	}
	pres, err := f.ing.Prune(f.ctx, "leo", export, false)
	if err != nil || pres.Skipped != "" || pres.Removed != 0 {
		t.Fatalf("second prune: %+v %v", pres, err)
	}
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/library/2022/07/A.jpg", "leo/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A crash between the journaled rename of an item (new capture month from a
// changed sidecar) and the database commit: journal replay has moved the file,
// the item still records the old path. The retry must adopt the moved file.
func TestMoveRecoversAfterCrash(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	f.ingest("leo", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	// what journal replay leaves behind: the bytes at the new month, the row at the old one
	oldAbs, newAbs := filepath.Join(f.cfg.DataDir, "leo/library/2022/07/A.jpg"), filepath.Join(f.cfg.DataDir, "leo/library/2022/03/A.jpg")
	os.MkdirAll(filepath.Dir(newAbs), 0o755)
	if err := os.Rename(oldAbs, newAbs); err != nil {
		t.Fatal(err)
	}
	r := f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tMar, "")},
	}), Options{})
	if r.Changed != 1 || r.Failed != 0 {
		t.Fatalf("retry after crash: %+v", r)
	}
	f.mustExist("leo/library/2022/03/A.jpg", "leo/library/2022/03/A.jpg.json")
	if entries, _ := filepath.Glob(filepath.Join(f.cfg.DataDir, "leo/library/2022/03/A~*.jpg")); len(entries) != 0 {
		t.Errorf("item was renamed to a suffixed path: %v", entries)
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	f := newFixture(t)
	p := f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	})
	r := f.ingest("leo", p, Options{DryRun: true})
	if r.Status != "dry-run" || r.Media != 1 {
		t.Fatalf("dry run: %+v", r)
	}
	if acct, _ := f.st.GetAccount(f.ctx, "leo"); acct != nil {
		t.Error("dry run created the account row")
	}
	if f.exists("leo") {
		t.Error("dry run created files under data_dir")
	}
}

// A newer export delivers new bytes of a known photo before its sidecar (in a
// later part). The fallback item created meanwhile must, on promotion, make
// its bytes and the arriving sidecar current on the Google-identified item
// instead of being folded away as a version.
func TestLateSidecarMergesNewerBytes(t *testing.T) {
	f := newFixture(t)
	x, y := randBytes(100), randBytes(120)
	f.ingest("leo", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, "")},
	}), Options{})
	r := f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", y},
	}), Options{})
	if r.Added != 1 {
		t.Fatalf("newer bytes without sidecar: %+v", r)
	}
	r = f.ingest("leo", f.zip("takeout-20261106T000000Z-1-002.zip", []file{
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, `,"favorited":true`)},
	}), Options{})
	if r.Merged != 1 || r.Changed != 1 || r.Failed != 0 {
		t.Fatalf("late sidecar: %+v", r)
	}
	lib := "leo/library/2022/07/X.jpg"
	if !bytes.Equal(f.read(lib), y) {
		t.Error("newer bytes are not current after the merge")
	}
	f.mustExist("leo/library/2022/07/X~v" + sha8(x) + ".jpg")
	if !bytes.Contains(f.read(lib+".json"), []byte(`"favorited":true`)) {
		t.Error("arriving sidecar was not applied")
	}
	if f.exists("leo/library/2022/00/X.jpg") {
		t.Error("fallback file left behind")
	}
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 1 {
		t.Errorf("items: %d", c["items"])
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}

	// an even newer export confirms the same bytes, again before its sidecar:
	// the merge must move the bytes' export forward
	f.ingest("leo", f.zip("takeout-20261206T000000Z-1-001.zip", []file{{gp + "Photos from 2022/X.jpg", y}}), Options{})
	r = f.ingest("leo", f.zip("takeout-20261206T000000Z-1-002.zip", []file{
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, `,"favorited":true`)},
	}), Options{})
	if r.Merged != 1 || r.Changed != 0 {
		t.Fatalf("merge with identical bytes: %+v", r)
	}
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original"); it == nil || it.BytesExportID != "20261206T000000Z-1" {
		t.Fatalf("bytes export not advanced by the merge: %+v", it)
	}
	// a delayed export from in between with other bytes may not replace them
	z := randBytes(130)
	r = f.ingest("leo", f.zip("takeout-20261120T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", z},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, "")},
	}), Options{})
	if r.Versioned != 1 || r.Changed != 0 || !bytes.Equal(f.read(lib), y) {
		t.Errorf("delayed intermediate export replaced newer bytes: %+v", r)
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A local zip re-ingested under the same part name: a changed file (detected
// by size here) or --replace forgets the earlier bookkeeping; an unchanged one
// resumes.
func TestReplacedLocalPartDetected(t *testing.T) {
	f := newFixture(t)
	name := "takeout-20261006T035113Z-1-001.zip"
	ingestSized := func(p string, opt Options) *Result {
		t.Helper()
		src, err := OpenZip(p)
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		fi, _ := os.Stat(p)
		res, err := f.ing.IngestPart(f.ctx, src, PartMeta{Account: "leo", ExportID: "20261006T035113Z-1", PartNo: 1, Name: name, Size: fi.Size()}, opt)
		if err != nil || len(res.Errors) > 0 {
			t.Fatalf("ingest: %v %v", err, res)
		}
		return res
	}
	a := randBytes(100)
	p := f.zip(name, []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	})
	ingestSized(p, Options{})
	// unchanged file: resumes, nothing re-processed
	if r := ingestSized(p, Options{}); r.Replaced || r.Duplicates != 0 || r.Added != 0 {
		t.Fatalf("unchanged re-ingest: %+v", r)
	}
	// as if sync had fetched it from Drive
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	if _, err := f.st.UpsertArchive(f.ctx, &store.Archive{AccountID: acct.ID, RemoteID: "drive-id", Name: name, ExportID: "20261006T035113Z-1", PartNo: 1, MD5: "abc", Status: store.ArchiveIngested}); err != nil {
		t.Fatal(err)
	}
	// --replace on the unchanged file re-evaluates every entry and forgets the Drive identity
	if r := ingestSized(p, Options{Replace: true}); !r.Replaced || r.Duplicates != 1 || r.Changed != 0 {
		t.Fatalf("forced replace: %+v", r)
	}
	if arch, _ := f.st.GetArchive(f.ctx, acct.ID, "20261006T035113Z-1", 1); arch.RemoteID != "" || arch.MD5 != "" {
		t.Errorf("local replacement kept the Drive identity: %+v", arch)
	}
	// a changed zip (other bytes, other size) is detected and ingested again
	b := randBytes(140)
	p = f.zip(name, []file{
		{gp + "Photos from 2022/A.jpg", b},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	})
	r := ingestSized(p, Options{})
	if !r.Replaced || r.Changed != 1 || r.Status != store.ArchiveIngested {
		t.Fatalf("changed zip: %+v", r)
	}
	if !bytes.Equal(f.read("leo/library/2022/07/A.jpg"), b) {
		t.Error("changed bytes not current")
	}
	f.mustExist("leo/library/2022/07/A~v" + sha8(a) + ".jpg")
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A delayed part of an older export lists an item in an album that a newer
// complete listing already excluded: the membership is not recreated.
func TestOlderExportDoesNotRestorePrunedMembership(t *testing.T) {
	f := newFixture(t)
	a, b := randBytes(100), randBytes(110)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "leo", "20261106T000000Z-1", false); err != nil || pres.Skipped != "" {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	r := f.ingest("leo", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	if r.Added != 1 || r.Links != 0 {
		t.Fatalf("older export: %+v", r)
	}
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/library/2022/07/B.jpg")
	if f.exists("leo/albums/Trip to Austin/B.jpg") {
		t.Error("older export recreated a membership the newer listing excluded")
	}
	// the excluded occurrence is history: it must not count as support later
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	if o, _ := f.st.FindOccurrence(f.ctx, acct.ID, "20261006T000000Z-1", "Trip ", "B.jpg"); o == nil || o.Active {
		t.Errorf("excluded album occurrence left active: %+v", o)
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A replaced part of a non-authoritative export (never pruned) drops an item
// from an album: after the re-ingest the unsupported link is removed, the one
// still listed stays.
func TestReplacedPartNonAuthoritativeUnlinks(t *testing.T) {
	f := newFixture(t)
	// an earlier export makes the next one non-authoritative
	f.ingest("leo", f.zip("takeout-20261005T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/Z.jpg", randBytes(90)},
		{gp + "Photos from 2022/Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
	}), Options{})
	const export = "20261006T035113Z-1"
	name := "takeout-" + export + "-001.zip"
	a, b := randBytes(100), randBytes(110)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("leo", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/albums/Trip to Austin/B.jpg")
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	if exp, _ := f.st.GetExport(f.ctx, acct.ID, export); exp.Authoritative {
		t.Fatal("second export unexpectedly authoritative")
	}
	r := f.ingest("leo", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{Replace: true})
	if !r.Replaced || r.Unlinked != 1 {
		t.Fatalf("re-ingest: %+v", r)
	}
	if f.exists("leo/albums/Trip to Austin/B.jpg") {
		t.Error("link of the dropped item survived")
	}
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A delayed older export brings an album that did not exist when a newer
// complete listing was applied: the newer listing did not have it, so no
// links are created for it.
func TestOlderExportDoesNotCreatePrunedAlbum(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "leo", "20261106T000000Z-1", false); err != nil || pres.Skipped != "" {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	r := f.ingest("leo", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Old Trip/metadata.json", []byte(`{"title": "Old Trip", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Old Trip/A.jpg", a},
		{gp + "Old Trip/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Links != 0 {
		t.Fatalf("older export linked into an album the newer listing never had: %+v", r)
	}
	if f.exists("leo/albums/Old Trip/A.jpg") {
		t.Error("stale album created by an older export")
	}
	f.mustExist("leo/library/2022/07/A.jpg")
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A changed sidecar title renames the library file; album links follow the
// new name and their recorded paths stay valid.
func TestRenameFollowsAlbumLinks(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("leo", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	f.mustExist("leo/library/2022/07/A.jpg", "leo/albums/Trip to Austin/A.jpg")
	r := f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("Austin skyline.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Changed != 1 {
		t.Fatalf("title change: %+v", r)
	}
	f.mustExist("leo/library/2022/07/Austin skyline.jpg", "leo/albums/Trip to Austin/Austin skyline.jpg")
	if f.exists("leo/library/2022/07/A.jpg") || f.exists("leo/albums/Trip to Austin/A.jpg") {
		t.Error("old names left behind")
	}
	if !f.sameInode("leo/library/2022/07/Austin skyline.jpg", "leo/albums/Trip to Austin/Austin skyline.jpg") {
		t.Error("renamed album link is not the item's inode")
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// While a replaced part is still being ingested again, finishing an unrelated
// part must not remove the links its deactivated occurrences supported; the
// replacement's own completion does.
func TestReconcileWaitsForReplacement(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	name := "takeout-" + export + "-001.zip"
	a, b := randBytes(100), randBytes(110)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("leo", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	arch, _ := f.st.GetArchive(f.ctx, acct.ID, export, 1)
	// sync found a replacement on Drive: reset, then fetching
	if err := f.st.ResetPartForReingest(f.ctx, arch.ID, acct.ID, export, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.st.SetArchiveStatus(f.ctx, arch.ID, store.ArchiveFetching, ""); err != nil {
		t.Fatal(err)
	}
	// an unrelated part completes meanwhile
	r := f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/Z.jpg", randBytes(90)},
		{gp + "Photos from 2022/Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
	}), Options{})
	if r.Unlinked != 0 {
		t.Fatalf("unrelated part removed links of a replacement in progress: %+v", r)
	}
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/albums/Trip to Austin/B.jpg")
	// the replacement arrives without B
	r = f.ingest("leo", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Unlinked != 1 || r.Status != store.ArchiveIngested {
		t.Fatalf("replacement: %+v", r)
	}
	if f.exists("leo/albums/Trip to Austin/B.jpg") {
		t.Error("link of the dropped item survived the replacement")
	}
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A fallback item that has its own sidecar (one without a Google id) is never
// re-matched by a later sidecar for other media whose title happens to equal
// its name.
func TestLateSidecarNeverTouchesItemsWithSidecar(t *testing.T) {
	f := newFixture(t)
	noID := []byte(fmt.Sprintf(`{"title":"A.jpg","description":"mine","photoTakenTime":{"timestamp":"%d"}}`, tJuly))
	f.ingest("leo", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", noID},
	}), Options{})
	r := f.ingest("leo", f.zip("takeout-20261006T035113Z-1-002.zip", []file{
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("A.jpg", "BBB", tMar, "")},
	}), Options{})
	if r.Promoted != 0 || r.Changed != 0 || r.PendingSidecars != 1 {
		t.Fatalf("foreign sidecar touched an item that has its own: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original"); it != nil {
		t.Error("A.jpg was promoted under B's Google id")
	}
	if !bytes.Contains(f.read("leo/library/2022/07/A.jpg.json"), []byte(`"mine"`)) {
		t.Error("A.jpg's own sidecar was replaced")
	}
}

// An occurrence deactivated by pruning that belongs to a part left partial
// forever must not hold reconciliation back; only occurrences deactivated by
// a replacement still in progress do.
func TestReconcileIgnoresPrunedOccurrencesOfPartialParts(t *testing.T) {
	f := newFixture(t)
	a, b := randBytes(100), randBytes(110)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	// export D stays partial (truncated) and lists B in the album
	r := f.ingest("leo", f.zip("takeout-20261005T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Trip /Z.jpg", randBytes(90)},
		{gp + "Trip /Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
	}), Options{MaxEntries: 1})
	if r.Status != store.ArchivePartial {
		t.Fatalf("export D: %+v", r)
	}
	f.mustExist("leo/albums/Trip to Austin/B.jpg")
	// export E's complete listing has only A: B's membership is pruned
	f.ingest("leo", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "leo", "20261106T000000Z-1", false); err != nil || pres.Removed != 1 {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	// export F (non-authoritative) lists B again, then its part is replaced without B
	const fexp = "20261206T000000Z-1"
	fname := "takeout-" + fexp + "-001.zip"
	f.ingest("leo", f.zip(fname, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.mustExist("leo/albums/Trip to Austin/B.jpg")
	r = f.ingest("leo", f.zip(fname, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{Replace: true})
	if r.Unlinked != 1 || f.exists("leo/albums/Trip to Austin/B.jpg") {
		t.Fatalf("stale link kept alive by a pruned occurrence of a partial part: %+v", r)
	}
	f.mustExist("leo/albums/Trip to Austin/A.jpg", "leo/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A part that supplied the sidecar of media living in another part is
// replaced with a corrected sidecar: the media's stored metadata follows.
func TestReplacedPartCorrectsCrossPartSidecar(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	p1 := "takeout-" + export + "-001.zip"
	f.ingest("leo", f.zip(p1, []file{
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"description":"old"`)},
	}), Options{})
	f.ingest("leo", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
	}), Options{})
	if !bytes.Contains(f.read("leo/library/2022/07/A.jpg.json"), []byte(`"old"`)) {
		t.Fatal("pending sidecar not applied by part 2")
	}
	acct, _ := f.st.GetAccount(f.ctx, "leo")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original")
	if sc, _ := f.st.GetSidecar(f.ctx, it.ID); sc == nil || sc.PartNo != 1 {
		t.Fatalf("stored sidecar does not record part 1 as its source: %+v", sc)
	}
	r := f.ingest("leo", f.zip(p1, []file{
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"description":"corrected"`)},
	}), Options{Replace: true})
	if !r.Replaced || r.PendingSidecars != 0 {
		t.Fatalf("replacement of part 1: %+v", r)
	}
	if !bytes.Contains(f.read("leo/library/2022/07/A.jpg.json"), []byte(`"corrected"`)) {
		t.Error("corrected sidecar from the replaced part not applied")
	}
	if v, err := f.ing.Verify(f.ctx, "leo", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}
