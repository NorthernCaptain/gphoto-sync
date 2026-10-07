package ingest

import (
	"archive/zip"
	"context"
	"crypto/rand"
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
