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
	"strings"
	"testing"
	"time"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/db"
	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/store"
	"gphoto-sync/internal/takeout"
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
	cfg.Accounts = map[string]config.Account{"alice": {}, "jane": {}}
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

	r1 := f.ingest("alice", part1, Options{})
	if r1.Status != store.ArchiveIngested {
		t.Fatalf("part1 status %s", r1.Status)
	}
	// 6 distinct items: IMG_1, IMG_1(1), IMG_1-edited, PXL still, PXL motion, B
	if r1.Added != 6 {
		t.Errorf("added %d want 6 (%+v)", r1.Added, r1)
	}
	f.mustExist(
		"alice/library/2022/07/IMG_1.jpg", "alice/library/2022/07/IMG_1.jpg.json",
		"alice/library/2022/03/IMG_1.jpg", // BBB, different month, same name
		"alice/library/2022/07/IMG_1-edited.jpg",
		"alice/library/2022/07/PXL_9.MP.jpg", "alice/library/2022/07/PXL_9.MP",
		"alice/library/2021/00/B.jpg", // folder year only
		"alice/albums/Trip/IMG_1.jpg", "alice/albums/Trip/PXL_9.MP.jpg", "alice/albums/Trip/PXL_9.MP",
	)
	if f.exists("alice/library/0000/00/old.jpg") || f.exists("alice/albums/Trash") {
		t.Error("trash was ingested")
	}
	if !f.sameInode("alice/library/2022/07/IMG_1.jpg", "alice/albums/Trip/IMG_1.jpg") {
		t.Error("album link is not a hardlink")
	}
	if r1.PendingSidecars != 1 {
		t.Errorf("pending sidecars %d want 1", r1.PendingSidecars)
	}

	// part 2: album descriptor renames the provisional album, B gets promoted, Z arrives for its waiting sidecar
	r2 := f.ingest("alice", part2, Options{})
	if r2.Promoted != 1 || r2.Added != 1 {
		t.Errorf("part2: promoted %d added %d (%+v)", r2.Promoted, r2.Added, r2)
	}
	f.mustExist("alice/albums/Trip to Austin/IMG_1.jpg", "alice/library/2022/03/B.jpg", "alice/library/2022/03/B.jpg.json", "alice/library/2022/07/Z.jpg")
	if f.exists("alice/albums/Trip") || f.exists("alice/library/2021/00/B.jpg") {
		t.Error("old paths still present after rename/promotion")
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
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
	r1b := f.ingest("alice", part1, Options{})
	r2b := f.ingest("alice", part2, Options{})
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
	pres, err := f.ing.Prune(f.ctx, "alice", "20261006T035113Z-1", false)
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
	r3 := f.ingest("alice", part3, Options{})
	if r3.Changed != 1 || r3.Added != 0 {
		t.Errorf("part3: %+v", r3)
	}
	entries, _ := filepath.Glob(filepath.Join(f.cfg.DataDir, "alice/library/2022/07/IMG_1~v*.jpg"))
	if len(entries) != 1 {
		t.Errorf("superseded version not kept: %v", entries)
	}
	if !f.sameInode("alice/library/2022/07/IMG_1.jpg", "alice/albums/Trip to Austin/IMG_1.jpg") {
		t.Error("album link not re-pointed after supersede")
	}

	// a later complete export where IMG_1 left the album: the stale link goes, the motion pair stays
	part4 := f.zip("takeout-20261206T000000Z-3-001.zip", []file{
		{gp + "Trip /metadata.json", []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip /PXL_9.MP.jpg", mpStill},
		{gp + "Trip /PXL_9.MP.jpg.supplemental-metadata.json", sidecar("PXL_9.MP.jpg", "CCC", tJuly, "")},
		{gp + "Trip /PXL_9.MP", mpMotion},
	})
	f.ingest("alice", part4, Options{FullAlbums: true, Parts: 1})
	pres, err = f.ing.Prune(f.ctx, "alice", "20261206T000000Z-3", false)
	if err != nil {
		t.Fatal(err)
	}
	if pres.Removed != 1 || f.exists("alice/albums/Trip to Austin/IMG_1.jpg") {
		t.Errorf("prune did not remove the stale link: %+v", pres)
	}
	f.mustExist("alice/albums/Trip to Austin/PXL_9.MP.jpg", "alice/albums/Trip to Austin/PXL_9.MP", "alice/library/2022/07/IMG_1.jpg")

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
	v, err := f.ing.Verify(f.ctx, "alice", true)
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
	r := f.ingest("alice", p, Options{MaxEntries: 1})
	if r.Added != 1 {
		t.Fatalf("first pass added %d", r.Added)
	}
	r = f.ingest("alice", p, Options{})
	if r.Added != 1 || r.Duplicates != 0 {
		t.Errorf("resume: %+v", r)
	}
	// simulate a crash after the file move but before the DB commit: journal replay must be harmless
	id, _ := f.st.JournalBegin(f.ctx, "create", `[{"op":"rename","src":"alice/.tmp/x","dst":"alice/library/2022/07/A.jpg"}]`)
	_ = id
	if err := f.ing.RecoverJournal(f.ctx); err != nil {
		t.Fatal(err)
	}
	if open, _ := f.st.JournalOpen(f.ctx); len(open) != 0 {
		t.Error("journal not drained")
	}
	f.mustExist("alice/library/2022/07/A.jpg", "alice/library/2022/07/C.jpg")
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
	lib := "alice/library/2022/07/A.jpg"
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", newer},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"favorited":true`)},
	}), Options{})

	old := f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", older},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	})
	r := f.ingest("alice", old, Options{})
	if r.Versioned != 1 || r.Changed != 0 || r.Duplicates != 0 || r.Added != 0 {
		t.Fatalf("older export: %+v", r)
	}
	if !bytes.Equal(f.read(lib), newer) {
		t.Error("older export replaced the newer bytes")
	}
	if !bytes.Contains(f.read(lib+".json"), []byte(`"favorited":true`)) {
		t.Error("older export replaced the newer sidecar")
	}
	ver := "alice/library/2022/07/A~v" + sha8(older) + ".jpg"
	f.mustExist(ver)
	if !bytes.Equal(f.read(ver), older) {
		t.Error("kept version has wrong content")
	}
	// the same older part again skips its finished entries
	r = f.ingest("alice", old, Options{})
	if r.Versioned != 0 || r.Changed != 0 || r.Added != 0 {
		t.Errorf("older export re-run: %+v", r)
	}
	// another part of the older export carrying the same file: already kept
	r = f.ingest("alice", f.zip("takeout-20261006T000000Z-1-002.zip", []file{
		{gp + "Photos from 2022/A.jpg", older},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Versioned != 0 || r.Changed != 0 || r.Duplicates != 1 {
		t.Errorf("older export, second part: %+v", r)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
	// a newer export still supersedes, keeping the previous bytes
	r = f.ingest("alice", f.zip("takeout-20261206T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", newest},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"favorited":true`)},
	}), Options{})
	if r.Changed != 1 {
		t.Fatalf("newest export: %+v", r)
	}
	if !bytes.Equal(f.read(lib), newest) {
		t.Error("newest export did not replace the bytes")
	}
	f.mustExist("alice/library/2022/07/A~v" + sha8(newer) + ".jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
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
	meta := PartMeta{Account: "alice", ExportID: "20261006T035113Z-1", PartNo: 1, Name: name}
	res, err := f.ing.IngestPart(f.ctx, src, meta, Options{})
	src.Close()
	if err != nil || res.Added != 2 || res.Failed != 2 || res.Status != store.ArchivePartial {
		t.Fatalf("first run: %+v %v", res, err)
	}
	f.mustExist("alice/library/2022/00/A.jpg", "alice/library/2022/00/B.jpg")

	// the retry reads both sidecars fine; B's carries no Google id
	p = f.zip(name, []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", []byte(fmt.Sprintf(`{"title":"B.jpg","photoTakenTime":{"timestamp":"%d"}}`, tMar))},
	})
	r := f.ingest("alice", p, Options{})
	if r.Status != store.ArchiveIngested || r.Failed != 0 || r.Promoted != 1 || r.Added != 0 {
		t.Fatalf("retry: %+v", r)
	}
	f.mustExist("alice/library/2022/07/A.jpg", "alice/library/2022/07/A.jpg.json", "alice/library/2022/03/B.jpg", "alice/library/2022/03/B.jpg.json")
	if f.exists("alice/library/2022/00/A.jpg") || f.exists("alice/library/2022/00/B.jpg") {
		t.Error("files left at the folder-year path")
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original"); it == nil {
		t.Error("A.jpg did not get its Google identity")
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("%d pending sidecars after retry", n)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
	// a third run changes nothing
	r = f.ingest("alice", p, Options{})
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
	f.ingest("alice", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "alice", export, false); err != nil || pres.Skipped != "" || pres.Removed != 0 {
		t.Fatalf("first prune: %+v %v", pres, err)
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/albums/Trip to Austin/B.jpg")
	if ids, _ := f.ing.PrunableExports(f.ctx, "alice"); len(ids) != 0 {
		t.Fatalf("export still prunable after pruning: %v", ids)
	}

	// the replacement no longer lists B in the album
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	arch, _ := f.st.GetArchive(f.ctx, acct.ID, export, 1)
	if err := f.st.ResetPartForReingest(f.ctx, arch.ID, acct.ID, export, 1); err != nil {
		t.Fatal(err)
	}
	r := f.ingest("alice", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Status != store.ArchiveIngested || r.Duplicates != 1 || r.Added != 0 || r.Unlinked != 1 {
		t.Fatalf("re-ingest: %+v", r)
	}
	if f.exists("alice/albums/Trip to Austin/B.jpg") {
		t.Error("stale album link of the replaced part survived the re-ingest")
	}
	// the listing is applied again; nothing is left for it to remove
	ids, _ := f.ing.PrunableExports(f.ctx, "alice")
	if len(ids) != 1 || ids[0] != export {
		t.Fatalf("export not prunable again: %v", ids)
	}
	pres, err := f.ing.Prune(f.ctx, "alice", export, false)
	if err != nil || pres.Skipped != "" || pres.Removed != 0 {
		t.Fatalf("second prune: %+v %v", pres, err)
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/library/2022/07/A.jpg", "alice/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A crash between the journaled rename of an item (new capture month from a
// changed sidecar) and the database commit: journal replay has moved the file,
// the item still records the old path. The retry must adopt the moved file.
func TestMoveRecoversAfterCrash(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	// what journal replay leaves behind: the bytes at the new month, the row at the old one
	oldAbs, newAbs := filepath.Join(f.cfg.DataDir, "alice/library/2022/07/A.jpg"), filepath.Join(f.cfg.DataDir, "alice/library/2022/03/A.jpg")
	os.MkdirAll(filepath.Dir(newAbs), 0o755)
	if err := os.Rename(oldAbs, newAbs); err != nil {
		t.Fatal(err)
	}
	r := f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tMar, "")},
	}), Options{})
	if r.Changed != 1 || r.Failed != 0 {
		t.Fatalf("retry after crash: %+v", r)
	}
	f.mustExist("alice/library/2022/03/A.jpg", "alice/library/2022/03/A.jpg.json")
	if entries, _ := filepath.Glob(filepath.Join(f.cfg.DataDir, "alice/library/2022/03/A~*.jpg")); len(entries) != 0 {
		t.Errorf("item was renamed to a suffixed path: %v", entries)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	f := newFixture(t)
	p := f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	})
	r := f.ingest("alice", p, Options{DryRun: true})
	if r.Status != "dry-run" || r.Media != 1 {
		t.Fatalf("dry run: %+v", r)
	}
	if acct, _ := f.st.GetAccount(f.ctx, "alice"); acct != nil {
		t.Error("dry run created the account row")
	}
	if f.exists("alice") {
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
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, "")},
	}), Options{})
	r := f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", y},
	}), Options{})
	if r.Added != 1 {
		t.Fatalf("newer bytes without sidecar: %+v", r)
	}
	r = f.ingest("alice", f.zip("takeout-20261106T000000Z-1-002.zip", []file{
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, `,"favorited":true`)},
	}), Options{})
	if r.Merged != 1 || r.Changed != 1 || r.Failed != 0 {
		t.Fatalf("late sidecar: %+v", r)
	}
	lib := "alice/library/2022/07/X.jpg"
	if !bytes.Equal(f.read(lib), y) {
		t.Error("newer bytes are not current after the merge")
	}
	f.mustExist("alice/library/2022/07/X~v" + sha8(x) + ".jpg")
	if !bytes.Contains(f.read(lib+".json"), []byte(`"favorited":true`)) {
		t.Error("arriving sidecar was not applied")
	}
	if f.exists("alice/library/2022/00/X.jpg") {
		t.Error("fallback file left behind")
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 1 {
		t.Errorf("items: %d", c["items"])
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}

	// an even newer export confirms the same bytes, again before its sidecar:
	// the merge must move the bytes' export forward
	f.ingest("alice", f.zip("takeout-20261206T000000Z-1-001.zip", []file{{gp + "Photos from 2022/X.jpg", y}}), Options{})
	r = f.ingest("alice", f.zip("takeout-20261206T000000Z-1-002.zip", []file{
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
	r = f.ingest("alice", f.zip("takeout-20261120T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", z},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, "")},
	}), Options{})
	if r.Versioned != 1 || r.Changed != 0 || !bytes.Equal(f.read(lib), y) {
		t.Errorf("delayed intermediate export replaced newer bytes: %+v", r)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
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
		res, err := f.ing.IngestPart(f.ctx, src, PartMeta{Account: "alice", ExportID: "20261006T035113Z-1", PartNo: 1, Name: name, Size: fi.Size()}, opt)
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
	// as if sync had fetched it from Drive (with errors: nothing was ever staged)
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	arch, err := f.st.UpsertArchive(f.ctx, &store.Archive{AccountID: acct.ID, RemoteID: "drive-id", Name: name, ExportID: "20261006T035113Z-1", PartNo: 1, MD5: "abc", Status: store.ArchiveWithErrors})
	if err != nil {
		t.Fatal(err)
	}
	// --replace on the unchanged file re-evaluates every entry and forgets the Drive identity
	if r := ingestSized(p, Options{Replace: true}); !r.Replaced || r.Duplicates != 1 || r.Changed != 0 {
		t.Fatalf("forced replace: %+v", r)
	}
	if arch, _ = f.st.GetArchive(f.ctx, acct.ID, "20261006T035113Z-1", 1); arch.RemoteID != "" || arch.MD5 != "" {
		t.Errorf("local replacement kept the Drive identity: %+v", arch)
	}
	// a copy moved into the Drive staging folder whose delete failed: only sync can finish it
	if _, err := f.st.UpsertArchive(f.ctx, &store.Archive{AccountID: acct.ID, RemoteID: "drive-id-2", Name: name, ExportID: "20261006T035113Z-1", PartNo: 1, MD5: "abc", Status: store.ArchiveIngested}); err != nil {
		t.Fatal(err)
	}
	if err := f.st.SetArchiveStaged(f.ctx, arch.ID); err != nil {
		t.Fatal(err)
	}
	src, err := OpenZip(p)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.ing.IngestPart(f.ctx, src, PartMeta{Account: "alice", ExportID: "20261006T035113Z-1", PartNo: 1, Name: name}, Options{Replace: true})
	src.Close()
	if err == nil || !strings.Contains(err.Error(), "staged for deletion") {
		t.Fatalf("local replace with a staged copy was not refused: %v", err)
	}
	if err := f.st.SetArchiveRemoteDeleted(f.ctx, arch.ID); err != nil {
		t.Fatal(err)
	}
	if r := ingestSized(p, Options{Replace: true}); !r.Replaced {
		t.Fatalf("replace after the staged deletion finished: %+v", r)
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
	if !bytes.Equal(f.read("alice/library/2022/07/A.jpg"), b) {
		t.Error("changed bytes not current")
	}
	f.mustExist("alice/library/2022/07/A~v" + sha8(a) + ".jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A delayed part of an older export lists an item in an album that a newer
// complete listing already excluded: the membership is not recreated.
func TestOlderExportDoesNotRestorePrunedMembership(t *testing.T) {
	f := newFixture(t)
	a, b := randBytes(100), randBytes(110)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "alice", "20261106T000000Z-1", false); err != nil || pres.Skipped != "" {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	r := f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	if r.Added != 1 || r.Links != 0 {
		t.Fatalf("older export: %+v", r)
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/library/2022/07/B.jpg")
	if f.exists("alice/albums/Trip to Austin/B.jpg") {
		t.Error("older export recreated a membership the newer listing excluded")
	}
	// the excluded occurrence is history: it must not count as support later
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if o, _ := f.st.FindOccurrence(f.ctx, acct.ID, "20261006T000000Z-1", "Trip ", "B.jpg"); o == nil || o.Active {
		t.Errorf("excluded album occurrence left active: %+v", o)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A replaced part of a non-authoritative export (never pruned) drops an item
// from an album: after the re-ingest the unsupported link is removed, the one
// still listed stays.
func TestReplacedPartNonAuthoritativeUnlinks(t *testing.T) {
	f := newFixture(t)
	// an earlier export makes the next one non-authoritative
	f.ingest("alice", f.zip("takeout-20261005T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/Z.jpg", randBytes(90)},
		{gp + "Photos from 2022/Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
	}), Options{})
	const export = "20261006T035113Z-1"
	name := "takeout-" + export + "-001.zip"
	a, b := randBytes(100), randBytes(110)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("alice", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/albums/Trip to Austin/B.jpg")
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if exp, _ := f.st.GetExport(f.ctx, acct.ID, export); exp.Authoritative {
		t.Fatal("second export unexpectedly authoritative")
	}
	r := f.ingest("alice", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{Replace: true})
	if !r.Replaced || r.Unlinked != 1 {
		t.Fatalf("re-ingest: %+v", r)
	}
	if f.exists("alice/albums/Trip to Austin/B.jpg") {
		t.Error("link of the dropped item survived")
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A delayed older export brings an album that did not exist when a newer
// complete listing was applied: the newer listing did not have it, so no
// links are created for it.
func TestOlderExportDoesNotCreatePrunedAlbum(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "alice", "20261106T000000Z-1", false); err != nil || pres.Skipped != "" {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	r := f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Old Trip/metadata.json", []byte(`{"title": "Old Trip", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Old Trip/A.jpg", a},
		{gp + "Old Trip/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Links != 0 {
		t.Fatalf("older export linked into an album the newer listing never had: %+v", r)
	}
	if f.exists("alice/albums/Old Trip/A.jpg") {
		t.Error("stale album created by an older export")
	}
	f.mustExist("alice/library/2022/07/A.jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A changed sidecar title renames the library file; album links follow the
// new name and their recorded paths stay valid.
func TestRenameFollowsAlbumLinks(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	f.mustExist("alice/library/2022/07/A.jpg", "alice/albums/Trip to Austin/A.jpg")
	r := f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("Austin skyline.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Changed != 1 {
		t.Fatalf("title change: %+v", r)
	}
	f.mustExist("alice/library/2022/07/Austin skyline.jpg", "alice/albums/Trip to Austin/Austin skyline.jpg")
	if f.exists("alice/library/2022/07/A.jpg") || f.exists("alice/albums/Trip to Austin/A.jpg") {
		t.Error("old names left behind")
	}
	if !f.sameInode("alice/library/2022/07/Austin skyline.jpg", "alice/albums/Trip to Austin/Austin skyline.jpg") {
		t.Error("renamed album link is not the item's inode")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
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
	f.ingest("alice", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	arch, _ := f.st.GetArchive(f.ctx, acct.ID, export, 1)
	// sync found a replacement on Drive: reset, then fetching
	if err := f.st.ResetPartForReingest(f.ctx, arch.ID, acct.ID, export, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.st.SetArchiveStatus(f.ctx, arch.ID, store.ArchiveFetching, ""); err != nil {
		t.Fatal(err)
	}
	// an unrelated part completes meanwhile
	r := f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/Z.jpg", randBytes(90)},
		{gp + "Photos from 2022/Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
	}), Options{})
	if r.Unlinked != 0 {
		t.Fatalf("unrelated part removed links of a replacement in progress: %+v", r)
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/albums/Trip to Austin/B.jpg")
	// the replacement arrives without B
	r = f.ingest("alice", f.zip(name, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Unlinked != 1 || r.Status != store.ArchiveIngested {
		t.Fatalf("replacement: %+v", r)
	}
	if f.exists("alice/albums/Trip to Austin/B.jpg") {
		t.Error("link of the dropped item survived the replacement")
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A fallback item that has its own sidecar (one without a Google id) is never
// re-matched by a later sidecar for other media whose title happens to equal
// its name.
func TestLateSidecarNeverTouchesItemsWithSidecar(t *testing.T) {
	f := newFixture(t)
	noID := []byte(fmt.Sprintf(`{"title":"A.jpg","description":"mine","photoTakenTime":{"timestamp":"%d"}}`, tJuly))
	f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", noID},
	}), Options{})
	r := f.ingest("alice", f.zip("takeout-20261006T035113Z-1-002.zip", []file{
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("A.jpg", "BBB", tMar, "")},
	}), Options{})
	if r.Promoted != 0 || r.Changed != 0 || r.PendingSidecars != 1 {
		t.Fatalf("foreign sidecar touched an item that has its own: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original"); it != nil {
		t.Error("A.jpg was promoted under B's Google id")
	}
	if !bytes.Contains(f.read("alice/library/2022/07/A.jpg.json"), []byte(`"mine"`)) {
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
	r := f.ingest("alice", f.zip("takeout-20261005T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Trip /Z.jpg", randBytes(90)},
		{gp + "Trip /Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
	}), Options{MaxEntries: 1})
	if r.Status != store.ArchivePartial {
		t.Fatalf("export D: %+v", r)
	}
	f.mustExist("alice/albums/Trip to Austin/B.jpg")
	// export E's complete listing has only A: B's membership is pruned
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "alice", "20261106T000000Z-1", false); err != nil || pres.Removed != 1 {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	// export F (non-authoritative) lists B again, then its part is replaced without B
	const fexp = "20261206T000000Z-1"
	fname := "takeout-" + fexp + "-001.zip"
	f.ingest("alice", f.zip(fname, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /B.jpg", b},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip to Austin/B.jpg")
	r = f.ingest("alice", f.zip(fname, []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{Replace: true})
	if r.Unlinked != 1 || f.exists("alice/albums/Trip to Austin/B.jpg") {
		t.Fatalf("stale link kept alive by a pruned occurrence of a partial part: %+v", r)
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg", "alice/library/2022/07/B.jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A part that supplied the sidecar of media living in another part is
// replaced with a corrected sidecar: the media's stored metadata follows.
func TestReplacedPartCorrectsCrossPartSidecar(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	p1 := "takeout-" + export + "-001.zip"
	f.ingest("alice", f.zip(p1, []file{
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"description":"old"`)},
	}), Options{})
	f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
	}), Options{})
	if !bytes.Contains(f.read("alice/library/2022/07/A.jpg.json"), []byte(`"old"`)) {
		t.Fatal("pending sidecar not applied by part 2")
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original")
	if sc, _ := f.st.GetSidecar(f.ctx, it.ID); sc == nil || sc.PartNo != 1 {
		t.Fatalf("stored sidecar does not record part 1 as its source: %+v", sc)
	}
	r := f.ingest("alice", f.zip(p1, []file{
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"description":"corrected"`)},
	}), Options{Replace: true})
	if !r.Replaced || r.PendingSidecars != 0 {
		t.Fatalf("replacement of part 1: %+v", r)
	}
	if !bytes.Contains(f.read("alice/library/2022/07/A.jpg.json"), []byte(`"corrected"`)) {
		t.Error("corrected sidecar from the replaced part not applied")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// An older export's sidecar that gives a fallback item its Google id must not
// replace metadata a newer export already stored for it.
func TestPromotionKeepsNewerStoredSidecar(t *testing.T) {
	f := newFixture(t)
	x := randBytes(100)
	noID := []byte(fmt.Sprintf(`{"title":"X.jpg","description":"newer","photoTakenTime":{"timestamp":"%d"}}`, tJuly))
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", noID},
	}), Options{})
	r := f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, `,"description":"older"`)},
	}), Options{})
	if r.Promoted != 1 {
		t.Fatalf("older export: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if it == nil {
		t.Fatal("identity from the older export not applied")
	}
	sc, _ := f.st.GetSidecar(f.ctx, it.ID)
	if sc == nil || !bytes.Contains(sc.JSON, []byte(`"newer"`)) || sc.ExportID != "20261106T000000Z-1" {
		t.Errorf("newer stored sidecar replaced by the older export's: %+v", sc)
	}
	if !bytes.Contains(f.read(it.LibPath+".json"), []byte(`"newer"`)) {
		t.Error("sidecar file does not hold the newer metadata")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A replaced part no longer carries a sidecar it had supplied for media in
// another part: the metadata stays but its provenance is cleared, and a later
// exact sidecar from any export replaces it.
func TestReplacedPartReleasesOrphanedSidecars(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	p1 := "takeout-" + export + "-001.zip"
	f.ingest("alice", f.zip(p1, []file{
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"description":"from part 1"`)},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/B.jpg", randBytes(110)},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	a, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original")
	b, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original")
	if a == nil || b == nil {
		t.Fatal("pending sidecars not applied by part 2")
	}
	// the replacement of part 1 keeps B's sidecar but drops A's
	r := f.ingest("alice", f.zip(p1, []file{
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{Replace: true})
	if !r.Replaced || r.SidecarsOrphaned != 1 {
		t.Fatalf("replacement: %+v", r)
	}
	sa, _ := f.st.GetSidecar(f.ctx, a.ID)
	sb, _ := f.st.GetSidecar(f.ctx, b.ID)
	if sa == nil || sa.ExportID != "" || !bytes.Contains(sa.JSON, []byte("from part 1")) {
		t.Errorf("orphaned sidecar: %+v", sa)
	}
	if sb == nil || sb.ExportID != export || sb.PartNo != 1 {
		t.Errorf("confirmed sidecar lost its provenance: %+v", sb)
	}
	f.mustExist("alice/library/2022/07/A.jpg.json")
	// a later export resending the photo with its sidecar replaces the orphaned
	// one (unknown provenance never wins)
	r = f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", f.read("alice/library/2022/07/A.jpg")},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"description":"resent"`)},
	}), Options{})
	if r.Failed != 0 {
		t.Fatalf("later export: %+v", r)
	}
	if sa, _ = f.st.GetSidecar(f.ctx, a.ID); sa == nil || !bytes.Contains(sa.JSON, []byte("resent")) || sa.ExportID != "20261106T000000Z-1" {
		t.Errorf("orphaned sidecar not replaced by a later exact one: %+v", sa)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// Media seen only in an album whose membership a newer complete listing then
// withdrew still receives its delayed sidecar: identity and metadata are
// applied, the album link stays retired.
func TestLateSidecarReachesPrunedAlbumOccurrence(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	// export C part 1: the photo only in the album, without its sidecar
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /A.jpg", a},
	}), Options{})
	f.mustExist("alice/albums/Trip to Austin/A.jpg")
	// export D's complete listing no longer has it: membership withdrawn
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /Z.jpg", randBytes(90)},
		{gp + "Trip /Z.jpg.supplemental-metadata.json", sidecar("Z.jpg", "ZZZ", tJuly, "")},
	}), Options{FullAlbums: true, Parts: 1})
	if pres, err := f.ing.Prune(f.ctx, "alice", "20261106T000000Z-1", false); err != nil || pres.Removed != 1 {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	// export C part 2 arrives late with the sidecar
	r := f.ingest("alice", f.zip("takeout-20261006T000000Z-1-002.zip", []file{
		{gp + "Trip /A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Promoted != 1 || r.PendingSidecars != 0 || r.Links != 0 {
		t.Fatalf("late sidecar for a pruned occurrence: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original"); it == nil {
		t.Fatal("identity not applied")
	}
	f.mustExist("alice/library/2022/07/A.jpg", "alice/library/2022/07/A.jpg.json")
	if f.exists("alice/albums/Trip to Austin/A.jpg") {
		t.Error("withdrawn album link recreated by the late sidecar")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// While a part's replacement is pending, an identical sidecar from an older
// export does not confirm the part; the release still happens when the
// replacement finishes without that sidecar.
func TestOlderExportDoesNotConfirmReplacedPartSidecar(t *testing.T) {
	f := newFixture(t)
	const export = "20261106T000000Z-1"
	p1 := "takeout-" + export + "-001.zip"
	a := randBytes(100)
	sc := sidecar("A.jpg", "AAA", tJuly, `,"description":"same"`)
	f.ingest("alice", f.zip(p1, []file{{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sc}}), Options{})
	f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Photos from 2022/A.jpg", a}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	arch, _ := f.st.GetArchive(f.ctx, acct.ID, export, 1)
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original")
	// sync found part 1 replaced: pending re-ingest
	if err := f.st.ResetPartForReingest(f.ctx, arch.ID, acct.ID, export, 1); err != nil {
		t.Fatal(err)
	}
	if row, _ := f.st.GetSidecar(f.ctx, it.ID); row == nil || !row.Reconfirm {
		t.Fatalf("sidecar of the replaced part not awaiting confirmation: %+v", row)
	}
	// an older export delivers the photo with the identical sidecar meanwhile
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sc},
	}), Options{})
	if row, _ := f.st.GetSidecar(f.ctx, it.ID); row == nil || !row.Reconfirm || row.ExportID != export || row.PartNo != 1 {
		t.Fatalf("older export confirmed or took over the sidecar: %+v", row)
	}
	// the replacement arrives without the sidecar: provenance released
	r := f.ingest("alice", f.zip(p1, []file{{gp + "Photos from 2022/Other.jpg.supplemental-metadata.json", sidecar("Other.jpg", "OOO", tJuly, "")}}), Options{})
	if r.SidecarsOrphaned != 1 {
		t.Fatalf("replacement: %+v", r)
	}
	if row, _ := f.st.GetSidecar(f.ctx, it.ID); row == nil || row.ExportID != "" || row.Reconfirm {
		t.Errorf("dropped sidecar kept its provenance: %+v", row)
	}
}

// A replacement keeps a sidecar but moves its media to a later part: the
// sidecar is stored as pending despite the old (reset) occurrence, and the
// later part's media picks it up.
func TestReplacementMovingMediaToLaterPartKeepsSidecarPending(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	p1 := "takeout-" + export + "-001.zip"
	a := randBytes(100)
	f.ingest("alice", f.zip(p1, []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	r := f.ingest("alice", f.zip(p1, []file{
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, `,"description":"corrected"`)},
	}), Options{Replace: true})
	if !r.Replaced || r.PendingSidecars != 1 {
		t.Fatalf("replacement without the media: %+v", r)
	}
	r = f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Photos from 2022/A.jpg", a}}), Options{})
	if r.Changed != 1 {
		t.Fatalf("media in the later part: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original")
	if sc, _ := f.st.GetSidecar(f.ctx, it.ID); sc == nil || !bytes.Contains(sc.JSON, []byte("corrected")) || sc.PartNo != 1 {
		t.Errorf("corrected sidecar not applied via pending: %+v", sc)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// Merging a fallback item that carries a newer export's sidecar into the
// item holding its Google id keeps that newer metadata on the survivor, even
// when the sidecar establishing the identity comes from an older export.
func TestMergeKeepsNewerFallbackSidecar(t *testing.T) {
	f := newFixture(t)
	x := randBytes(100)
	// the identity holder, from an older export, with its own sidecar
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, `,"description":"older"`)},
	}), Options{})
	// a newer export: same photo under another name, sidecar without a Google id
	noID := []byte(fmt.Sprintf(`{"title":"X2.jpg","description":"newer","photoTakenTime":{"timestamp":"%d"}}`, tJuly))
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X2.jpg", x},
		{gp + "Photos from 2022/X2.jpg.supplemental-metadata.json", noID},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:20261106T000000Z-1#Photos from 2022#X2.jpg")
	if fb == nil {
		t.Fatal("fallback item not found")
	}
	// the newer export's part is meanwhile being replaced: its sidecar awaits reconfirmation
	fbArch, _ := f.st.GetArchive(f.ctx, acct.ID, "20261106T000000Z-1", 1)
	if err := f.st.ResetPartForReingest(f.ctx, fbArch.ID, acct.ID, "20261106T000000Z-1", 1); err != nil {
		t.Fatal(err)
	}
	// a delayed part of the older export identifies the fallback: promotion merges it
	pr := &partRun{ing: f.ing, ctx: f.ctx, acct: acct, part: PartMeta{Account: "alice", ExportID: "20261006T000000Z-1", PartNo: 2, Name: "takeout-20261006T000000Z-1-002.zip"}, folders: map[string]*folderCtx{}}
	arriving, err := takeoutParse(sidecar("X2.jpg", "XXX", tJuly, `,"description":"older too"`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pr.promote(fb, arriving, "original", "XXX", "XXX#original"); err != nil {
		t.Fatal(err)
	}
	target, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	sc, _ := f.st.GetSidecar(f.ctx, target.ID)
	if sc == nil || !bytes.Contains(sc.JSON, []byte(`"newer"`)) || sc.ExportID != "20261106T000000Z-1" {
		t.Errorf("newer metadata lost in the merge: %+v", sc)
	}
	if sc != nil && !sc.Reconfirm {
		t.Error("the pending reconfirmation did not follow the sidecar to the survivor")
	}
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 1 {
		t.Errorf("items after merge: %d", c["items"])
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

func takeoutParse(raw []byte) (*takeout.Sidecar, error) { return takeout.ParseSidecar(raw) }

// After a replacement dropped a media entry, a later part's sidecar of the
// same name is not applied to the removed photo: it waits for its media.
func TestRemovedEntryDoesNotTakeLaterSidecar(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	p1 := "takeout-" + export + "-001.zip"
	f.ingest("alice", f.zip(p1, []file{{gp + "Photos from 2022/A.jpg", randBytes(100)}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	removed, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Photos from 2022#A.jpg")
	if removed == nil {
		t.Fatal("fallback item not found")
	}
	// the replacement no longer carries A.jpg
	r := f.ingest("alice", f.zip(p1, []file{{gp + "Photos from 2022/Z.jpg", randBytes(90)}}), Options{Replace: true})
	if !r.Replaced {
		t.Fatalf("replacement: %+v", r)
	}
	// a later part brings a sidecar named for A.jpg: not for the removed photo
	r = f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.Promoted != 0 || r.PendingSidecars != 1 {
		t.Fatalf("sidecar applied to a removed entry: %+v", r)
	}
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original"); it != nil {
		t.Error("removed photo took the later sidecar's identity")
	}
	if sc, _ := f.st.GetSidecar(f.ctx, removed.ID); sc != nil {
		t.Error("removed photo received a sidecar")
	}
}

// While a part is being replaced, a fallback sidecar from that part that still
// awaits reconfirmation does not displace the target's sidecar the replacement
// has already confirmed.
func TestMergePrefersConfirmedTargetSidecar(t *testing.T) {
	f := newFixture(t)
	x := randBytes(100)
	const export = "20261106T000000Z-1"
	noID := []byte(fmt.Sprintf(`{"title":"X2.jpg","description":"fallback copy","photoTakenTime":{"timestamp":"%d"}}`, tJuly))
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, `,"description":"confirmed"`)},
		{gp + "Photos from 2022/X2.jpg", x},
		{gp + "Photos from 2022/X2.jpg.supplemental-metadata.json", noID},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	target, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Photos from 2022#X2.jpg")
	arch, _ := f.st.GetArchive(f.ctx, acct.ID, export, 1)
	if err := f.st.ResetPartForReingest(f.ctx, arch.ID, acct.ID, export, 1); err != nil {
		t.Fatal(err)
	}
	// another part of the same export has confirmed the target's sidecar, not
	// (yet) the fallback's
	if err := f.st.SetSidecarExport(f.ctx, target.ID, export, 2); err != nil {
		t.Fatal(err)
	}
	pr := &partRun{ing: f.ing, ctx: f.ctx, acct: acct, part: PartMeta{Account: "alice", ExportID: "20261006T000000Z-1", PartNo: 2, Name: "takeout-20261006T000000Z-1-002.zip"}, folders: map[string]*folderCtx{}}
	arriving, _ := takeoutParse(sidecar("X2.jpg", "XXX", tJuly, `,"description":"older"`))
	if _, err := pr.promote(fb, arriving, "original", "XXX", "XXX#original"); err != nil {
		t.Fatal(err)
	}
	sc, _ := f.st.GetSidecar(f.ctx, target.ID)
	if sc == nil || !bytes.Contains(sc.JSON, []byte(`"confirmed"`)) || sc.Reconfirm {
		t.Errorf("confirmed target sidecar displaced or flagged: %+v", sc)
	}
}

// Takeout puts a photo's JSON into every album folder but the photo itself
// into one folder only: such sidecar-only album entries become album links,
// whether the photo was ingested earlier, in the same part, or later.
func TestSidecarOnlyAlbumEntriesBecomeLinks(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Serbia 2025", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	a, b := randBytes(100), randBytes(110)
	// part 1: A in its year folder; the album carries only A's and B's sidecars
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Serbia 2025/metadata.json", meta},
		{gp + "Serbia 2025/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Serbia 2025/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	if r.SidecarLinks != 1 {
		t.Fatalf("part 1: %+v", r)
	}
	f.mustExist("alice/albums/Serbia 2025/A.jpg")
	if !f.sameInode("alice/albums/Serbia 2025/A.jpg", "alice/library/2022/07/A.jpg") {
		t.Error("album link is not the photo's inode")
	}
	// part 2 brings B in its year folder: the album link follows
	r = f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	if r.SidecarLinks != 1 {
		t.Fatalf("part 2: %+v", r)
	}
	f.mustExist("alice/albums/Serbia 2025/B.jpg")
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("pending sidecars left: %d", n)
	}
	// the membership counts as export evidence: a complete listing keeps it
	if err := f.st.SetExportConfirmedParts(f.ctx, acct.ID, export, 2, true); err != nil {
		t.Fatal(err)
	}
	if pres, err := f.ing.Prune(f.ctx, "alice", export, false); err != nil || pres.Skipped != "" || pres.Removed != 0 {
		t.Fatalf("prune: %+v %v", pres, err)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A sidecar named after a title without extension ("X.json" next to "X.jpg")
// describes that photo.
func TestSidecarNamedWithoutExtension(t *testing.T) {
	f := newFixture(t)
	r := f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2012/11_16_12 - 1.jpg", randBytes(100)},
		{gp + "Photos from 2012/11_16_12 - 1.json", sidecar("11/16/12 - 1", "AAA", tJuly, "")},
	}), Options{})
	if r.Added != 1 || r.PendingSidecars != 0 {
		t.Fatalf("ingest: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original")
	if it == nil {
		t.Fatal("photo did not get its sidecar's identity")
	}
	f.mustExist(it.LibPath + ".json")
	// the pending store resolves the same way when the sidecar came in an earlier part
	r = f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2013/New Year - 1.json", sidecar("New Year - 1", "NNN", tJuly, "")},
	}), Options{})
	if r.PendingSidecars != 1 {
		t.Fatalf("sidecar-only part: %+v", r)
	}
	r = f.ingest("alice", f.zip("takeout-20261106T000000Z-1-002.zip", []file{
		{gp + "Photos from 2013/New Year - 1.jpg", randBytes(90)},
	}), Options{})
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "NNN#original"); it == nil {
		t.Errorf("pending sidecar without extension not applied: %+v", r)
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("resolved stem sidecar still counted as pending: %d", n)
	}
	// an edited file keeps its variant under this rule
	r = f.ingest("alice", f.zip("takeout-20261206T000000Z-1-001.zip", []file{
		{gp + "Photos from 2014/Trip - 2-edited.jpg", randBytes(80)},
		{gp + "Photos from 2014/Trip - 2-edited.json", sidecar("Trip - 2", "EEE", tJuly, "")},
	}), Options{})
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "EEE#edited"); it == nil || r.PendingSidecars != 0 {
		t.Errorf("edited file with an extensionless sidecar: %+v", r)
	}
	// a still and its video share the stem of an extensionless sidecar: the
	// still is the original and the video its motion companion, whichever the
	// archive lists first
	for i, order := range [][]file{
		{{gp + "Photos from 2015/Pair - 1.jpg", randBytes(70)}, {gp + "Photos from 2015/Pair - 1.mp4", randBytes(75)}},
		{{gp + "Photos from 2016/Pair - 2.mp4", randBytes(75)}, {gp + "Photos from 2016/Pair - 2.jpg", randBytes(70)}},
	} {
		exp := fmt.Sprintf("2027010%dT000000Z-1", i+1)
		gid, year := fmt.Sprintf("PPP%d", i), 2015+i
		r = f.ingest("alice", f.zip("takeout-"+exp+"-001.zip", append(order,
			file{fmt.Sprintf("%sPhotos from %d/Pair - %d.json", gp, year, i+1), sidecar(fmt.Sprintf("Pair - %d", i+1), gid, tJuly, "")})), Options{})
		still, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#original")
		video, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#motion")
		if still == nil || still.Ext != ".jpg" || video == nil || video.Ext != ".mp4" || r.PendingSidecars != 0 {
			t.Errorf("order %d: still=%+v video=%+v %+v", i, still, video, r)
		}
	}
	// a lone video is the original
	r = f.ingest("alice", f.zip("takeout-20270301T000000Z-1-001.zip", []file{
		{gp + "Photos from 2017/Clip - 1.mp4", randBytes(60)},
		{gp + "Photos from 2017/Clip - 1.json", sidecar("Clip - 1", "VVV", tJuly, "")},
	}), Options{})
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "VVV#original"); it == nil || it.Ext != ".mp4" || r.PendingSidecars != 0 {
		t.Errorf("lone video with a title-only sidecar: %+v", r)
	}
	// a still with its video in a part before their title-only sidecar (the
	// pending resolution) and in a part after it (the pending lookup)
	for i, order := range [][]file{
		{{gp + "Photos from 2020/Late - 1.mp4", randBytes(66)}, {gp + "Photos from 2020/Late - 1.jpg", randBytes(65)}},
		{{gp + "Photos from 2021/Late - 2.jpg", randBytes(65)}, {gp + "Photos from 2021/Late - 2.mp4", randBytes(66)}},
	} {
		exp := fmt.Sprintf("2027060%dT000000Z-1", i+1)
		gid, year := fmt.Sprintf("LLL%d", i), 2020+i
		sc := file{fmt.Sprintf("%sPhotos from %d/Late - %d.json", gp, year, i+1), sidecar(fmt.Sprintf("Late - %d", i+1), gid, tJuly, "")}
		parts := [][]file{order, {sc}}
		if i == 1 {
			parts = [][]file{{sc}, order}
		}
		for n, files := range parts {
			r = f.ingest("alice", f.zip(fmt.Sprintf("takeout-%s-00%d.zip", exp, n+1), files), Options{})
		}
		still, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#original")
		video, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#motion")
		if still == nil || still.Ext != ".jpg" || video == nil || video.Ext != ".mp4" {
			t.Errorf("late pair %d: still=%+v video=%+v %+v", i, still, video, r)
		}
		if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
			t.Errorf("late pair %d: pending %d", i, n)
		}
	}
	// a video arriving before its still, with the sidecar in a part before
	// the video or between the two: the lone video is the original until the
	// still arrives, then its motion companion
	for i, parts := range [][][]file{
		{{{gp + "Photos from 2023/Solo - 1.json", sidecar("Solo - 1", "SSS0", tJuly, "")}},
			{{gp + "Photos from 2023/Solo - 1.mp4", randBytes(66)}},
			{{gp + "Photos from 2023/Solo - 1.jpg", randBytes(65)}}},
		{{{gp + "Photos from 2024/Solo - 2.json", sidecar("Solo - 2", "SSS1", tJuly, "")}},
			{{gp + "Photos from 2024/Solo - 2.mp4", randBytes(66)}, {gp + "Photos from 2024/Other.jpg", randBytes(64)}},
			{{gp + "Photos from 2024/Solo - 2.jpg", randBytes(65)}}},
		{{{gp + "Photos from 2025/Solo - 3.mp4", randBytes(66)}},
			{{gp + "Photos from 2025/Solo - 3.json", sidecar("Solo - 3", "SSS2", tJuly, "")}},
			{{gp + "Photos from 2025/Solo - 3.jpg", randBytes(65)}}},
	} {
		exp := fmt.Sprintf("2027070%dT000000Z-1", i+1)
		gid := fmt.Sprintf("SSS%d", i)
		for n, files := range parts {
			r = f.ingest("alice", f.zip(fmt.Sprintf("takeout-%s-00%d.zip", exp, n+1), files), Options{})
			if n == 1 {
				if v, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#original"); v == nil || v.Ext != ".mp4" {
					t.Errorf("lone video %d not the original before its still: %+v", i, v)
				}
			}
		}
		still, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#original")
		video, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#motion")
		if still == nil || still.Ext != ".jpg" || video == nil || video.Ext != ".mp4" || video.GroupID != gid+"#original" {
			t.Errorf("video before still %d: still=%+v video=%+v %+v", i, still, video, r)
		}
		if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
			t.Errorf("video before still %d: pending %d", i, n)
		}
	}
	// the companion in the part after the one holding the file and its sidecar
	for i, parts := range [][][]file{
		{{{gp + "Photos from 2027/Pair - 7.mp4", randBytes(66)}, {gp + "Photos from 2027/Pair - 7.json", sidecar("Pair - 7", "PP7", tJuly, "")}},
			{{gp + "Photos from 2027/Pair - 7.jpg", randBytes(65)}}},
		{{{gp + "Photos from 2028/Pair - 8.jpg", randBytes(65)}, {gp + "Photos from 2028/Pair - 8.json", sidecar("Pair - 8", "PP8", tJuly, "")}},
			{{gp + "Photos from 2028/Pair - 8.mp4", randBytes(66)}}},
	} {
		exp := fmt.Sprintf("2027090%dT000000Z-1", i+1)
		gid := fmt.Sprintf("PP%d", 7+i)
		for n, files := range parts {
			r = f.ingest("alice", f.zip(fmt.Sprintf("takeout-%s-00%d.zip", exp, n+1), files), Options{})
		}
		still, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#original")
		video, _ := f.st.FindItemByKey(f.ctx, acct.ID, gid+"#motion")
		if still == nil || still.Ext != ".jpg" || video == nil || video.Ext != ".mp4" || video.GroupID != gid+"#original" {
			t.Errorf("companion in the next part %d: still=%+v video=%+v %+v", i, still, video, r)
		}
		if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
			t.Errorf("companion in the next part %d: pending %d", i, n)
		}
	}
	// and a video arriving after its still, with the sidecar between the two
	f.ingest("alice", f.zip("takeout-20270801T000000Z-1-001.zip", []file{{gp + "Photos from 2026/Late - 9.jpg", randBytes(65)}}), Options{})
	f.ingest("alice", f.zip("takeout-20270801T000000Z-1-002.zip", []file{{gp + "Photos from 2026/Late - 9.json", sidecar("Late - 9", "LL9", tJuly, "")}}), Options{})
	r = f.ingest("alice", f.zip("takeout-20270801T000000Z-1-003.zip", []file{{gp + "Photos from 2026/Late - 9.mp4", randBytes(66)}}), Options{})
	if video, _ := f.st.FindItemByKey(f.ctx, acct.ID, "LL9#motion"); video == nil || video.Ext != ".mp4" || r.Added != 1 {
		t.Errorf("video after its still and sidecar: video=%+v %+v", video, r)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
	// two stills sharing the stem: ambiguous, the sidecar waits
	r = f.ingest("alice", f.zip("takeout-20270401T000000Z-1-001.zip", []file{
		{gp + "Photos from 2018/Dup - 1.jpg", randBytes(61)},
		{gp + "Photos from 2018/Dup - 1.png", randBytes(62)},
		{gp + "Photos from 2018/Dup - 1.json", sidecar("Dup - 1", "DDD", tJuly, "")},
	}), Options{})
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "DDD#original"); it != nil || r.PendingSidecars != 1 || r.Added != 2 {
		t.Errorf("ambiguous title-only sidecar was applied: %+v", r)
	}
	// the same two stills with the sidecar arriving first: the pending path is as careful
	f.ingest("alice", f.zip("takeout-20270501T000000Z-1-001.zip", []file{
		{gp + "Photos from 2019/Dup - 2.json", sidecar("Dup - 2", "DD2", tJuly, "")},
	}), Options{})
	r = f.ingest("alice", f.zip("takeout-20270501T000000Z-1-002.zip", []file{
		{gp + "Photos from 2019/Dup - 2.jpg", randBytes(63)},
		{gp + "Photos from 2019/Dup - 2.png", randBytes(64)},
	}), Options{})
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "DD2#original"); it != nil || r.Promoted != 0 {
		t.Errorf("ambiguous pending title-only sidecar was applied: %+v", r)
	}
}

// A folder name reused by a later export for a different album: its
// sidecar-only entries go to the album that export attached the folder to.
func TestSidecarOnlyEntriesFollowTheExportsAlbum(t *testing.T) {
	f := newFixture(t)
	a, b := randBytes(100), randBytes(110)
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Trip/metadata.json", []byte(`{"title": "Trip 2022", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip/A.jpg", a},
		{gp + "Trip/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	// a later export: the same folder name now is another album (other title and
	// date) holding only B's sidecar
	r := f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Trip/metadata.json", []byte(`{"title": "Trip 2023", "access": "protected", "date": {"timestamp": "1694627246"}}`)},
		{gp + "Trip/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	if r.SidecarLinks != 1 {
		t.Fatalf("later export: %+v", r)
	}
	f.mustExist("alice/albums/Trip 2022/A.jpg", "alice/albums/Trip 2023/B.jpg")
	if f.exists("alice/albums/Trip 2022/B.jpg") {
		t.Error("sidecar-only entry linked into the older album of the same folder name")
	}
	// a later part of that export has media in the folder but no metadata: the
	// recorded album still wins, for its media and for later sidecar-only entries
	c := randBytes(120)
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-002.zip", []file{
		{gp + "Trip/C.jpg", c},
		{gp + "Trip/C.jpg.supplemental-metadata.json", sidecar("C.jpg", "CCC", tJuly, "")},
	}), Options{})
	f.ingest("alice", f.zip("takeout-20261106T000000Z-1-003.zip", []file{
		{gp + "Photos from 2022/D.jpg", randBytes(130)},
		{gp + "Photos from 2022/D.jpg.supplemental-metadata.json", sidecar("D.jpg", "DDD", tJuly, "")},
		{gp + "Trip/D.jpg.supplemental-metadata.json", sidecar("D.jpg", "DDD", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip 2023/C.jpg", "alice/albums/Trip 2023/D.jpg")
	if f.exists("alice/albums/Trip 2022/C.jpg") || f.exists("alice/albums/Trip 2022/D.jpg") {
		t.Error("a part without metadata re-attached the folder to the older album")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// Sidecars in a folder whose media is excluded by configuration are not
// stored as pending.
func TestExcludedFolderSidecarsNotPending(t *testing.T) {
	f := newFixture(t)
	r := f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Trash/T.jpg", randBytes(100)},
		{gp + "Trash/T.jpg.supplemental-metadata.json", sidecar("T.jpg", "TTT", tJuly, "")},
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	if r.PendingSidecars != 0 || r.Added != 1 {
		t.Fatalf("trash sidecar stored as pending: %+v", r)
	}
}

// An album folder holding only a title-named sidecar (A.json) of a photo that
// lives in a year folder is a membership like any other sidecar-only entry.
func TestTitleOnlySidecarAloneInAlbumLinks(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	r := f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", a},
		{gp + "Photos from 2022/A.json", sidecar("A", "AAA", tJuly, "")},
		{gp + "Trip /metadata.json", []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip /A.json", sidecar("A", "AAA", tJuly, "")},
	}), Options{})
	if r.SidecarLinks != 1 {
		t.Fatalf("title-only album entry: %+v", r)
	}
	f.mustExist("alice/albums/Trip to Austin/A.jpg")
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("resolved album entry still counted as waiting: %d", n)
	}
}

// A title-named sidecar stored from an earlier part is handed to one later
// file only: a second, unrelated still arriving in yet another part is left
// alone.
func TestTitleOnlySidecarNotReusedAcrossParts(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{{gp + "Photos from 2022/X.json", sidecar("X", "XXX", tJuly, "")}}), Options{})
	f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Photos from 2022/X.jpg", randBytes(70)}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original"); it == nil || it.Ext != ".jpg" {
		t.Fatalf("first file not matched: %+v", it)
	}
	r := f.ingest("alice", f.zip("takeout-"+export+"-003.zip", []file{{gp + "Photos from 2022/X.png", randBytes(71)}}), Options{})
	if r.Added != 1 {
		t.Fatalf("third part: %+v", r)
	}
	if alt, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#alt:png"); alt != nil {
		t.Error("the unrelated still took the earlier file's Google id")
	}
	if fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Photos from 2022#X.png"); fb == nil {
		t.Error("the unrelated still is not a fallback item")
	}
}

// A title may contain dots: X.Y.json beside X.Y.jpg describes that photo.
func TestTitleWithDots(t *testing.T) {
	f := newFixture(t)
	r := f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.Y.jpg", randBytes(70)},
		{gp + "Photos from 2022/X.Y.json", sidecar("X.Y", "XY1", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XY1#original"); it == nil || r.PendingSidecars != 0 {
		t.Errorf("dotted title not matched: %+v", r)
	}
}

// A reused folder name whose first part of a new export has no metadata is
// guessed to the older album; the metadata in a later part moves the
// folder's media to the album it names.
func TestLaterMetadataMovesFolderToItsAlbum(t *testing.T) {
	f := newFixture(t)
	a, c := randBytes(100), randBytes(120)
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Trip/metadata.json", []byte(`{"title": "Trip 2022", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip/A.jpg", a},
		{gp + "Trip/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Trip/C.jpg", c},
		{gp + "Trip/C.jpg.supplemental-metadata.json", sidecar("C.jpg", "CCC", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip 2022/C.jpg") // the guess by folder name
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Trip/metadata.json", []byte(`{"title": "Trip 2023", "access": "protected", "date": {"timestamp": "1694627246"}}`)},
		{gp + "Trip/D.jpg", randBytes(130)},
		{gp + "Trip/D.jpg.supplemental-metadata.json", sidecar("D.jpg", "DDD", tJuly, "")},
	}), Options{})
	if r.Status != store.ArchiveIngested {
		t.Fatalf("part with metadata: %+v", r)
	}
	f.mustExist("alice/albums/Trip 2023/C.jpg", "alice/albums/Trip 2023/D.jpg", "alice/albums/Trip 2022/A.jpg")
	if f.exists("alice/albums/Trip 2022/C.jpg") {
		t.Error("the guessed membership in the older album survived")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// Two unrelated stills ingested earlier share the stem of a title-named
// sidecar arriving now: ambiguous, applied to neither, and still counted as
// waiting.
func TestLateTitleSidecarAmbiguousAcrossParts(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", randBytes(70)},
		{gp + "Photos from 2022/X.png", randBytes(71)},
	}), Options{})
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Photos from 2022/X.json", sidecar("X", "XXX", tJuly, "")}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if r.Promoted != 0 {
		t.Fatalf("ambiguous title sidecar applied: %+v", r)
	}
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original"); it != nil {
		t.Error("one of two unrelated stills took the sidecar's identity")
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 1 {
		t.Errorf("ambiguous sidecar not counted as waiting: %d", n)
	}
}

// A sidecar-only album folder whose first part of a new export has no
// metadata is provisionally attached to the album of that name; metadata in
// a later part naming another album moves the membership.
func TestProvisionalSidecarOnlyFolderMovesWithMetadata(t *testing.T) {
	f := newFixture(t)
	a, b := randBytes(100), randBytes(110)
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Trip/metadata.json", []byte(`{"title": "Trip 2022", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip/A.jpg", a},
		{gp + "Trip/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Trip/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	if r.SidecarLinks != 1 {
		t.Fatalf("provisional membership: %+v", r)
	}
	f.mustExist("alice/albums/Trip 2022/B.jpg")
	r = f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Trip/metadata.json", []byte(`{"title": "Trip 2023", "access": "protected", "date": {"timestamp": "1694627246"}}`)},
	}), Options{})
	if r.Status != store.ArchiveIngested {
		t.Fatalf("metadata part: %+v", r)
	}
	f.mustExist("alice/albums/Trip 2023/B.jpg", "alice/albums/Trip 2022/A.jpg")
	if f.exists("alice/albums/Trip 2022/B.jpg") {
		t.Error("provisional membership stayed in the album guessed by name")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// An album occurrence whose link was lost (a failed step after the occurrence
// was written) gets its link back at the next reconciliation.
func TestReconcileRecreatesMissingAlbumLinks(t *testing.T) {
	f := newFixture(t)
	a := randBytes(100)
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Trip/metadata.json", []byte(`{"title": "Trip 2022", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip/A.jpg", a},
		{gp + "Trip/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original")
	albums, _ := f.st.ListAlbums(f.ctx, acct.ID)
	if err := f.st.DeleteAlbumItem(f.ctx, albums[0].ID, it.ID); err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(f.cfg.DataDir, "alice/albums/Trip 2022/A.jpg"))
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-002.zip", []file{{gp + "Photos from 2022/Z.jpg", randBytes(90)}}), Options{})
	f.mustExist("alice/albums/Trip 2022/A.jpg")
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A title-named album entry for photo B whose stem collides with an unrelated
// X.jpg already in that album still links B into the album.
func TestTitleAlbumEntryCollidingWithOtherFile(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /X.jpg", randBytes(70)},
		{gp + "Trip /X.jpg.supplemental-metadata.json", sidecar("X.jpg", "AAA", tJuly, "")},
		{gp + "Photos from 2022/B.jpg", randBytes(80)},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Trip /X.json", sidecar("X", "BBB", tJuly, "")}, // Google id of B, title X
	}), Options{})
	if r.SidecarLinks != 1 || r.Promoted != 0 {
		t.Fatalf("colliding title entry: %+v", r)
	}
	f.mustExist("alice/albums/Trip to Austin/X.jpg", "alice/albums/Trip to Austin/B.jpg")
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if x, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#original"); x == nil {
		t.Error("the unrelated file lost its identity")
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("pending after linking: %d", n)
	}
}

// A title-named sidecar in an album names a known photo B; a nameless album
// file X.jpg sharing the title's stem is not that photo (other bytes): it
// stays a fallback item, B keeps its bytes and is linked into the album.
func TestTitleSidecarNamingAnotherPhotoIsNotApplied(t *testing.T) {
	for _, late := range []bool{false, true} {
		f := newFixture(t)
		const export = "20261006T035113Z-1"
		meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
		b, x := randBytes(80), randBytes(70)
		part1 := []file{
			{gp + "Photos from 2022/B.jpg", b},
			{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
			{gp + "Trip /metadata.json", meta},
			{gp + "Trip /X.jpg", x},
		}
		sc := file{gp + "Trip /X.json", sidecar("X", "BBB", tJuly, "")}
		if !late {
			part1 = append(part1, sc)
		}
		f.ingest("alice", f.zip("takeout-"+export+"-001.zip", part1), Options{})
		if late {
			f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{sc}), Options{})
		}
		acct, _ := f.st.GetAccount(f.ctx, "alice")
		bb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original")
		if bb == nil || !bytes.Equal(f.read(bb.LibPath), b) {
			t.Fatalf("late=%v: B lost its bytes or identity: %+v", late, bb)
		}
		if fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Trip #X.jpg"); fb == nil || !bytes.Equal(f.read(fb.LibPath), x) {
			t.Errorf("late=%v: the nameless file was re-identified: %+v", late, fb)
		}
		f.mustExist("alice/albums/Trip to Austin/B.jpg", "alice/albums/Trip to Austin/X.jpg")
		if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 2 {
			t.Errorf("late=%v: items %d", late, c["items"])
		}
		if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
			t.Errorf("late=%v: verify: %v %v", late, err, v.Problems)
		}
	}
}

// An exact sidecar for a media extension outside any list (X.cr3) is not a
// title match: a later export with other bytes supersedes as usual.
func TestExactSidecarOfUnlistedExtensionSupersedes(t *testing.T) {
	f := newFixture(t)
	f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.cr3", randBytes(70)},
		{gp + "Photos from 2022/X.cr3.supplemental-metadata.json", sidecar("X.cr3", "RRR", tJuly, "")},
	}), Options{})
	r := f.ingest("alice", f.zip("takeout-20261106T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.cr3", randBytes(72)},
		{gp + "Photos from 2022/X.cr3.supplemental-metadata.json", sidecar("X.cr3", "RRR", tJuly, "")},
	}), Options{})
	if r.Changed != 1 || r.Added != 0 {
		t.Fatalf("changed bytes of an unlisted extension: %+v", r)
	}
}

// A stored sidecar already consumed as an album-only membership of photo B
// does not hand B's identity to a later file of the same name in that album
// folder whose bytes are not B's.
func TestSettledAlbumSidecarNotReusedForOtherBytes(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	b := randBytes(80)
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip to Austin/B.jpg")
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Trip /B.jpg", randBytes(81)}}), Options{})
	if r.Added != 1 || r.Changed != 0 {
		t.Fatalf("other bytes under the settled name: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	bb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original")
	if bb == nil || !bytes.Equal(f.read(bb.LibPath), b) {
		t.Fatalf("B's bytes replaced: %+v", bb)
	}
	if fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Trip #B.jpg"); fb == nil {
		t.Error("the other file is not a fallback item")
	}
	// B was delivered elsewhere in this export: the album file stays an
	// unrelated file even once the export is complete
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, true); err != nil || promoted != 0 {
		t.Errorf("album file promoted at completion: %d %v", promoted, err)
	}
	if bb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original"); !bytes.Equal(f.read(bb.LibPath), b) {
		t.Error("B's bytes replaced at completion")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A sidecar consumed as an album membership of a photo known from an earlier
// export, followed by a same-named file with other bytes in a later part: the
// row is reopened, and with the export complete without the photo elsewhere
// the file is the photo's new version (the membership follows).
func TestSettledSidecarReopenedForNewVersion(t *testing.T) {
	f := newFixture(t)
	b1, b2 := randBytes(80), randBytes(82)
	f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/B.jpg", b1},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip to Austin/B.jpg")
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Trip /B.jpg", b2}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	bb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original")
	if r.Added != 1 || r.Changed != 0 || !bytes.Equal(f.read(bb.LibPath), b1) {
		t.Fatalf("decided before the export was complete: %+v", r)
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 1 {
		t.Fatalf("deferred row not reopened: pending %d", n)
	}
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, true); err != nil || promoted != 1 {
		t.Fatalf("resolve at completion: %d %v", promoted, err)
	}
	bb, _ = f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original")
	if bb == nil || !bytes.Equal(f.read(bb.LibPath), b2) {
		t.Error("new bytes not current")
	}
	if !bytes.Equal(f.read("alice/albums/Trip to Austin/B.jpg"), b2) {
		t.Error("album link does not follow the new bytes")
	}
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 1 || c["pending_sidecars"] != 0 {
		t.Errorf("items %d pending %d", c["items"], c["pending_sidecars"])
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A truncated-name prefix match of the index (the sidecar's title need not
// equal the media name) deferred for a collision is decided at completion
// from the recorded entry.
func TestTruncatedPrefixDeferralResolvesAtCompletion(t *testing.T) {
	f := newFixture(t)
	x1, x2 := randBytes(70), randBytes(72)
	const name = "LongNameTruncatedByTakeout.jpg"
	f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/" + name, x1},
		{gp + "Photos from 2022/" + name + ".supplemental-metadata.json", sidecar(name, "XXX", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/" + name, x2},
		{gp + "Photos from 2022/LongNameTruncatedByTak.json", sidecar("Some other title", "XXX", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if r.Changed != 0 || r.Added != 1 || !bytes.Equal(f.read(it.LibPath), x1) {
		t.Fatalf("decided before the export was complete: %+v", r)
	}
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, false); err != nil || promoted != 0 {
		t.Fatalf("resolved while incomplete: %d %v", promoted, err)
	}
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, true); err != nil || promoted != 1 {
		t.Fatalf("resolve at completion: %d %v", promoted, err)
	}
	it, _ = f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if it == nil || !bytes.Equal(f.read(it.LibPath), x2) {
		t.Error("new bytes not current")
	}
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 1 || c["pending_sidecars"] != 0 {
		t.Errorf("items %d pending %d", c["items"], c["pending_sidecars"])
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A stored album sidecar consumed by media of a later part: when that part is
// replaced without the media, the sidecar is reopened and, the photo being
// known, makes the membership again.
func TestReplacedPartReopensConsumedSidecar(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	b := randBytes(80)
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /B.json", sidecar("B", "BBB", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 1 {
		t.Fatalf("pending: %d", n)
	}
	f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Trip /B.jpg", b}}), Options{})
	f.mustExist("alice/albums/Trip to Austin/B.jpg")
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Fatalf("consumed sidecar still pending: %d", n)
	}
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Trip /C.jpg", randBytes(70)}}), Options{Replace: true})
	if !r.Replaced {
		t.Fatalf("replacement: %+v", r)
	}
	// the photo is still known: the album sidecar of part 1 makes the membership again
	f.mustExist("alice/albums/Trip to Austin/B.jpg")
	f.ingest("alice", f.zip("takeout-"+export+"-003.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip to Austin/B.jpg", "alice/library/2022/07/B.jpg")
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("pending after resolution: %d", n)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// Stored sidecars of an album whose photos only get their identity in the
// same resolution (title-named sidecars of a year folder applied to fallback
// items, as after an upgrade) are linked in that same call, whatever the
// folder order.
func TestResolvePendingLinksPhotosPromotedInTheSameCall(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", randBytes(70)},
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /Z.jpg", randBytes(60)},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	raw := sidecar("X", "XXX", tJuly, "")
	for _, folder := range []string{"Photos from 2022", "Trip "} {
		if err := f.st.PutPendingSidecar(f.ctx, acct.ID, export, 1, folder, "X", raw, "XXX"); err != nil {
			t.Fatal(err)
		}
	}
	links, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, false)
	if err != nil || promoted != 1 || links != 1 {
		t.Fatalf("one call: links %d promoted %d err %v", links, promoted, err)
	}
	f.mustExist("alice/albums/Trip to Austin/X.jpg")
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("pending: %d", n)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// Stored sidecars of an excluded folder (rows from before the exclusion
// covered sidecars) are dropped: nothing waits for them.
func TestExcludedFolderPendingRowsDropped(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{{gp + "Photos from 2022/A.jpg", randBytes(70)}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if err := f.st.PutPendingSidecar(f.ctx, acct.ID, export, 1, "Trash", "T.jpg", sidecar("T.jpg", "TTT", tJuly, ""), "TTT"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.ing.ResolvePending(f.ctx, acct, export, false); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("pending: %d", n)
	}
	// a folder ingested while it was included keeps its rows
	f.ing.cfg.Takeout.IncludeTrash = true
	f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{{gp + "Trash/U.jpg", randBytes(71)}}), Options{})
	f.ing.cfg.Takeout.IncludeTrash = false
	if err := f.st.PutPendingSidecar(f.ctx, acct.ID, export, 1, "Trash", "V.jpg", sidecar("V.jpg", "VVV", tJuly, ""), "VVV"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.ing.ResolvePending(f.ctx, acct, export, false); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 1 {
		t.Errorf("row of an ingested folder dropped: pending %d", n)
	}
}

// A year-folder sidecar of a photo whose file the export delivered in an
// album folder only is consumed, not waited for.
func TestYearFolderSidecarOfPhotoDeliveredInAlbumResolved(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /X.jpg", randBytes(70)},
		{gp + "Trip /X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, "")},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 || r.Added != 1 {
		t.Errorf("pending %d: %+v", n, r)
	}
	f.mustExist("alice/albums/Trip to Austin/X.jpg")
}

// A stored sidecar whose source part is unknown (rows from before part
// tracking) never makes an album membership: it could not be withdrawn with
// its part.
func TestUnknownPartPendingRowMakesNoMembership(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/A.jpg", randBytes(100)},
		{gp + "Photos from 2022/A.jpg.supplemental-metadata.json", sidecar("A.jpg", "AAA", tJuly, "")},
		{gp + "Trip /metadata.json", []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip /Z.jpg", randBytes(90)},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if err := f.st.PutPendingSidecar(f.ctx, acct.ID, export, 0, "Trip ", "A.jpg", sidecar("A.jpg", "AAA", tJuly, ""), "AAA"); err != nil {
		t.Fatal(err)
	}
	links, _, err := f.ing.ResolvePending(f.ctx, acct, export, true)
	if err != nil || links != 0 {
		t.Fatalf("unknown-part row made a membership: %d %v", links, err)
	}
	if f.exists("alice/albums/Trip to Austin/A.jpg") {
		t.Error("link made from a row of unknown part")
	}
}

// Name-only matches of the index (a sidecar whose title equals the media
// name, a truncated-name prefix) obey the same guard as title-named ones: a
// known photo's identity never lands on a file with other bytes.
func TestHeuristicMatchesGuardKnownPhotos(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	b := randBytes(80)
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	// an album folder with a file named B.jpg (other bytes) and a sidecar whose
	// title is B.jpg but whose file name differs (matched by title only)
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Trip /metadata.json", []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)},
		{gp + "Trip /B.jpg", randBytes(81)},
		{gp + "Trip /B-other.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	bb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original")
	if bb == nil || !bytes.Equal(f.read(bb.LibPath), b) {
		t.Fatalf("B's bytes replaced through a title match: %+v %+v", bb, r)
	}
	if fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Trip #B.jpg"); fb == nil {
		t.Error("the other file is not a fallback item")
	}
	f.mustExist("alice/albums/Trip to Austin/B.jpg") // B's membership from the kept sidecar
}

// A sidecar-only membership of photo B survives an unrelated file named B.jpg
// arriving in that album folder later; and a sidecar naming B that arrives
// after an album file of that name (with its own, other sidecar) is still
// kept for B's membership.
func TestSidecarOnlyMembershipAndSameNamedFileCoexist(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	b, c := randBytes(80), randBytes(85)
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Photos from 2022/C.jpg", c},
		{gp + "Photos from 2022/C.jpg.supplemental-metadata.json", sidecar("C.jpg", "CCC", tJuly, "")},
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Trip /C.jpg", randBytes(86)}, // another photo that happens to be named C.jpg
		{gp + "Trip /C.jpg.supplemental-metadata.json", sidecar("C.jpg", "CC2", tJuly, "")},
	}), Options{})
	f.mustExist("alice/albums/Trip to Austin/B.jpg") // the other C.jpg links under a collision-suffixed name
	// an unrelated B.jpg lands in the album folder, and a sidecar naming C
	// arrives although the folder already holds another photo of that name
	r := f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Trip /B.jpg", randBytes(81)},
		{gp + "Trip /C.jpg.supplemental-metadata(1).json", sidecar("C.jpg", "CCC", tJuly, "")},
	}), Options{})
	if r.Status != store.ArchiveIngested {
		t.Fatalf("part 2: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	for _, key := range []string{"BBB#original", "CCC#original", "CC2#original"} {
		it, _ := f.st.FindItemByKey(f.ctx, acct.ID, key)
		if it == nil {
			t.Fatalf("%s lost", key)
		}
		ais, _ := f.st.AlbumItemsByItem(f.ctx, it.ID)
		if len(ais) != 1 || !ais[0].Direct {
			t.Errorf("%s membership: %+v", key, ais)
		}
	}
	if bb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original"); !bytes.Equal(f.read(bb.LibPath), b) {
		t.Error("B's bytes replaced")
	}
	if fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Trip #B.jpg"); fb == nil {
		t.Error("the unrelated B.jpg is not a fallback item")
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("pending: %d", n)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// A later export delivering a photo's new bytes with a title-named sidecar
// (its only delivery of that photo) is a new version, not a name collision.
func TestTitleSidecarNewVersionSupersedes(t *testing.T) {
	f := newFixture(t)
	x1, x2 := randBytes(70), randBytes(72)
	f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x1},
		{gp + "Photos from 2022/X.json", sidecar("X", "XXX", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x2},
		{gp + "Photos from 2022/X.json", sidecar("X", "XXX", tJuly, "")},
	}), Options{})
	// undecided while the export may still deliver the photo elsewhere
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if r.Changed != 0 || r.Added != 1 || !bytes.Equal(f.read(it.LibPath), x1) {
		t.Fatalf("decided before the export was complete: %+v", r)
	}
	// the export is complete without another delivery: the new version
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, true); err != nil || promoted != 1 {
		t.Fatalf("resolve at completion: %d %v", promoted, err)
	}
	it, _ = f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if it == nil || !bytes.Equal(f.read(it.LibPath), x2) {
		t.Error("new bytes not current")
	}
	f.mustExist("alice/library/2022/07/X~v" + sha8(x1) + ".jpg")
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 1 {
		t.Errorf("items: %d", c["items"])
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// The same through the title-equality heuristic (a sidecar whose name does
// not match the file but whose title does): deferred, then applied at
// completion.
func TestTitleEqualSidecarNewVersionWaitsForCompletion(t *testing.T) {
	f := newFixture(t)
	x1, x2 := randBytes(70), randBytes(72)
	f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x1},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata.json", sidecar("X.jpg", "XXX", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x2},
		{gp + "Photos from 2022/X.jpg.supplemental-metadata(1).json", sidecar("X.jpg", "XXX", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if r.Changed != 0 || r.Added != 1 || !bytes.Equal(f.read(it.LibPath), x1) {
		t.Fatalf("decided before the export was complete: %+v", r)
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 1 {
		t.Fatalf("deferred sidecar not stored: %d", n)
	}
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, false); err != nil || promoted != 0 {
		t.Fatalf("resolved while incomplete: %d %v", promoted, err)
	}
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, true); err != nil || promoted != 1 {
		t.Fatalf("resolve at completion: %d %v", promoted, err)
	}
	it, _ = f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if it == nil || !bytes.Equal(f.read(it.LibPath), x2) {
		t.Error("new bytes not current")
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 0 {
		t.Errorf("pending after resolution: %d", n)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// ExportComplete: every seen part ingested without a gap, and the confirmed
// count reached or a quiet period elapsed.
func TestExportComplete(t *testing.T) {
	f := newFixture(t)
	const export = "20261106T000000Z-1"
	part := func(n int) {
		f.ingest("alice", f.zip(fmt.Sprintf("takeout-%s-00%d.zip", export, n), []file{
			{fmt.Sprintf("%sPhotos from 2022/P%d.jpg", gp, n), randBytes(50 + n)},
		}), Options{})
	}
	part(1)
	part(3)
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	far := time.Now().Add(30 * 24 * time.Hour)
	check := func(want bool, quiet time.Duration, now time.Time, what string) {
		t.Helper()
		if got, err := f.ing.ExportComplete(f.ctx, acct.ID, export, quiet, now); err != nil || got != want {
			t.Errorf("%s: complete=%v err=%v", what, got, err)
		}
	}
	check(false, 0, time.Now(), "gap, unconfirmed")
	check(false, time.Hour, far, "gap, quiet")
	part(2)
	check(false, 0, time.Now(), "unconfirmed, no quiet period")
	check(false, time.Hour, time.Now(), "unconfirmed, too soon")
	check(true, time.Hour, far, "unconfirmed, quiet")
	if err := f.st.SetExportConfirmedParts(f.ctx, acct.ID, export, 4, false); err != nil {
		t.Fatal(err)
	}
	check(false, time.Hour, far, "a confirmed part missing")
	if err := f.st.SetExportConfirmedParts(f.ctx, acct.ID, export, 3, false); err != nil {
		t.Fatal(err)
	}
	check(true, 0, time.Now(), "confirmed and ingested")
}

// A deferred stem match is judged again at resolution: a second still
// sharing the stem that arrived since makes it ambiguous, so the sidecar
// stays pending even once the export is complete.
func TestDeferredStemEntryRevalidated(t *testing.T) {
	f := newFixture(t)
	x1 := randBytes(70)
	f.ingest("alice", f.zip("takeout-20261006T035113Z-1-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", x1},
		{gp + "Photos from 2022/X.json", sidecar("X", "XXX", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/X.jpg", randBytes(72)},
		{gp + "Photos from 2022/X.json", sidecar("X", "XXX", tJuly, "")},
	}), Options{})
	f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Photos from 2022/X.png", randBytes(73)},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, true); err != nil || promoted != 0 {
		t.Fatalf("ambiguous deferred entry applied: %d %v", promoted, err)
	}
	it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "XXX#original")
	if it == nil || !bytes.Equal(f.read(it.LibPath), x1) {
		t.Error("known photo's bytes replaced")
	}
	if n, _ := f.st.CountPendingSidecars(f.ctx, acct.ID); n != 1 {
		t.Errorf("pending: %d", n)
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// An album part ingested before the year-folder part of the photo its
// title-named entry names: the unrelated album file is not re-identified at
// that time, and when the photo arrives elsewhere the entry becomes its
// membership.
func TestAlbumPartBeforePhotoPartDefersCollision(t *testing.T) {
	f := newFixture(t)
	b, x := randBytes(80), randBytes(70)
	f.ingest("alice", f.zip("takeout-20261006T000000Z-1-001.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	const export = "20261106T000000Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /X.jpg", x},
		{gp + "Trip /X.json", sidecar("X", "BBB", tJuly, "")},
	}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	bb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#original")
	if r.Changed != 0 || !bytes.Equal(f.read(bb.LibPath), b) {
		t.Fatalf("known photo superseded before the export was seen: %+v", r)
	}
	if f.exists("alice/albums/Trip to Austin/B.jpg") {
		t.Fatal("membership made before the photo was seen in this export")
	}
	r = f.ingest("alice", f.zip("takeout-"+export+"-002.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
	}), Options{})
	if r.SidecarLinks != 1 {
		t.Fatalf("membership after the photo arrived elsewhere: %+v", r)
	}
	f.mustExist("alice/albums/Trip to Austin/B.jpg", "alice/albums/Trip to Austin/X.jpg")
	if fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Trip #X.jpg"); fb == nil || !bytes.Equal(f.read(fb.LibPath), x) {
		t.Error("the unrelated file was re-identified")
	}
	if v, err := f.ing.Verify(f.ctx, "alice", true); err != nil || len(v.Problems) > 0 {
		t.Errorf("verify: %v %v", err, v.Problems)
	}
}

// An unrelated still+video pair in an album sharing the stem of a title-named
// entry that names a known photo: neither takes the photo's identity (not
// even as its motion variant), and the photo is linked.
func TestTitleEntryWithUnrelatedPairMakesNoVariant(t *testing.T) {
	f := newFixture(t)
	const export = "20261006T035113Z-1"
	meta := []byte(`{"title": "Trip to Austin", "access": "protected", "date": {"timestamp": "1663091246"}}`)
	b := randBytes(80)
	r := f.ingest("alice", f.zip("takeout-"+export+"-001.zip", []file{
		{gp + "Photos from 2022/B.jpg", b},
		{gp + "Photos from 2022/B.jpg.supplemental-metadata.json", sidecar("B.jpg", "BBB", tJuly, "")},
		{gp + "Trip /metadata.json", meta},
		{gp + "Trip /X.jpg", randBytes(70)},
		{gp + "Trip /X.mp4", randBytes(75)},
		{gp + "Trip /X.json", sidecar("X", "BBB", tJuly, "")},
	}), Options{})
	if r.SidecarLinks != 1 {
		t.Fatalf("named photo not linked: %+v", r)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if v, _ := f.st.FindItemByKey(f.ctx, acct.ID, "BBB#motion"); v != nil {
		t.Error("the unrelated video became the photo's motion variant")
	}
	for _, name := range []string{"X.jpg", "X.mp4"} {
		if fb, _ := f.st.FindItemByKey(f.ctx, acct.ID, "occ:"+export+"#Trip #"+name); fb == nil {
			t.Errorf("%s is not a fallback item", name)
		}
	}
	f.mustExist("alice/albums/Trip to Austin/B.jpg")
}

// A Takeout made by a Russian-language account: with the English names only
// the part is refused as a configuration error and nothing is recorded; with
// the names configured, year folders, albums, the edited copy and Trash are
// all recognized.
func TestLocalizedTakeoutNames(t *testing.T) {
	const ru = "Takeout/Google Фото/"
	f := newFixture(t)
	part := f.zip("takeout-20261010T221633Z-1-001.zip", []file{
		{ru + "Фото 2023 г./20230101_1.jpg", randBytes(90)},
		{ru + "Фото 2023 г./20230101_1.jpg.supplemental-metadata.json", sidecar("20230101_1.jpg", "AAA", tJuly, "")},
		{ru + "Фото 2023 г./20230101_1-отредактировано.jpg", randBytes(91)},
		{ru + "Рим/20230101_2.jpg", randBytes(92)},
		{ru + "Рим/20230101_2.jpg.supplemental-metadata.json", sidecar("20230101_2.jpg", "BBB", tJuly, "")},
		{ru + "Рим/метаданные.json", []byte(`{"title":"Рим","description":"","access":"protected","date":{"timestamp":"1657473817","formatted":"x"}}`)},
		{ru + "Корзина/20230101_3.jpg", randBytes(93)},
		{ru + "Корзина/20230101_3.jpg.supplemental-metadata.json", sidecar("20230101_3.jpg", "TTT", tJuly, "")},
		{"Takeout/archive_browser.html", []byte("<html>")},
	})
	src, err := OpenZip(part)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.ing.IngestPart(f.ctx, src, PartMeta{Account: "alice", ExportID: "20261010T221633Z-1", PartNo: 1, Name: filepath.Base(part)}, Options{})
	src.Close()
	if err == nil || !strings.Contains(err.Error(), "takeout.language") {
		t.Fatalf("English rules: want a configuration error, got %v", err)
	}
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	if archs, _ := f.st.ListArchives(f.ctx, acct.ID); len(archs) > 0 && archs[0].Status == store.ArchiveIngested {
		t.Fatalf("refused part recorded as ingested: %+v", archs[0])
	}
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items"] != 0 {
		t.Fatalf("refused part created items: %v", c)
	}

	f.cfg.Accounts["alice"] = config.Account{Takeout: config.TakeoutOverride{Language: "ru"}}
	r := f.ingest("alice", part, Options{})
	if r.Added != 3 || r.Links != 1 {
		t.Fatalf("configured rules: added %d, links %d: %+v", r.Added, r.Links, r)
	}
	f.mustExist("alice/library/2022/07/20230101_1.jpg", "alice/library/2022/07/20230101_2.jpg", "alice/albums/Рим/20230101_2.jpg")
	if f.exists("alice/library/2022/07/20230101_3.jpg") || f.exists("alice/albums/Корзина") {
		t.Error("Trash was ingested")
	}
	if it, _ := f.st.FindItemByKey(f.ctx, acct.ID, "AAA#edited"); it == nil {
		t.Error("the localized edited copy was not recognized as the edited variant")
	}
	albums, _ := f.st.ListAlbums(f.ctx, acct.ID)
	if len(albums) != 1 || albums[0].Title != "Рим" {
		t.Errorf("albums: %+v", albums)
	}

	// an English-language account is classified with the English names only:
	// its album named like the Russian Trash folder is an album
	jane := f.zip("takeout-20261010T221633Z-2-001.zip", []file{
		{gp + "Корзина/x.jpg", randBytes(94)},
		{gp + "Корзина/x.jpg.supplemental-metadata.json", sidecar("x.jpg", "XXX", tJuly, "")},
	})
	if r := f.ingest("jane", jane, Options{}); r.Added != 1 || r.Links != 1 {
		t.Fatalf("English account: added %d, links %d", r.Added, r.Links)
	}
	f.mustExist("jane/albums/Корзина/x.jpg")
}

// A motion photo delivered as X.jpg plus X.MP4 (Samsung) whose sidecar came
// in an earlier part than the files, the video in an earlier part than the
// still: the video takes the Google id as the motion companion, and the
// still's sidecar then names an id already held by other bytes. The video is
// its companion, not a collision, whichever of the two arrives first.
func TestMotionPairStillAfterVideo(t *testing.T) {
	for _, order := range []string{"video first", "still first"} {
		t.Run(order, func(t *testing.T) {
			f := newFixture(t)
			const dir = gp + "Photos from 2023/"
			f.ingest("alice", f.zip("takeout-20261010T221633Z-1-001.zip", []file{
				{dir + "20230621_145830.jpg.supplemental-metadata.json", sidecar("20230621_145830.jpg", "GID1", tJuly, "")},
			}), Options{})
			video := file{dir + "20230621_145830.MP4", randBytes(70)}
			still := file{dir + "20230621_145830.jpg", randBytes(71)}
			second, third := video, still
			if order == "still first" {
				second, third = still, video
			}
			f.ingest("alice", f.zip("takeout-20261010T221633Z-1-002.zip", []file{second}), Options{})
			r := f.ingest("alice", f.zip("takeout-20261010T221633Z-1-003.zip", []file{third}), Options{})
			acct, _ := f.st.GetAccount(f.ctx, "alice")
			stillItem, _ := f.st.FindItemByKey(f.ctx, acct.ID, "GID1#original")
			motionItem, _ := f.st.FindItemByKey(f.ctx, acct.ID, "GID1#motion")
			if stillItem == nil || motionItem == nil || stillItem.Filename != "20230621_145830.jpg" || motionItem.Filename != "20230621_145830.MP4" {
				t.Fatalf("still %+v, motion %+v (result %+v)", stillItem, motionItem, r)
			}
			if c, _ := f.st.Counts(f.ctx, acct.ID); c["items_fallback"] != 0 || c["pending_sidecars"] != 0 {
				t.Errorf("counts %v", c)
			}
		})
	}
}

// A still that an earlier version of the companion rule left nameless, with
// its sidecar row deferred as a collision with its own motion companion
// (the 2026-10-11 export of a Russian account: 100 such pairs), is promoted by
// the resolver, which re-judges deferred rows with the current rule.
func TestDeferredStillOfMotionPairPromoted(t *testing.T) {
	f := newFixture(t)
	const dir = gp + "Photos from 2023/"
	const export, folder, key = "20261010T221633Z-1", "Photos from 2023", "20230621_145830.jpg"
	raw := sidecar(key, "GID1", tJuly, "")
	f.ingest("alice", f.zip("takeout-20261010T221633Z-1-001.zip", []file{{dir + key + ".supplemental-metadata.json", raw}}), Options{})
	f.ingest("alice", f.zip("takeout-20261010T221633Z-1-002.zip", []file{{dir + "20230621_145830.MP4", randBytes(70)}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	// the still arrives while its sidecar is out of reach: a nameless item
	if err := f.st.DeletePendingSidecar(f.ctx, acct.ID, export, folder, key); err != nil {
		t.Fatal(err)
	}
	f.ingest("alice", f.zip("takeout-20261010T221633Z-1-003.zip", []file{{dir + key, randBytes(71)}}), Options{})
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items_fallback"] != 1 {
		t.Fatalf("setup: counts %v", c)
	}
	// the row as the earlier rule left it: stored, deferred to the still, and
	// settled as an album entry of "the photo delivered elsewhere" (the video)
	if err := f.st.PutPendingSidecar(f.ctx, acct.ID, export, 1, folder, key, raw, "GID1"); err != nil {
		t.Fatal(err)
	}
	if err := f.st.DeferPendingSidecar(f.ctx, acct.ID, export, folder, key, key, takeout.VariantOriginal, key); err != nil {
		t.Fatal(err)
	}
	if err := f.st.MarkPendingResolved(f.ctx, acct.ID, export, folder, key); err != nil {
		t.Fatal(err)
	}
	links, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, false)
	if err != nil || promoted != 1 {
		t.Fatalf("resolve: links %d, promoted %d, err %v", links, promoted, err)
	}
	still, _ := f.st.FindItemByKey(f.ctx, acct.ID, "GID1#original")
	motion, _ := f.st.FindItemByKey(f.ctx, acct.ID, "GID1#motion")
	if still == nil || motion == nil || still.Filename != key || motion.Filename != "20230621_145830.MP4" {
		t.Fatalf("still %+v, motion %+v", still, motion)
	}
	if c, _ := f.st.Counts(f.ctx, acct.ID); c["items_fallback"] != 0 || c["pending_sidecars"] != 0 {
		t.Errorf("counts %v", c)
	}
}

// A file merely sharing the name of a photo delivered elsewhere stays a
// nameless album entry: the resolver judges its settled row again on every
// run and comes to the same decision, without touching it.
func TestResolvedCollisionRowStaysResolved(t *testing.T) {
	f := newFixture(t)
	const export, year, key = "20261010T221633Z-1", "Photos from 2023", "X.jpg"
	raw := sidecar(key, "GIDX", tJuly, "")
	f.ingest("alice", f.zip("takeout-20261010T221633Z-1-001.zip", []file{
		{gp + "Trip/" + key, randBytes(60)},
		{gp + "Trip/" + key + ".supplemental-metadata.json", raw},
	}), Options{})
	f.ingest("alice", f.zip("takeout-20261010T221633Z-1-002.zip", []file{{gp + year + "/" + key + ".supplemental-metadata.json", raw}}), Options{})
	f.ingest("alice", f.zip("takeout-20261010T221633Z-1-003.zip", []file{{gp + year + "/" + key, randBytes(61)}}), Options{})
	acct, _ := f.st.GetAccount(f.ctx, "alice")
	check := func(when string) {
		t.Helper()
		if c, _ := f.st.Counts(f.ctx, acct.ID); c["items_fallback"] != 1 || c["items"] != 2 {
			t.Fatalf("%s: counts %v", when, c)
		}
		p, _ := f.st.GetPendingSidecar(f.ctx, acct.ID, export, year, key)
		if p == nil || !p.Resolved {
			t.Fatalf("%s: row %+v", when, p)
		}
		// settled, yet listed for the resolver because its file is nameless
		if rows, _ := f.st.PendingSidecarsOfExport(f.ctx, acct.ID, export); len(rows) != 1 || rows[0].Entry != key {
			t.Fatalf("%s: rows %+v", when, rows)
		}
	}
	// the sidecar of the year folder is deferred to the nameless file and
	// settled: the photo was delivered in the album folder
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, false); err != nil || promoted != 0 {
		t.Fatalf("first resolve: promoted %d, err %v", promoted, err)
	}
	check("after the first resolve")
	if _, promoted, err := f.ing.ResolvePending(f.ctx, acct, export, false); err != nil || promoted != 0 {
		t.Fatalf("second resolve: promoted %d, err %v", promoted, err)
	}
	check("after the second resolve")
}
