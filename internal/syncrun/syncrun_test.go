package syncrun

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/db"
	"gphoto-sync/internal/ingest"
	"gphoto-sync/internal/rclone"
	"gphoto-sync/internal/store"
)

// fakeRemote serves fixed listings and records deletions; CopyID serves a
// local file per Drive id when one is registered in files.
type fakeRemote struct {
	listings map[string][]rclone.RemoteFile
	deleted  []string
	listErr  map[string]error
	files    map[string]string
	copies   int // downloads served from files
}

func (f *fakeRemote) List(_ context.Context, remote string) ([]rclone.RemoteFile, error) {
	if err := f.listErr[remote]; err != nil {
		return nil, err
	}
	return f.listings[remote], nil
}
func (f *fakeRemote) CopyID(_ context.Context, _, id, dest string) error {
	src, ok := f.files[id]
	if !ok {
		return nil
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	f.copies++
	return os.WriteFile(dest, data, 0o644)
}
func (f *fakeRemote) MoveID(context.Context, string, string, string) error { return nil }
func (f *fakeRemote) DeleteFile(_ context.Context, p string) error {
	f.deleted = append(f.deleted, p)
	return nil
}

func newRunner(t *testing.T, rc Remote) (*Runner, *store.Store) {
	t.Helper()
	root := t.TempDir()
	d, err := db.Open(context.Background(), filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	st := store.New(d)
	cfg := config.Default()
	cfg.DataDir, cfg.StateDir, cfg.Takeout.InboxDir = filepath.Join(root, "data"), root, filepath.Join(root, "inbox")
	cfg.Accounts = map[string]config.Account{"alice": {RcloneRemote: "gdrive:Takeout"}}
	os.MkdirAll(cfg.DataDir, 0o755)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Runner{Cfg: cfg, St: st, Log: log, Ing: ingest.New(cfg, st, log), RC: rc}, st
}

// A part re-uploaded under a new id while its earlier copy still sits in the
// staging folder: the staged copy is deleted before the row records the new
// file, on the resume path as much as on the replacement path.
func TestResumeSettlesStagedCopyFirst(t *testing.T) {
	ctx := context.Background()
	const name = "takeout-20261006T035113Z-1-001.zip"
	md5 := map[string]string{"MD5": "aa"}
	old := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	rc := &fakeRemote{listings: map[string][]rclone.RemoteFile{
		"gdrive:Takeout":                {{ID: "B", Name: name, Size: 5, ModTime: old, Hashes: md5}},
		"gdrive:Takeout/" + trashFolder: {{ID: "A", Name: name + ".A", Size: 5, Hashes: md5}},
	}}
	r, st := newRunner(t, rc)
	r.Now = func() time.Time { return old.Add(48 * time.Hour) }
	acct, _ := st.EnsureAccount(ctx, "alice", "gdrive:Takeout")
	a, err := st.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: "A", Name: name, ExportID: "20261006T035113Z-1", PartNo: 1, Size: 5, MD5: "aa", Status: store.ArchiveIngested})
	if err != nil {
		t.Fatal(err)
	}
	_ = st.SetArchiveStaged(ctx, a.ID)
	// a later local ingest left the part partial with an error: the new copy resumes it
	if err := st.FinishArchive(ctx, a.ID, store.ArchivePartial, 10, 9, 0, 1, 20); err != nil {
		t.Fatal(err)
	}
	ar := r.runAccount(ctx, "alice", false)
	if len(rc.deleted) != 1 || rc.deleted[0] != "gdrive:Takeout/"+trashFolder+"/"+name+".A" {
		t.Fatalf("staged copy not deleted before adopting the new file: deleted %v, report %+v", rc.deleted, ar)
	}
	got, _ := st.GetArchive(ctx, acct.ID, "20261006T035113Z-1", 1)
	if got.RemoteID != "B" || got.StagedAt != "" {
		t.Errorf("row after the resume: %+v", got)
	}
}

// A file staged for deletion is finished on a later run even when a later
// local ingest left the part with errors, and whether or not remote deletion
// is still switched on; a never-staged errored part is left alone.
func TestFinishStagedDeletes(t *testing.T) {
	ctx := context.Background()
	md5 := map[string]string{"MD5": "aa"}
	for _, deleteOn := range []bool{true, false} {
		rc := &fakeRemote{listings: map[string][]rclone.RemoteFile{
			"gdrive:Takeout/" + trashFolder: {{ID: "x", Name: "p.x", Size: 5, Hashes: md5}},
		}}
		r, st := newRunner(t, rc)
		r.Cfg.Takeout.DeleteRemoteAfterIngest = deleteOn
		acct, _ := st.EnsureAccount(ctx, "alice", "gdrive:Takeout")
		a, err := st.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: "x", Name: "p", ExportID: "e-1", PartNo: 1, Size: 5, MD5: "aa", Status: store.ArchiveIngested})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SetArchiveStaged(ctx, a.ID); err != nil {
			t.Fatal(err)
		}
		// a later local ingest of the part failed one entry
		if err := st.FinishArchive(ctx, a.ID, store.ArchiveWithErrors, 10, 9, 0, 1, 20); err != nil {
			t.Fatal(err)
		}
		// an errored part never staged is in neither listing: nothing to touch
		if _, err := st.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: "y", Name: "q", ExportID: "e-1", PartNo: 2, Size: 5, MD5: "bb", Status: store.ArchiveWithErrors}); err != nil {
			t.Fatal(err)
		}
		ar := &AccountReport{Name: "alice"}
		r.finishStagedDeletes(ctx, ar, acct.ID, "gdrive:Takeout", nil)
		if len(rc.deleted) != 1 || rc.deleted[0] != "gdrive:Takeout/"+trashFolder+"/p.x" {
			t.Fatalf("delete_remote_after_ingest=%v: deleted %v", deleteOn, rc.deleted)
		}
		if got, _ := st.GetArchive(ctx, acct.ID, "e-1", 1); got.RemoteDeletedAt == "" {
			t.Errorf("delete_remote_after_ingest=%v: staged deletion not recorded", deleteOn)
		}
		if ar.RemoteDeleted != 1 || ar.Exit != ExitOK {
			t.Errorf("delete_remote_after_ingest=%v: report %+v", deleteOn, ar)
		}
	}
}

// A staged file whose listing no longer matches the ingested copy is left
// alone and reported; one that is in neither place is recorded as gone.
func TestFinishStagedDeletesVerifies(t *testing.T) {
	ctx := context.Background()
	rc := &fakeRemote{listings: map[string][]rclone.RemoteFile{
		"gdrive:Takeout/" + trashFolder: {{ID: "x", Name: "p.x", Size: 5, Hashes: map[string]string{"MD5": "zz"}}},
	}}
	r, st := newRunner(t, rc)
	r.Cfg.Takeout.DeleteRemoteAfterIngest = true
	acct, _ := st.EnsureAccount(ctx, "alice", "gdrive:Takeout")
	a, _ := st.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: "x", Name: "p", ExportID: "e-1", PartNo: 1, Size: 5, MD5: "aa", Status: store.ArchiveIngested})
	_ = st.SetArchiveStaged(ctx, a.ID)
	b, _ := st.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: "gone", Name: "r", ExportID: "e-1", PartNo: 3, Size: 5, MD5: "cc", Status: store.ArchiveIngested})
	_ = st.SetArchiveStaged(ctx, b.ID)
	ar := &AccountReport{Name: "alice"}
	r.finishStagedDeletes(ctx, ar, acct.ID, "gdrive:Takeout", nil)
	if len(rc.deleted) != 0 {
		t.Fatalf("mismatching staged file deleted: %v", rc.deleted)
	}
	if ar.Exit != ExitPartial || len(ar.Errors) != 1 {
		t.Errorf("mismatch not reported: %+v", ar)
	}
	if got, _ := st.GetArchive(ctx, acct.ID, "e-1", 3); got.RemoteDeletedAt == "" {
		t.Error("file in neither place not recorded as gone")
	}
	if got, _ := st.GetArchive(ctx, acct.ID, "e-1", 1); got.RemoteDeletedAt != "" {
		t.Error("mismatching staged file recorded as deleted")
	}
}

func TestDecide(t *testing.T) {
	md5 := func(h string) map[string]string { return map[string]string{"MD5": h} }
	cases := []struct {
		name  string
		arch  *store.Archive
		f     rclone.RemoteFile
		retry bool
		want  disposition
	}{
		{"unknown part", nil, rclone.RemoteFile{ID: "x"}, false, dispNew},
		{"given up entries, retry requested", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveWithErrors}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, true, dispResume},
		{"same file, partial", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchivePartial}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispResume},
		{"same file, ingested", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveIngested, Entries: 3}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispSkipIngested},
		{"same file, ingested with nothing recognized", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveIngested}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispReplace},
		{"same file, ingested, nothing recognized by count", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveIngested, Entries: 3, RecognizedKnown: true}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispReplace},
		{"same file, ingested, only skipped media", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveIngested, Recognized: 40, RecognizedKnown: true}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispSkipIngested},
		{"unprovable, ingested with nothing recognized", &store.Archive{Size: 5, Status: store.ArchiveIngested}, rclone.RemoteFile{ID: "y", Size: 5}, false, dispReplace},
		{"same file, given up entries", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveWithErrors}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispSkipErrors},
		{"other md5", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveIngested}, rclone.RemoteFile{ID: "x", Hashes: md5("bb")}, false, dispReplace},
		{"other size, partial", &store.Archive{RemoteID: "x", Size: 5, Status: store.ArchivePartial}, rclone.RemoteFile{ID: "y", Size: 6}, false, dispReplace},
		{"unprovable, ingested", &store.Archive{Size: 5, Status: store.ArchiveIngested, Entries: 3}, rclone.RemoteFile{ID: "y", Size: 5}, false, dispSkipUnverified},
		{"unprovable, partial", &store.Archive{Size: 5, Status: store.ArchivePartial}, rclone.RemoteFile{ID: "y", Size: 5}, false, dispReplace},
		{"same id and size, no hash, partial", &store.Archive{RemoteID: "x", Size: 5, Status: store.ArchivePartial}, rclone.RemoteFile{ID: "x", Size: 5}, false, dispResume},
	}
	for _, c := range cases {
		if got, _ := decide(c.arch, c.f, c.retry); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestMergeArchives(t *testing.T) {
	a := []*store.Archive{{ID: 1}, {ID: 2}}
	b := []*store.Archive{{ID: 2}, {ID: 3}}
	got := mergeArchives(a, b)
	if len(got) != 3 || got[0].ID != 1 || got[1].ID != 2 || got[2].ID != 3 {
		t.Errorf("merge: %+v", got)
	}
}

func TestNotDeletable(t *testing.T) {
	md5 := func(h string) map[string]string { return map[string]string{"MD5": h} }
	cases := []struct {
		name string
		arch store.Archive
		f    rclone.RemoteFile
		ok   bool
	}{
		{"same id and md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("aa")}, true},
		{"other id", store.Archive{RemoteID: "x", MD5: "aa"}, rclone.RemoteFile{ID: "y", Hashes: md5("aa")}, false},
		{"md5 changed", store.Archive{RemoteID: "x", MD5: "aa"}, rclone.RemoteFile{ID: "x", Hashes: md5("bb")}, false},
		{"md5 missing from listing", store.Archive{RemoteID: "x", MD5: "aa"}, rclone.RemoteFile{ID: "x"}, false},
		{"no md5 recorded, same size", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("bb")}, true},
		{"no md5 recorded, other size", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 6}, false},
	}
	for _, c := range cases {
		if why := notDeletable(&c.arch, c.f); (why == "") != c.ok {
			t.Errorf("%s: %q", c.name, why)
		}
	}
}

func TestCompareArchive(t *testing.T) {
	md5 := func(h string) map[string]string { return map[string]string{"MD5": h} }
	cases := []struct {
		name     string
		arch     store.Archive
		f        rclone.RemoteFile
		differs  bool
		verified bool
	}{
		{"same id and md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("aa")}, false, true},
		{"re-upload, same md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "y", Size: 5, Hashes: md5("aa")}, false, true},
		{"re-upload, other md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "y", Size: 5, Hashes: md5("bb")}, true, true},
		{"changed in place", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "x", Size: 6, Hashes: md5("bb")}, true, true},
		{"no md5 recorded, size differs", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 6, Hashes: md5("bb")}, true, true},
		{"no md5 recorded, same id and size", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("bb")}, false, true},
		{"no md5 recorded, other id, same size", store.Archive{RemoteID: "", Size: 5}, rclone.RemoteFile{ID: "y", Size: 5, Hashes: md5("bb")}, false, false},
		{"nothing recorded", store.Archive{}, rclone.RemoteFile{ID: "y", Size: 5}, false, false},
	}
	for _, c := range cases {
		why, verified := compareArchive(&c.arch, c.f)
		if (why != "") != c.differs || verified != c.verified {
			t.Errorf("%s: why=%q verified=%v", c.name, why, verified)
		}
	}
}
