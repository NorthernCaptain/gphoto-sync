package store

import (
	"context"
	"path/filepath"
	"testing"

	"gphoto-sync/internal/db"
)

// Recording another remote file for a part (a replacement) owes a new remote
// deletion: the stamp of the earlier file's deletion must not survive.
func TestUpsertArchiveClearsDeletionOnNewRemote(t *testing.T) {
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := New(d)
	acct, err := st.EnsureAccount(ctx, "alice", "r:")
	if err != nil {
		t.Fatal(err)
	}
	a, err := st.UpsertArchive(ctx, &Archive{AccountID: acct.ID, RemoteID: "x", Name: "p", ExportID: "e-1", PartNo: 1, Size: 5, MD5: "aa", Status: ArchiveIngested})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetArchiveRemoteDeleted(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if w, _ := st.ArchivesAwaitingRemoteDelete(ctx, acct.ID); len(w) != 0 {
		t.Fatalf("deleted archive still awaiting deletion: %d", len(w))
	}
	if a, _ = st.GetArchive(ctx, acct.ID, "e-1", 1); a.RemoteDeletedAt == "" {
		t.Fatal("deletion stamp not exposed on the archive row")
	}
	// a local ingest of the same part (no remote file) changes nothing about the remote
	if _, err := st.UpsertArchive(ctx, &Archive{AccountID: acct.ID, Name: "p", ExportID: "e-1", PartNo: 1, Status: ArchiveIngested}); err != nil {
		t.Fatal(err)
	}
	if w, _ := st.ArchivesAwaitingRemoteDelete(ctx, acct.ID); len(w) != 0 {
		t.Fatalf("local ingest revived the deletion: %d", len(w))
	}
	if a, _ := st.GetArchive(ctx, acct.ID, "e-1", 1); a.RemoteID != "x" || a.MD5 != "aa" || a.Size != 5 {
		t.Fatalf("local ingest lost the remote identity: %+v", a)
	}
	// a replacement under a new id whose listing carries no hash: recorded as
	// listed (hash unknown, new size) and its deletion owed
	if _, err := st.UpsertArchive(ctx, &Archive{AccountID: acct.ID, RemoteID: "y", Name: "p", ExportID: "e-1", PartNo: 1, Size: 6, Status: ArchiveIngested}); err != nil {
		t.Fatal(err)
	}
	w, _ := st.ArchivesAwaitingRemoteDelete(ctx, acct.ID)
	if len(w) != 1 || w[0].RemoteID != "y" || w[0].MD5 != "" || w[0].Size != 6 {
		t.Fatalf("replacement not recorded as listed: %+v", w)
	}
}

// A staged copy is listed for cleanup whatever the part's status became.
func TestStagedArchivesListedRegardlessOfStatus(t *testing.T) {
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := New(d)
	acct, _ := st.EnsureAccount(ctx, "alice", "r:")
	a, err := st.UpsertArchive(ctx, &Archive{AccountID: acct.ID, RemoteID: "x", Name: "p", ExportID: "e-1", PartNo: 1, MD5: "aa", Status: ArchiveIngested})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetArchiveStaged(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	// a local ingest of the same part changes the status but not the staging
	if _, err := st.UpsertArchive(ctx, &Archive{AccountID: acct.ID, Name: "p", ExportID: "e-1", PartNo: 1, Status: ArchiveWithErrors}); err != nil {
		t.Fatal(err)
	}
	if w, _ := st.ArchivesAwaitingRemoteDelete(ctx, acct.ID); len(w) != 0 {
		t.Fatalf("non-ingested row listed as awaiting deletion: %d", len(w))
	}
	s, _ := st.ArchivesStaged(ctx, acct.ID)
	if len(s) != 1 || s[0].RemoteID != "x" || s[0].StagedAt == "" {
		t.Fatalf("staged row not listed: %+v", s)
	}
	if err := st.SetArchiveRemoteDeleted(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if s, _ := st.ArchivesStaged(ctx, acct.ID); len(s) != 0 {
		t.Fatalf("finished staged row still listed: %d", len(s))
	}
}

// A Drive file id recorded by one account is not another account's ingest.
func TestArchiveLookupIsPerAccount(t *testing.T) {
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := New(d)
	alice, _ := st.EnsureAccount(ctx, "alice", "r:")
	jane, _ := st.EnsureAccount(ctx, "jane", "r:")
	if _, err := st.UpsertArchive(ctx, &Archive{AccountID: alice.ID, RemoteID: "x", Name: "p", ExportID: "e-1", PartNo: 1, Status: ArchiveIngested}); err != nil {
		t.Fatal(err)
	}
	if a, _ := st.GetArchiveByRemoteID(ctx, jane.ID, "x"); a != nil {
		t.Error("another account's archive returned for jane")
	}
	if a, _ := st.GetArchiveByRemoteID(ctx, alice.ID, "x"); a == nil {
		t.Error("alice's own archive not found")
	}
	if a, _ := st.ArchiveByRemoteIDAnyAccount(ctx, "x"); a == nil || a.AccountID != alice.ID {
		t.Errorf("owner lookup: %+v", a)
	}
}

// Renaming an album's link paths must work for names with non-ASCII letters
// (SQLite substr counts characters, not bytes).
func TestRenameLinkPrefixNonASCII(t *testing.T) {
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := New(d)
	acct, _ := st.EnsureAccount(ctx, "alice", "r:")
	b, _ := st.InsertBlob(ctx, "s", 1, "alice/library/2016/04/a.jpg")
	itemID, err := st.InsertItem(ctx, &Item{AccountID: acct.ID, BlobID: b.ID, Variant: "original", IdentityKey: "k", IdentitySrc: IdentityGoogle, Filename: "a.jpg", Ext: ".jpg", LibPath: "alice/library/2016/04/a.jpg", GroupID: "k"})
	if err != nil {
		t.Fatal(err)
	}
	albumID, err := st.InsertAlbum(ctx, &Album{AccountID: acct.ID, Title: "Spotlight on Андрейка", TakeoutFolder: "Spotlight on Андрейка(1)", Path: "alice/albums/Spotlight on Андрейка(1)", Kind: "own"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := "alice/albums/Spotlight on Андрейка(1)/a.jpg"
	if err := st.UpsertAlbumItem(ctx, &AlbumItem{AlbumID: albumID, ItemID: itemID, LinkPath: old, Direct: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.RenameLinkPrefix(ctx, albumID, "alice/albums/Spotlight on Андрейка(1)/", "alice/albums/Spotlight on Андрейка~6acaacac/"); err != nil {
		t.Fatal(err)
	}
	ai, _ := st.GetAlbumItem(ctx, albumID, itemID)
	if ai == nil || ai.LinkPath != "alice/albums/Spotlight on Андрейка~6acaacac/a.jpg" {
		t.Fatalf("link path after rename: %+v", ai)
	}
}

// A same-named media occurrence satisfies a stored sidecar only when it is
// the photo the sidecar names.
// A stored sidecar keeps its consumption state only while the same part
// stores the same payload again.
func TestPendingUpsertResetsStateOnChange(t *testing.T) {
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := New(d)
	acct, _ := st.EnsureAccount(ctx, "alice", "r:")
	raw1, raw2 := []byte(`{"title":"a"}`), []byte(`{"title":"b"}`)
	put := func(part int, raw []byte) {
		t.Helper()
		if err := st.PutPendingSidecar(ctx, acct.ID, "e-1", part, "Trip", "a.jpg", raw, "AAA"); err != nil {
			t.Fatal(err)
		}
	}
	state := func() (bool, string) {
		t.Helper()
		p, err := st.GetPendingSidecar(ctx, acct.ID, "e-1", "Trip", "a.jpg")
		if err != nil || p == nil {
			t.Fatalf("row: %v %v", p, err)
		}
		rows, _ := st.PendingSidecarsOfExport(ctx, acct.ID, "e-1")
		entry := "(hidden)"
		for _, r := range rows {
			entry = r.Entry
		}
		return p.Resolved, entry
	}
	put(1, raw1)
	if err := st.MarkPendingResolved(ctx, acct.ID, "e-1", "Trip", "a.jpg"); err != nil {
		t.Fatal(err)
	}
	put(1, raw1)
	if resolved, _ := state(); !resolved {
		t.Error("the same payload from the same part lost the resolved flag")
	}
	put(2, raw1)
	if resolved, _ := state(); resolved {
		t.Error("another part's copy kept the resolved flag")
	}
	if err := st.DeferPendingSidecar(ctx, acct.ID, "e-1", "Trip", "a.jpg", "a.jpg", "original", "a.jpg"); err != nil {
		t.Fatal(err)
	}
	put(2, raw1)
	if _, entry := state(); entry != "a.jpg" {
		t.Errorf("the same payload lost its deferred entry: %q", entry)
	}
	put(2, raw2)
	if resolved, entry := state(); resolved || entry != "" {
		t.Errorf("a changed payload kept its state: resolved=%v entry=%q", resolved, entry)
	}
}

func TestPendingFoundOnlyByNamedPhoto(t *testing.T) {
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := New(d)
	acct, _ := st.EnsureAccount(ctx, "alice", "r:")
	b, _ := st.InsertBlob(ctx, "s", 1, "alice/library/2016/04/c.jpg")
	other, _ := st.InsertItem(ctx, &Item{AccountID: acct.ID, BlobID: b.ID, GoogleID: "CC2", Variant: "original", IdentityKey: "CC2#original", IdentitySrc: IdentityGoogle, Filename: "c.jpg", Ext: ".jpg", LibPath: "alice/library/2016/04/c.jpg", GroupID: "CC2#original"})
	if _, err := st.UpsertOccurrence(ctx, &Occurrence{AccountID: acct.ID, ExportID: "e-1", PartNo: 1, Folder: "Trip", EntryName: "c.jpg", Variant: "original", SHA256: "s", ItemID: other, State: OccResolved}); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"title":"c.jpg","url":"https://photos.google.com/photo/CCC"}`)
	if err := st.PutPendingSidecar(ctx, acct.ID, "e-1", 2, "Trip", "c.jpg", raw, "CCC"); err != nil {
		t.Fatal(err)
	}
	rows, _ := st.PendingSidecarsOfExport(ctx, acct.ID, "e-1")
	if len(rows) != 1 {
		t.Fatalf("sidecar naming another photo hidden by a same-named file: %+v", rows)
	}
	if n, _ := st.CountPendingSidecars(ctx, acct.ID); n != 1 {
		t.Errorf("count: %d", n)
	}
	// the same row for the photo that is there: found
	if err := st.PutPendingSidecar(ctx, acct.ID, "e-1", 2, "Trip", "c.jpg", raw, "CC2"); err != nil {
		t.Fatal(err)
	}
	if rows, _ := st.PendingSidecarsOfExport(ctx, acct.ID, "e-1"); len(rows) != 0 {
		t.Errorf("sidecar of the present photo still listed: %+v", rows)
	}
}
