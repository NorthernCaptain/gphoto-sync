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
	acct, err := st.EnsureAccount(ctx, "leo", "r:")
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
	acct, _ := st.EnsureAccount(ctx, "leo", "r:")
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
	leo, _ := st.EnsureAccount(ctx, "leo", "r:")
	jane, _ := st.EnsureAccount(ctx, "jane", "r:")
	if _, err := st.UpsertArchive(ctx, &Archive{AccountID: leo.ID, RemoteID: "x", Name: "p", ExportID: "e-1", PartNo: 1, Status: ArchiveIngested}); err != nil {
		t.Fatal(err)
	}
	if a, _ := st.GetArchiveByRemoteID(ctx, jane.ID, "x"); a != nil {
		t.Error("another account's archive returned for jane")
	}
	if a, _ := st.GetArchiveByRemoteID(ctx, leo.ID, "x"); a == nil {
		t.Error("leo's own archive not found")
	}
	if a, _ := st.ArchiveByRemoteIDAnyAccount(ctx, "x"); a == nil || a.AccountID != leo.ID {
		t.Errorf("owner lookup: %+v", a)
	}
}
