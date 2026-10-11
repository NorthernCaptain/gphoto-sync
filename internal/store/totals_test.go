package store

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gphoto-sync/internal/db"
)

// Library totals: items and albums per account, bytes over the distinct blobs
// of items and kept versions, bytes shared between accounts by hardlink
// counted once in a combined total, and accounts outside the list (removed
// from the configuration) left out of it.
func TestLibraryTotals(t *testing.T) {
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := New(d)
	alice, _ := st.EnsureAccount(ctx, "alice", "r:")
	bob, _ := st.EnsureAccount(ctx, "bob", "r:")
	gone, _ := st.EnsureAccount(ctx, "gone", "r:")
	blob := func(sha string, size int64) *Blob {
		t.Helper()
		b, err := st.InsertBlob(ctx, sha, size, "alice/library/2020/01/"+sha)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	shared, own, old, other := blob("shared", 100), blob("own", 50), blob("old", 30), blob("other", 70)
	item := func(acct *Account, b *Blob, name string) int64 {
		t.Helper()
		id, err := st.InsertItem(ctx, &Item{AccountID: acct.ID, BlobID: b.ID, GoogleID: "G" + name, Variant: "original", IdentityKey: "G" + name + "#original",
			IdentitySrc: "google_id", Filename: name, Ext: strings.ToLower(filepath.Ext(name)), LibPath: acct.Name + "/library/2020/01/" + name, GroupID: "G" + name})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	a := item(alice, shared, "a.jpg")
	item(alice, own, "b.MP4")
	item(bob, shared, "a.jpg")
	item(gone, other, "c.jpg")
	if err := st.InsertVersion(ctx, a, old.ID, "alice/versions/a.jpg", "e-0"); err != nil {
		t.Fatal(err)
	}
	albumID, err := st.InsertAlbum(ctx, &Album{AccountID: alice.ID, Title: "Trip", TakeoutFolder: "Trip", Kind: "own", Path: "alice/albums/Trip"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertAlbumItem(ctx, &AlbumItem{AlbumID: albumID, ItemID: a, LinkPath: "alice/albums/Trip/a.jpg", Direct: true}); err != nil {
		t.Fatal(err)
	}
	// an album pruned empty has no folder any more and is not counted
	if _, err := st.InsertAlbum(ctx, &Album{AccountID: alice.ID, Title: "Empty", TakeoutFolder: "Empty", Kind: "own", Path: "alice/albums/Empty"}, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		ids  []int64
		want LibraryTotals
	}{
		{"alice", []int64{alice.ID}, LibraryTotals{Items: 2, Albums: 1, Links: 1, Bytes: 180, LinkBytes: 100, ByExt: map[string]int{".jpg": 1, ".mp4": 1}}},
		{"bob", []int64{bob.ID}, LibraryTotals{Items: 1, Bytes: 100, ByExt: map[string]int{".jpg": 1}}},
		{"configured", []int64{alice.ID, bob.ID}, LibraryTotals{Items: 3, Albums: 1, Links: 1, Bytes: 180, LinkBytes: 100, ByExt: map[string]int{".jpg": 2, ".mp4": 1}}},
		{"none", nil, LibraryTotals{ByExt: map[string]int{}}},
	} {
		got, err := st.LibraryTotals(ctx, tc.ids)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !reflect.DeepEqual(*got, tc.want) {
			t.Errorf("%s: got %+v, want %+v", tc.name, *got, tc.want)
		}
	}
}
