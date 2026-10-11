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
	leo, _ := st.EnsureAccount(ctx, "leo", "r:")
	ira, _ := st.EnsureAccount(ctx, "ira", "r:")
	gone, _ := st.EnsureAccount(ctx, "gone", "r:")
	blob := func(sha string, size int64) *Blob {
		t.Helper()
		b, err := st.InsertBlob(ctx, sha, size, "leo/library/2020/01/"+sha)
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
	a := item(leo, shared, "a.jpg")
	item(leo, own, "b.MP4")
	item(ira, shared, "a.jpg")
	item(gone, other, "c.jpg")
	if err := st.InsertVersion(ctx, a, old.ID, "leo/versions/a.jpg", "e-0"); err != nil {
		t.Fatal(err)
	}
	albumID, err := st.InsertAlbum(ctx, &Album{AccountID: leo.ID, Title: "Trip", TakeoutFolder: "Trip", Kind: "own", Path: "leo/albums/Trip"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertAlbumItem(ctx, &AlbumItem{AlbumID: albumID, ItemID: a, LinkPath: "leo/albums/Trip/a.jpg", Direct: true}); err != nil {
		t.Fatal(err)
	}
	// an album pruned empty has no folder any more and is not counted
	if _, err := st.InsertAlbum(ctx, &Album{AccountID: leo.ID, Title: "Empty", TakeoutFolder: "Empty", Kind: "own", Path: "leo/albums/Empty"}, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		ids  []int64
		want LibraryTotals
	}{
		{"leo", []int64{leo.ID}, LibraryTotals{Items: 2, Albums: 1, Links: 1, Bytes: 180, LinkBytes: 100, ByExt: map[string]int{".jpg": 1, ".mp4": 1}}},
		{"ira", []int64{ira.ID}, LibraryTotals{Items: 1, Bytes: 100, ByExt: map[string]int{".jpg": 1}}},
		{"configured", []int64{leo.ID, ira.ID}, LibraryTotals{Items: 3, Albums: 1, Links: 1, Bytes: 180, LinkBytes: 100, ByExt: map[string]int{".jpg": 2, ".mp4": 1}}},
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
