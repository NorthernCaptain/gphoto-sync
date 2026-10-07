package db

import (
	"context"
	"path/filepath"
	"testing"
)

// The 0002 backfill must order export ids by timestamp and numeric sequence
// and attribute an item's bytes to the export whose occurrence carries the
// current hash.
func TestExportTrackingBackfill(t *testing.T) {
	ctx := context.Background()
	d, err := openRaw(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := applyMigration(ctx, d, "0001_init.sql"); err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		`INSERT INTO accounts(id, name, rclone_remote, created_at) VALUES (1, 'leo', 'r:', 'x')`,
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (1, 'S', 1, 'p1', 'x'), (2, 'T', 1, 'p2', 'x')`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, first_seen, last_seen)
		   VALUES (1, 1, 1, 'original', 'k1', 'google_id', 'a.jpg', '.jpg', 'p1', 'g1', 'x', 'x'),
		          (2, 1, 2, 'original', 'k2', 'google_id', 'b.jpg', '.jpg', 'p2', 'g2', 'x', 'x')`,
		`INSERT INTO sidecars(item_id, json, sha256) VALUES (1, X'00', 'h'), (2, X'00', 'h')`,
		// item 1: bytes S came with sequence 10; sequence 9 (lexically larger) and a
		// later export carry other bytes
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260101T000000Z-10', 1, 'f', 'a.jpg', 'original', 'S', 1, 'resolved', 1),
		   (1, '20260101T000000Z-9',  1, 'f', 'a.jpg', 'original', 'T', 1, 'resolved', 1),
		   (1, '20260301T000000Z-1',  1, 'f', 'a.jpg', 'original', 'T', 1, 'resolved', 1),
		   (1, '20260401T000000Z-1',  1, 'f', 'a.jpg', 'original', 'S', 1, 'resolved', 0)`,
		// item 2: no hash recorded; sequence 10 is the newest
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, item_id, state, active) VALUES
		   (1, '20260101T000000Z-9',  1, 'f', 'b.jpg', 'original', 2, 'resolved', 1),
		   (1, '20260101T000000Z-10', 1, 'f', 'b.jpg', 'original', 2, 'resolved', 1)`,
	}
	for _, q := range stmts {
		if _, err := d.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if err := migrate(ctx, d); err != nil {
		t.Fatal(err)
	}
	var got string
	for _, c := range []struct{ q, want string }{
		{`SELECT bytes_export_id FROM items WHERE id = 1`, "20260101T000000Z-10"},
		{`SELECT bytes_export_id FROM items WHERE id = 2`, "20260101T000000Z-10"},
	} {
		if err := d.QueryRowContext(ctx, c.q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		if got != c.want {
			t.Errorf("%s = %q, want %q", c.q, got, c.want)
		}
	}
	// a sidecar's source cannot be told from occurrences: it stays unknown
	var n int
	if err := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM sidecars WHERE export_id IS NOT NULL OR part_no IS NOT NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d migrated sidecars were given a guessed source", n)
	}
}

func TestOpenReadOnly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	if _, err := OpenReadOnly(ctx, path); err == nil {
		t.Fatal("read-only open created or accepted a missing database")
	}
	d, err := openRaw(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyMigration(ctx, d, "0001_init.sql"); err != nil {
		t.Fatal(err)
	}
	d.Close()
	if _, err := OpenReadOnly(ctx, path); err == nil {
		t.Fatal("read-only open accepted a database behind the schema")
	}
	if d, err = Open(ctx, path); err != nil {
		t.Fatal(err)
	}
	d.Close()
	d, err = OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatalf("read-only open of a current database: %v", err)
	}
	defer d.Close()
	if _, err := d.ExecContext(ctx, `INSERT INTO accounts(name, created_at) VALUES ('x', 'now')`); err == nil {
		t.Fatal("read-only connection accepted a write")
	}
	var n int
	if err := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&n); err != nil {
		t.Fatalf("read on the read-only connection: %v", err)
	}
}
