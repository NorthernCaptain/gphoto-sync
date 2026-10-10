package db

import (
	"context"
	"path/filepath"
	"testing"
)

// The byte provenance backfill (0002, repaired by 0007 for databases that
// already ran it) must order export ids by timestamp and numeric sequence and
// attribute an item's bytes to the newest export whose occurrence, active or
// not, carries the current hash.
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
		// item 1: bytes S came with sequence 10 and again with a later export whose
		// album occurrence was pruned (inactive); sequence 9 (lexically larger) and
		// another export carry other bytes
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
	// first as 0002 alone attributed it (active occurrences only), then repaired
	if err := applyMigration(ctx, d, "0002_export_tracking.sql"); err != nil {
		t.Fatal(err)
	}
	var before string
	if err := d.QueryRowContext(ctx, `SELECT bytes_export_id FROM items WHERE id = 1`).Scan(&before); err != nil || before != "20260101T000000Z-10" {
		t.Fatalf("0002 alone: %q %v", before, err)
	}
	// item 3: provenance written by an interrupted ingest before its occurrence
	// (newer than any occurrence): the repair must not move it backward
	for _, q := range []string{
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (3, 'U', 1, 'p3', 'x'), (4, 'W', 1, 'p4', 'x')`,
		// item 4: current bytes W came with an export whose album occurrence was
		// pruned; 0002 fell back to a newer active occurrence that delivered
		// other bytes, which must be corrected back
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, first_seen, last_seen)
		   VALUES (4, 1, 4, 'original', 'k4', 'google_id', 'd.jpg', '.jpg', 'p4', 'g4', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260201T000000Z-1', 1, 'f', 'd.jpg', 'original', 'W', 4, 'resolved', 0),
		   (1, '20260301T000000Z-1', 1, 'f', 'd.jpg', 'original', 'Q', 4, 'resolved', 1)`,
		`UPDATE items SET bytes_export_id = '20260301T000000Z-1' WHERE id = 4`,
		`INSERT INTO archives(account_id, name, export_id, part_no, status) VALUES (1, 'p', '20260301T000000Z-1', 1, 'ingested')`,
		// item 5: like item 4, but the recorded export still has an unfinished part:
		// the provenance may be the ingest's own word, written before its occurrence
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (5, 'Y', 1, 'p5', 'x')`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, bytes_export_id, first_seen, last_seen)
		   VALUES (5, 1, 5, 'original', 'k5', 'google_id', 'e.jpg', '.jpg', 'p5', 'g5', '20260501T000000Z-1', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260101T000000Z-1', 1, 'f', 'e.jpg', 'original', 'Y', 5, 'resolved', 0),
		   (1, '20260501T000000Z-1', 1, 'f', 'e.jpg', 'original', 'R', 5, 'resolved', 1)`,
		`INSERT INTO archives(account_id, name, export_id, part_no, status) VALUES (1, 'q1', '20260501T000000Z-1', 1, 'ingested'), (1, 'q2', '20260501T000000Z-1', 2, 'partial')`,
		// item 6: current bytes Z came with an export whose album occurrence was
		// pruned; a later export's part also carried Z once, but a replacement of
		// that part dropped the entry (older database: inactive 'ingested' with no
		// finished entry left in the part's bookkeeping): not evidence
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (6, 'Z', 1, 'p6', 'x')`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, first_seen, last_seen)
		   VALUES (6, 1, 6, 'original', 'k6', 'google_id', 'g.jpg', '.jpg', 'p6', 'g6', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260101T000000Z-1', 1, 'f', 'g.jpg', 'original', 'Z', 6, 'resolved', 0),
		   (1, '20260301T000000Z-1', 2, 'f', 'g.jpg', 'original', 'Z', 6, 'ingested', 0)`,
		`INSERT INTO archives(id, account_id, name, export_id, part_no, status) VALUES (20, 1, 'r1', '20260101T000000Z-1', 1, 'ingested'), (21, 1, 'r2', '20260301T000000Z-1', 2, 'ingested')`,
		`INSERT INTO archive_entries(archive_id, entry_name, status) VALUES (20, 'Takeout/Google Photos/f/g.jpg', 'done'), (21, 'Takeout/Google Photos/f/other.jpg', 'done')`,
		// the pruned (inactive) occurrences of items 1 and 4 belong to parts whose
		// bookkeeping still lists their entries, as any ingested part's does
		`INSERT INTO archives(id, account_id, name, export_id, part_no, status) VALUES (30, 1, 's1', '20260401T000000Z-1', 1, 'ingested'), (31, 1, 's2', '20260201T000000Z-1', 1, 'ingested')`,
		`INSERT INTO archive_entries(archive_id, entry_name, status) VALUES (30, 'Takeout/Google Photos/f/a.jpg', 'done'), (31, 'Takeout/Google Photos/f/d.jpg', 'done')`,
		// item 7: a dropped legacy occurrence whose name holds LIKE metacharacters
		// must not be matched by a different finished entry (A_1.jpg vs AB1.jpg)
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (7, 'M', 1, 'p7', 'x')`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, first_seen, last_seen)
		   VALUES (7, 1, 7, 'original', 'k7', 'google_id', 'A_1.jpg', '.jpg', 'p7', 'g7', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260101T000000Z-1', 1, 'f', 'A_1.jpg', 'original', 'M', 7, 'resolved', 0),
		   (1, '20260301T000000Z-1', 3, 'f', 'A_1.jpg', 'original', 'M', 7, 'ingested', 0)`,
		`INSERT INTO archives(id, account_id, name, export_id, part_no, status) VALUES (40, 1, 't3', '20260301T000000Z-1', 3, 'ingested')`,
		`INSERT INTO archive_entries(archive_id, entry_name, status) VALUES (20, 'Takeout/Google Photos/f/A_1.jpg', 'done'), (40, 'Takeout/Google Photos/f/AB1.jpg', 'done')`,
		// item 8: a pruned occurrence whose entry sits in a nested folder of its
		// album (Takeout/<product>/Trip/sub/h.jpg): still present, not dropped
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (8, 'N', 1, 'p8', 'x')`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, first_seen, last_seen)
		   VALUES (8, 1, 8, 'original', 'k8', 'google_id', 'h.jpg', '.jpg', 'p8', 'g8', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260301T000000Z-1', 3, 'Trip', 'h.jpg', 'original', 'N', 8, 'resolved', 0)`,
		`INSERT INTO archive_entries(archive_id, entry_name, status) VALUES (40, 'Takeout/Google Photos/Trip/sub/h.jpg', 'done')`,
		// item 9: an occurrence awaiting its part's replacement (reset) carries
		// the current hash in a newer export: not evidence yet
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (9, 'K', 1, 'p9', 'x')`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, bytes_export_id, first_seen, last_seen)
		   VALUES (9, 1, 9, 'original', 'k9', 'google_id', 'i.jpg', '.jpg', 'p9', 'g9', '20260101T000000Z-1', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260101T000000Z-1', 1, 'f', 'i.jpg', 'original', 'K', 9, 'resolved', 1),
		   (1, '20260301T000000Z-1', 3, 'f', 'i.jpg', 'original', 'K', 9, 'reset', 0)`,
		// item 10: the recorded export's only occurrence was dropped by a finished
		// replacement; an older export's occurrence still carries the bytes
		`INSERT INTO blobs(id, sha256, size, canonical_path, first_seen) VALUES (10, 'L', 1, 'p10', 'x')`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, bytes_export_id, first_seen, last_seen)
		   VALUES (10, 1, 10, 'original', 'k10', 'google_id', 'j.jpg', '.jpg', 'p10', 'g10', '20260301T000000Z-1', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260101T000000Z-1', 1, 'f', 'j.jpg', 'original', 'L', 10, 'resolved', 1),
		   (1, '20260301T000000Z-1', 3, 'f', 'j.jpg', 'original', 'L', 10, 'removed', 0)`,
		`INSERT INTO items(id, account_id, blob_id, variant, identity_key, identity_src, filename, ext, lib_path, group_id, bytes_export_id, first_seen, last_seen)
		   VALUES (3, 1, 3, 'original', 'k3', 'google_id', 'c.jpg', '.jpg', 'p3', 'g3', '20260601T000000Z-1', 'x', 'x')`,
		`INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, item_id, state, active) VALUES
		   (1, '20260201T000000Z-1', 1, 'f', 'c.jpg', 'original', 'V', 3, 'resolved', 1)`,
	} {
		if _, err := d.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if err := migrate(ctx, d); err != nil {
		t.Fatal(err)
	}
	var got string
	for _, c := range []struct{ q, want string }{
		{`SELECT bytes_export_id FROM items WHERE id = 1`, "20260401T000000Z-1"},
		{`SELECT bytes_export_id FROM items WHERE id = 2`, "20260101T000000Z-10"},
		{`SELECT bytes_export_id FROM items WHERE id = 3`, "20260601T000000Z-1"},
		{`SELECT bytes_export_id FROM items WHERE id = 4`, "20260201T000000Z-1"},
		{`SELECT bytes_export_id FROM items WHERE id = 5`, "20260501T000000Z-1"},
		{`SELECT bytes_export_id FROM items WHERE id = 6`, "20260101T000000Z-1"},
		{`SELECT bytes_export_id FROM items WHERE id = 7`, "20260101T000000Z-1"},
		{`SELECT state FROM occurrences WHERE item_id = 6 AND export_id = '20260301T000000Z-1'`, "removed"},
		{`SELECT state FROM occurrences WHERE item_id = 7 AND export_id = '20260301T000000Z-1'`, "removed"},
		{`SELECT state FROM occurrences WHERE item_id = 6 AND export_id = '20260101T000000Z-1'`, "resolved"},
		{`SELECT state FROM occurrences WHERE item_id = 8`, "resolved"},
		{`SELECT bytes_export_id FROM items WHERE id = 8`, "20260301T000000Z-1"},
		{`SELECT bytes_export_id FROM items WHERE id = 9`, "20260101T000000Z-1"},
		{`SELECT state FROM occurrences WHERE item_id = 9 AND export_id = '20260301T000000Z-1'`, "reset"},
		{`SELECT bytes_export_id FROM items WHERE id = 10`, "20260101T000000Z-1"},
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
