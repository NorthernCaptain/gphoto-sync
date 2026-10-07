// Package store is the typed data-access layer over the SQLite state database.
// All paths stored here are relative to data_dir and start with the account
// folder. Times are RFC 3339 UTC strings.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Querier is satisfied by *sql.DB and *sql.Tx.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Store wraps a database (or a transaction when created by Tx).
type Store struct {
	db *sql.DB
	q  Querier
}

// New creates a Store over db.
func New(db *sql.DB) *Store { return &Store{db: db, q: db} }

// Tx runs fn inside a transaction; the Store passed to fn uses the transaction.
func (s *Store) Tx(ctx context.Context, fn func(tx *Store) error) error {
	if _, already := s.q.(*sql.Tx); already {
		return fn(s)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	ts := &Store{db: s.db, q: tx}
	if err := fn(ts); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Now returns the current time formatted for storage.
func Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// FormatTime formats a time for storage ("" for zero).
func FormatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// ParseTime parses a stored time ("" -> zero).
func ParseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullInt(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}

// ---- accounts -------------------------------------------------------------

// Account is a configured Google account.
type Account struct {
	ID           int64
	Name         string
	RcloneRemote string
}

// EnsureAccount returns the account row for name, creating it if needed.
func (s *Store) EnsureAccount(ctx context.Context, name, remote string) (*Account, error) {
	if _, err := s.q.ExecContext(ctx, `INSERT INTO accounts(name, rclone_remote, created_at) VALUES (?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET rclone_remote = excluded.rclone_remote`, name, nullStr(remote), Now()); err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, name)
}

// GetAccount looks an account up by name.
func (s *Store) GetAccount(ctx context.Context, name string) (*Account, error) {
	var a Account
	var remote sql.NullString
	err := s.q.QueryRowContext(ctx, `SELECT id, name, rclone_remote FROM accounts WHERE name = ?`, name).Scan(&a.ID, &a.Name, &remote)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a.RcloneRemote = remote.String
	return &a, nil
}

// ---- exports and archives -------------------------------------------------

// Export tracks completeness of one Takeout export (all its parts).
type Export struct {
	AccountID      int64
	ExportID       string
	HighestPart    int
	ConfirmedParts int
	LastPartSeenAt string
	CompleteAt     string
	Authoritative  bool
	AlbumsPrunedAt string
}

// TouchExport records that a part of the export was seen.
func (s *Store) TouchExport(ctx context.Context, accountID int64, exportID string, partNo int) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO exports(account_id, export_id, highest_part, last_part_seen_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(account_id, export_id) DO UPDATE SET
		  highest_part = MAX(highest_part, excluded.highest_part),
		  last_part_seen_at = excluded.last_part_seen_at`, accountID, exportID, partNo, Now())
	return err
}

// GetExport returns an export row or nil.
func (s *Store) GetExport(ctx context.Context, accountID int64, exportID string) (*Export, error) {
	var e Export
	var confirmed sql.NullInt64
	var last, complete, pruned sql.NullString
	err := s.q.QueryRowContext(ctx, `SELECT account_id, export_id, highest_part, confirmed_parts, last_part_seen_at, complete_at, authoritative, albums_pruned_at
		FROM exports WHERE account_id = ? AND export_id = ?`, accountID, exportID).
		Scan(&e.AccountID, &e.ExportID, &e.HighestPart, &confirmed, &last, &complete, &e.Authoritative, &pruned)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	e.ConfirmedParts = int(confirmed.Int64)
	e.LastPartSeenAt, e.CompleteAt, e.AlbumsPrunedAt = last.String, complete.String, pruned.String
	return &e, nil
}

// ListExports returns the exports of an account, newest first.
func (s *Store) ListExports(ctx context.Context, accountID int64) ([]Export, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT account_id, export_id, highest_part, confirmed_parts, last_part_seen_at, complete_at, authoritative, albums_pruned_at
		FROM exports WHERE account_id = ? ORDER BY export_id DESC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Export
	for rows.Next() {
		var e Export
		var confirmed sql.NullInt64
		var last, complete, pruned sql.NullString
		if err := rows.Scan(&e.AccountID, &e.ExportID, &e.HighestPart, &confirmed, &last, &complete, &e.Authoritative, &pruned); err != nil {
			return nil, err
		}
		e.ConfirmedParts = int(confirmed.Int64)
		e.LastPartSeenAt, e.CompleteAt, e.AlbumsPrunedAt = last.String, complete.String, pruned.String
		out = append(out, e)
	}
	return out, rows.Err()
}

// SetExportConfirmedParts records the user-confirmed part count.
func (s *Store) SetExportConfirmedParts(ctx context.Context, accountID int64, exportID string, parts int, authoritative bool) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO exports(account_id, export_id, confirmed_parts, authoritative) VALUES (?, ?, ?, ?)
		ON CONFLICT(account_id, export_id) DO UPDATE SET confirmed_parts = excluded.confirmed_parts,
		  authoritative = MAX(authoritative, excluded.authoritative)`, accountID, exportID, parts, authoritative)
	return err
}

// CountExports returns how many exports an account has.
func (s *Store) CountExports(ctx context.Context, accountID int64) (int, error) {
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM exports WHERE account_id = ?`, accountID).Scan(&n)
	return n, err
}

// Archive is one part of an export.
type Archive struct {
	ID              int64
	AccountID       int64
	RemoteID        string
	Name            string
	ExportID        string
	PartNo          int
	Size            int64
	MD5             string
	Status          string
	LocalPath       string
	RemoteDeletedAt string // when the Drive copy was confirmed deleted ("" while still owed)
	Entries         int
	Ingested        int
	Duplicates      int
	Errors          int
}

// Archive statuses.
const (
	ArchiveSeen       = "seen"
	ArchiveFetching   = "fetching"
	ArchiveIngesting  = "ingesting"
	ArchiveIngested   = "ingested"
	ArchivePartial    = "partial"
	ArchiveWithErrors = "ingested_with_errors"
	ArchiveFailed     = "failed"
)

// UpsertArchive creates or updates the archive row identified by (account, export, part).
func (s *Store) UpsertArchive(ctx context.Context, a *Archive) (*Archive, error) {
	_, err := s.q.ExecContext(ctx, `INSERT INTO archives(account_id, remote_id, name, export_id, part_no, size, md5, status, local_path, started_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(account_id, export_id, part_no) DO UPDATE SET
		  -- a row written for a listed Drive file (remote_id given) records that file exactly:
		  -- its size and hash as listed (the hash may be unknown), and its deletion still owed;
		  -- a local ingest (no remote_id) keeps what is known
		  remote_deleted_at = CASE WHEN excluded.remote_id IS NOT NULL THEN NULL ELSE remote_deleted_at END,
		  size = CASE WHEN excluded.remote_id IS NOT NULL THEN excluded.size ELSE COALESCE(excluded.size, size) END,
		  md5 = CASE WHEN excluded.remote_id IS NOT NULL THEN excluded.md5 ELSE COALESCE(excluded.md5, md5) END,
		  remote_id = COALESCE(excluded.remote_id, remote_id), name = excluded.name,
		  status = excluded.status, local_path = COALESCE(excluded.local_path, local_path),
		  started_at = COALESCE(started_at, excluded.started_at)`,
		a.AccountID, nullStr(a.RemoteID), a.Name, a.ExportID, a.PartNo, nullInt(a.Size), nullStr(a.MD5), a.Status, nullStr(a.LocalPath), Now())
	if err != nil {
		return nil, err
	}
	return s.GetArchive(ctx, a.AccountID, a.ExportID, a.PartNo)
}

const archiveCols = `id, account_id, remote_id, name, export_id, part_no, size, md5, status, local_path, remote_deleted_at, entries, ingested, duplicates, errors`

func scanArchive(sc interface{ Scan(...any) error }) (*Archive, error) {
	var a Archive
	var remote, md5, local, deleted sql.NullString
	var size sql.NullInt64
	if err := sc.Scan(&a.ID, &a.AccountID, &remote, &a.Name, &a.ExportID, &a.PartNo, &size, &md5, &a.Status, &local, &deleted, &a.Entries, &a.Ingested, &a.Duplicates, &a.Errors); err != nil {
		return nil, err
	}
	a.RemoteID, a.MD5, a.LocalPath, a.Size, a.RemoteDeletedAt = remote.String, md5.String, local.String, size.Int64, deleted.String
	return &a, nil
}

// GetArchive returns an archive row or nil.
func (s *Store) GetArchive(ctx context.Context, accountID int64, exportID string, partNo int) (*Archive, error) {
	a, err := scanArchive(s.q.QueryRowContext(ctx, `SELECT `+archiveCols+` FROM archives WHERE account_id = ? AND export_id = ? AND part_no = ?`, accountID, exportID, partNo))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return a, err
}

// GetArchiveByRemoteID returns an account's archive row for a Drive file id,
// or nil. Another account's row for the same id is never this account's ingest.
func (s *Store) GetArchiveByRemoteID(ctx context.Context, accountID int64, remoteID string) (*Archive, error) {
	a, err := scanArchive(s.q.QueryRowContext(ctx, `SELECT `+archiveCols+` FROM archives WHERE account_id = ? AND remote_id = ?`, accountID, remoteID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return a, err
}

// ArchiveByRemoteIDAnyAccount returns whichever account's archive row records
// a Drive file id (remote ids are unique across the table), or nil.
func (s *Store) ArchiveByRemoteIDAnyAccount(ctx context.Context, remoteID string) (*Archive, error) {
	a, err := scanArchive(s.q.QueryRowContext(ctx, `SELECT `+archiveCols+` FROM archives WHERE remote_id = ?`, remoteID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return a, err
}

// NewestPrunedExport returns the newest export whose complete album listing
// was applied to the account ("" if none).
func (s *Store) NewestPrunedExport(ctx context.Context, accountID int64) (string, error) {
	var v string
	err := s.q.QueryRowContext(ctx, `SELECT export_id FROM exports WHERE account_id = ? AND albums_pruned_at IS NOT NULL ORDER BY `+exportOrderDesc+` LIMIT 1`, accountID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// ListArchives returns an account's archives ordered by export and part.
func (s *Store) ListArchives(ctx context.Context, accountID int64) ([]*Archive, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+archiveCols+` FROM archives WHERE account_id = ? ORDER BY export_id, part_no`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Archive
	for rows.Next() {
		a, err := scanArchive(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// FinishArchive sets the final status and counters of a part.
func (s *Store) FinishArchive(ctx context.Context, id int64, status string, entries, ingested, duplicates, errs int) error {
	_, err := s.q.ExecContext(ctx, `UPDATE archives SET status = ?, entries = ?, ingested = ?, duplicates = ?, errors = ?, finished_at = ? WHERE id = ?`,
		status, entries, ingested, duplicates, errs, Now(), id)
	return err
}

// SetArchiveStatus updates only the status (and optionally local path).
func (s *Store) SetArchiveStatus(ctx context.Context, id int64, status, localPath string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE archives SET status = ?, local_path = COALESCE(?, local_path) WHERE id = ?`, status, nullStr(localPath), id)
	return err
}

// SetArchiveRemote records the Drive file an already ingested part was
// matched to by content (a re-upload with a new id).
func (s *Store) SetArchiveRemote(ctx context.Context, id int64, remoteID string, size int64, md5 string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE archives SET remote_id = ?, size = COALESCE(?, size), md5 = COALESCE(?, md5), remote_deleted_at = NULL WHERE id = ?`,
		nullStr(remoteID), nullInt(size), nullStr(md5), id)
	return err
}

// exportOrderDesc orders export ids (<timestamp>-<sequence>) newest first, by
// timestamp then numeric sequence, for a column named export_id.
const exportOrderDesc = `substr(export_id, 1, instr(export_id, '-') - 1) DESC, CAST(substr(export_id, instr(export_id, '-') + 1) AS INTEGER) DESC`

// ForgetArchiveSource drops what an archive row knows about the file it was
// ingested from (Drive id, hash, size, deletion state): a local replacement of
// unknown identity must not let a later sync match the old Drive file to it.
func (s *Store) ForgetArchiveSource(ctx context.Context, id int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE archives SET remote_id = NULL, md5 = NULL, size = NULL, remote_deleted_at = NULL WHERE id = ?`, id)
	return err
}

// ArchivesAwaitingRemoteDelete lists ingested parts fetched from Drive whose
// remote copy has not been confirmed deleted.
func (s *Store) ArchivesAwaitingRemoteDelete(ctx context.Context, accountID int64) ([]*Archive, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+archiveCols+` FROM archives WHERE account_id = ? AND status = ? AND remote_id IS NOT NULL AND remote_deleted_at IS NULL ORDER BY export_id, part_no`, accountID, ArchiveIngested)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Archive
	for rows.Next() {
		a, err := scanArchive(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ResetPartForReingest prepares a part whose file on Drive changed to be
// ingested from scratch: the per-entry bookkeeping and the sidecars it stored
// for other parts are dropped, the part's
// occurrences are deactivated (the re-ingest reactivates those still present),
// album memberships confirmed only by this export's vanished occurrences fall
// back to the newest remaining evidence, and the export's album listing
// becomes eligible for pruning again. Direct memberships left without any
// active occurrence are removed by the ingest once the part is complete
// (Ingester.ReconcileMemberships).
func (s *Store) ResetPartForReingest(ctx context.Context, archiveID, accountID int64, exportID string, partNo int) error {
	return s.Tx(ctx, func(tx *Store) error {
		q := tx.q
		if _, err := q.ExecContext(ctx, `DELETE FROM archive_entries WHERE archive_id = ?`, archiveID); err != nil {
			return err
		}
		// state 'reset' marks them as waiting for the replacement: the re-ingest
		// overwrites the state of those still present, ClearResetState the rest
		if _, err := q.ExecContext(ctx, `UPDATE occurrences SET active = 0, state = ? WHERE account_id = ? AND export_id = ? AND part_no = ?`, OccReset, accountID, exportID, partNo); err != nil {
			return err
		}
		// sidecars this part stored for media in other parts: the replacement
		// stores its own again. Rows from before migration 0003 whose source part
		// could not be backfilled (NULL) may be this part's, so they go too: a
		// stale sidecar handing its identity to the wrong media is worse than a
		// sidecar missing until a later export resends it.
		if _, err := q.ExecContext(ctx, `DELETE FROM pending_sidecars WHERE account_id = ? AND export_id = ? AND (part_no = ? OR part_no IS NULL)`, accountID, exportID, partNo); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `UPDATE album_items SET confirmed_export_id = (
				SELECT o.export_id FROM occurrences o WHERE o.album_id = album_items.album_id AND o.item_id = album_items.item_id AND o.active = 1
				ORDER BY `+exportOrderDesc+` LIMIT 1)
			WHERE confirmed_export_id = ?
			  AND album_id IN (SELECT id FROM albums WHERE account_id = ?)
			  AND NOT EXISTS (SELECT 1 FROM occurrences o WHERE o.album_id = album_items.album_id AND o.item_id = album_items.item_id
			                  AND o.export_id = ? AND o.active = 1)`, exportID, accountID, exportID); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `DELETE FROM albums_full WHERE listing_export_id = ? AND album_id IN (SELECT id FROM albums WHERE account_id = ?)`, exportID, accountID); err != nil {
			return err
		}
		_, err := q.ExecContext(ctx, `UPDATE exports SET albums_pruned_at = NULL WHERE account_id = ? AND export_id = ?`, accountID, exportID)
		return err
	})
}

// SetArchiveRemoteDeleted records a successful remote deletion.
func (s *Store) SetArchiveRemoteDeleted(ctx context.Context, id int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE archives SET remote_deleted_at = ? WHERE id = ?`, Now(), id)
	return err
}

// EntryState is the per-entry bookkeeping inside a part.
type EntryState struct {
	Status   string
	Attempts int
	LastErr  string
}

// Entry statuses.
const (
	EntryDone    = "done"
	EntrySkipped = "skipped"
	EntryFailed  = "failed"
	EntryGivenUp = "given_up"
)

// GetEntry returns the state of one entry or nil.
func (s *Store) GetEntry(ctx context.Context, archiveID int64, name string) (*EntryState, error) {
	var e EntryState
	var last sql.NullString
	err := s.q.QueryRowContext(ctx, `SELECT status, attempts, last_error FROM archive_entries WHERE archive_id = ? AND entry_name = ?`, archiveID, name).Scan(&e.Status, &e.Attempts, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	e.LastErr = last.String
	return &e, nil
}

// DoneEntries returns the names of entries already done or skipped in a part.
func (s *Store) DoneEntries(ctx context.Context, archiveID int64) (map[string]bool, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT entry_name FROM archive_entries WHERE archive_id = ? AND status IN ('done','skipped')`, archiveID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out[n] = true
	}
	return out, rows.Err()
}

// MarkEntry records an entry's status.
func (s *Store) MarkEntry(ctx context.Context, archiveID int64, name, status string, attempts int, lastErr string) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO archive_entries(archive_id, entry_name, status, attempts, last_error) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(archive_id, entry_name) DO UPDATE SET status = excluded.status, attempts = excluded.attempts, last_error = excluded.last_error`,
		archiveID, name, status, attempts, nullStr(lastErr))
	return err
}

// CountEntries returns the number of entries in each status for a part.
func (s *Store) CountEntries(ctx context.Context, archiveID int64) (map[string]int, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT status, COUNT(*) FROM archive_entries WHERE archive_id = ? GROUP BY status`, archiveID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// ListFailedEntries returns failed or given-up entries across an account.
func (s *Store) ListFailedEntries(ctx context.Context, accountID int64, statuses []string) ([]FailedEntry, error) {
	q := `SELECT a.name, e.entry_name, e.status, e.attempts, COALESCE(e.last_error,'') FROM archive_entries e JOIN archives a ON a.id = e.archive_id
		WHERE a.account_id = ? AND e.status IN (`
	args := []any{accountID}
	for i, st := range statuses {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, st)
	}
	q += `) ORDER BY a.name, e.entry_name`
	rows, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FailedEntry
	for rows.Next() {
		var f FailedEntry
		if err := rows.Scan(&f.Archive, &f.Entry, &f.Status, &f.Attempts, &f.LastErr); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// FailedEntry is a row of ListFailedEntries.
type FailedEntry struct {
	Archive, Entry, Status, LastErr string
	Attempts                        int
}

// ResetGivenUp turns given-up entries of a part (or all parts when archiveID is 0) back into failed.
func (s *Store) ResetGivenUp(ctx context.Context, accountID int64, archiveName string) (int64, error) {
	q := `UPDATE archive_entries SET status = 'failed', attempts = 0 WHERE status = 'given_up' AND archive_id IN (SELECT id FROM archives WHERE account_id = ?`
	args := []any{accountID}
	if archiveName != "" {
		q += ` AND name = ?`
		args = append(args, archiveName)
	}
	q += `)`
	res, err := s.q.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		q2 := `UPDATE archives SET status = 'partial' WHERE status = 'ingested_with_errors' AND account_id = ?`
		args2 := []any{accountID}
		if archiveName != "" {
			q2 += ` AND name = ?`
			args2 = append(args2, archiveName)
		}
		if _, err := s.q.ExecContext(ctx, q2, args2...); err != nil {
			return n, err
		}
	}
	return n, nil
}

// ---- blobs ----------------------------------------------------------------

// Blob is one distinct content.
type Blob struct {
	ID            int64
	SHA256        string
	Size          int64
	CanonicalPath string
}

// FindBlob returns the blob with the given hash or nil.
func (s *Store) FindBlob(ctx context.Context, sha string) (*Blob, error) {
	var b Blob
	err := s.q.QueryRowContext(ctx, `SELECT id, sha256, size, canonical_path FROM blobs WHERE sha256 = ?`, sha).Scan(&b.ID, &b.SHA256, &b.Size, &b.CanonicalPath)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// GetBlob returns a blob by id.
func (s *Store) GetBlob(ctx context.Context, id int64) (*Blob, error) {
	var b Blob
	err := s.q.QueryRowContext(ctx, `SELECT id, sha256, size, canonical_path FROM blobs WHERE id = ?`, id).Scan(&b.ID, &b.SHA256, &b.Size, &b.CanonicalPath)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &b, err
}

// InsertBlob creates a blob.
func (s *Store) InsertBlob(ctx context.Context, sha string, size int64, canonical string) (*Blob, error) {
	res, err := s.q.ExecContext(ctx, `INSERT INTO blobs(sha256, size, canonical_path, first_seen) VALUES (?, ?, ?, ?)`, sha, size, canonical, Now())
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Blob{ID: id, SHA256: sha, Size: size, CanonicalPath: canonical}, nil
}

// SetBlobCanonical re-points a blob's canonical path.
func (s *Store) SetBlobCanonical(ctx context.Context, id int64, path string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE blobs SET canonical_path = ? WHERE id = ?`, path, id)
	return err
}

// DeleteBlob removes a blob row.
func (s *Store) DeleteBlob(ctx context.Context, id int64) error {
	_, err := s.q.ExecContext(ctx, `DELETE FROM blobs WHERE id = ?`, id)
	return err
}

// BlobRefPaths returns every path (item or kept version) referencing a blob, excluding one.
func (s *Store) BlobRefPaths(ctx context.Context, blobID int64, exclude string) ([]string, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT lib_path FROM items WHERE blob_id = ? AND lib_path != ?
		UNION ALL SELECT path FROM item_versions WHERE blob_id = ? AND path != ?`, blobID, exclude, blobID, exclude)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---- items ----------------------------------------------------------------

// Item is one logical photo or video in one account.
type Item struct {
	ID               int64
	AccountID        int64
	BlobID           int64
	GoogleID         string
	Variant          string
	IdentityKey      string
	IdentitySrc      string
	Filename         string
	Ext              string
	Mime             string
	Width            int
	Height           int
	TakenAt          time.Time
	TakenSrc         string
	LibPath          string
	GroupID          string
	ArchivedExportID string
	BytesExportID    string // export that last supplied the bytes at lib_path
	FirstSeen        string
	LastSeen         string
}

// Identity sources.
const (
	IdentityGoogle   = "google_id"
	IdentityFallback = "fallback"
)

const itemCols = `id, account_id, blob_id, google_id, variant, identity_key, identity_src, filename, ext, mime, width, height, taken_at, taken_src, lib_path, group_id, archived_export_id, bytes_export_id, first_seen, last_seen`

func scanItem(sc interface{ Scan(...any) error }) (*Item, error) {
	var it Item
	var gid, mime, taken, takenSrc, archived, bytesExp sql.NullString
	var w, h sql.NullInt64
	if err := sc.Scan(&it.ID, &it.AccountID, &it.BlobID, &gid, &it.Variant, &it.IdentityKey, &it.IdentitySrc, &it.Filename, &it.Ext, &mime, &w, &h, &taken, &takenSrc, &it.LibPath, &it.GroupID, &archived, &bytesExp, &it.FirstSeen, &it.LastSeen); err != nil {
		return nil, err
	}
	it.GoogleID, it.Mime, it.TakenSrc, it.ArchivedExportID, it.BytesExportID = gid.String, mime.String, takenSrc.String, archived.String, bytesExp.String
	it.Width, it.Height = int(w.Int64), int(h.Int64)
	it.TakenAt = ParseTime(taken.String)
	return &it, nil
}

func (s *Store) queryItems(ctx context.Context, where string, args ...any) ([]*Item, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+itemCols+` FROM items WHERE `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Item
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// GetItem returns an item by id or nil.
func (s *Store) GetItem(ctx context.Context, id int64) (*Item, error) {
	it, err := scanItem(s.q.QueryRowContext(ctx, `SELECT `+itemCols+` FROM items WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return it, err
}

// FindItemByKey returns the item with an identity key in an account, or nil.
func (s *Store) FindItemByKey(ctx context.Context, accountID int64, key string) (*Item, error) {
	it, err := scanItem(s.q.QueryRowContext(ctx, `SELECT `+itemCols+` FROM items WHERE account_id = ? AND identity_key = ?`, accountID, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return it, err
}

// FindFallbackItems returns fallback-identity items matching content, name and variant.
func (s *Store) FindFallbackItems(ctx context.Context, accountID int64, sha, filename, variant string) ([]*Item, error) {
	return s.queryItems(ctx, `account_id = ? AND identity_src = 'fallback' AND filename = ? AND variant = ? AND blob_id IN (SELECT id FROM blobs WHERE sha256 = ?)`,
		accountID, filename, variant, sha)
}

// ItemsByGroup returns all items of a companion group in an account.
func (s *Store) ItemsByGroup(ctx context.Context, accountID int64, groupID string) ([]*Item, error) {
	return s.queryItems(ctx, `account_id = ? AND group_id = ? ORDER BY id`, accountID, groupID)
}

// ItemsByGoogleID returns the items sharing a Google id in an account.
func (s *Store) ItemsByGoogleID(ctx context.Context, accountID int64, googleID string) ([]*Item, error) {
	return s.queryItems(ctx, `account_id = ? AND google_id = ? ORDER BY id`, accountID, googleID)
}

// ListItems returns all items of an account (for verify/relink), ordered by path.
func (s *Store) ListItems(ctx context.Context, accountID int64) ([]*Item, error) {
	return s.queryItems(ctx, `account_id = ? ORDER BY lib_path`, accountID)
}

// InsertItem creates an item and returns its id.
func (s *Store) InsertItem(ctx context.Context, it *Item) (int64, error) {
	now := Now()
	res, err := s.q.ExecContext(ctx, `INSERT INTO items(account_id, blob_id, google_id, variant, identity_key, identity_src, filename, ext, mime, width, height, taken_at, taken_src, lib_path, group_id, archived_export_id, bytes_export_id, first_seen, last_seen)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		it.AccountID, it.BlobID, nullStr(it.GoogleID), it.Variant, it.IdentityKey, it.IdentitySrc, it.Filename, it.Ext, nullStr(it.Mime),
		nullInt(int64(it.Width)), nullInt(int64(it.Height)), nullStr(FormatTime(it.TakenAt)), nullStr(it.TakenSrc), it.LibPath, it.GroupID, nullStr(it.ArchivedExportID), nullStr(it.BytesExportID), now, now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// UpdateItem rewrites the mutable fields of an item.
func (s *Store) UpdateItem(ctx context.Context, it *Item) error {
	_, err := s.q.ExecContext(ctx, `UPDATE items SET blob_id = ?, google_id = ?, variant = ?, identity_key = ?, identity_src = ?, filename = ?, ext = ?, mime = ?,
		width = ?, height = ?, taken_at = ?, taken_src = ?, lib_path = ?, group_id = ?, archived_export_id = ?, bytes_export_id = ?, last_seen = ? WHERE id = ?`,
		it.BlobID, nullStr(it.GoogleID), it.Variant, it.IdentityKey, it.IdentitySrc, it.Filename, it.Ext, nullStr(it.Mime),
		nullInt(int64(it.Width)), nullInt(int64(it.Height)), nullStr(FormatTime(it.TakenAt)), nullStr(it.TakenSrc), it.LibPath, it.GroupID, nullStr(it.ArchivedExportID), nullStr(it.BytesExportID), Now(), it.ID)
	return err
}

// TouchItem updates last_seen.
func (s *Store) TouchItem(ctx context.Context, id int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE items SET last_seen = ? WHERE id = ?`, Now(), id)
	return err
}

// DeleteItem removes an item and its sidecar row.
func (s *Store) DeleteItem(ctx context.Context, id int64) error {
	if _, err := s.q.ExecContext(ctx, `DELETE FROM sidecars WHERE item_id = ?`, id); err != nil {
		return err
	}
	_, err := s.q.ExecContext(ctx, `DELETE FROM items WHERE id = ?`, id)
	return err
}

// LibPathTaken reports whether a library path is used by an item other than exceptID.
func (s *Store) LibPathTaken(ctx context.Context, path string, exceptID int64) (bool, error) {
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM items WHERE lib_path = ? AND id != ?`, path, exceptID).Scan(&n)
	return n > 0, err
}

// VersionPathTaken reports whether a path is used by a kept version.
func (s *Store) VersionPathTaken(ctx context.Context, path string) (bool, error) {
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM item_versions WHERE path = ?`, path).Scan(&n)
	return n > 0, err
}

// InsertVersion records a kept superseded file.
func (s *Store) InsertVersion(ctx context.Context, itemID, blobID int64, path, exportID string) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO item_versions(item_id, blob_id, path, superseded_at, by_export_id) VALUES (?, ?, ?, ?, ?)`, itemID, blobID, path, Now(), nullStr(exportID))
	return err
}

// ---- sidecars -------------------------------------------------------------

// SidecarRow is the stored sidecar of an item.
type SidecarRow struct {
	ItemID   int64
	JSON     []byte
	SHA256   string
	ExportID string // export that supplied this sidecar
	PartNo   int    // part of that export that supplied it (0 when unknown)
}

// GetSidecar returns an item's stored sidecar or nil.
func (s *Store) GetSidecar(ctx context.Context, itemID int64) (*SidecarRow, error) {
	var r SidecarRow
	var exp sql.NullString
	var part sql.NullInt64
	err := s.q.QueryRowContext(ctx, `SELECT item_id, json, sha256, export_id, part_no FROM sidecars WHERE item_id = ?`, itemID).Scan(&r.ItemID, &r.JSON, &r.SHA256, &exp, &part)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	r.ExportID, r.PartNo = exp.String, int(part.Int64)
	return &r, err
}

// SetSidecarExport records that a newer export (part) carries the same sidecar.
func (s *Store) SetSidecarExport(ctx context.Context, itemID int64, exportID string, partNo int) error {
	_, err := s.q.ExecContext(ctx, `UPDATE sidecars SET export_id = ?, part_no = ? WHERE item_id = ?`, nullStr(exportID), nullInt(int64(partNo)), itemID)
	return err
}

// UpsertSidecar stores an item's sidecar and the extracted fields.
func (s *Store) UpsertSidecar(ctx context.Context, itemID int64, raw []byte, sha, description string, favorited bool, lat, lon, alt *float64, people, exportID string, partNo int) error {
	var la, lo, al any
	if lat != nil {
		la, lo, al = *lat, *lon, *alt
	}
	_, err := s.q.ExecContext(ctx, `INSERT INTO sidecars(item_id, json, sha256, description, favorited, lat, lon, alt, people, export_id, part_no) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(item_id) DO UPDATE SET json = excluded.json, sha256 = excluded.sha256, description = excluded.description, favorited = excluded.favorited,
		  lat = excluded.lat, lon = excluded.lon, alt = excluded.alt, people = excluded.people, export_id = excluded.export_id, part_no = excluded.part_no`,
		itemID, raw, sha, nullStr(description), favorited, la, lo, al, nullStr(people), nullStr(exportID), nullInt(int64(partNo)))
	return err
}

// ---- albums ---------------------------------------------------------------

// Album is one album folder of one account.
type Album struct {
	ID            int64
	AccountID     int64
	Title         string
	AlbumDate     string
	TakeoutFolder string
	Kind          string
	Path          string
	HasMeta       bool
}

const albumCols = `id, account_id, title, album_date, takeout_folder, kind, path, has_meta`

func scanAlbum(sc interface{ Scan(...any) error }) (*Album, error) {
	var a Album
	if err := sc.Scan(&a.ID, &a.AccountID, &a.Title, &a.AlbumDate, &a.TakeoutFolder, &a.Kind, &a.Path, &a.HasMeta); err != nil {
		return nil, err
	}
	return &a, nil
}

// AlbumsByFolder returns albums of an account that were exported under a Takeout folder name.
func (s *Store) AlbumsByFolder(ctx context.Context, accountID int64, folder string) ([]*Album, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+albumCols+` FROM albums WHERE account_id = ? AND takeout_folder = ? ORDER BY id`, accountID, folder)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Album
	for rows.Next() {
		a, err := scanAlbum(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// GetAlbum returns an album by id or nil.
func (s *Store) GetAlbum(ctx context.Context, id int64) (*Album, error) {
	a, err := scanAlbum(s.q.QueryRowContext(ctx, `SELECT `+albumCols+` FROM albums WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return a, err
}

// ListAlbums returns all albums of an account.
func (s *Store) ListAlbums(ctx context.Context, accountID int64) ([]*Album, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+albumCols+` FROM albums WHERE account_id = ? ORDER BY path`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Album
	for rows.Next() {
		a, err := scanAlbum(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AlbumPathTaken reports whether an album path is in use.
func (s *Store) AlbumPathTaken(ctx context.Context, path string) (bool, error) {
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM albums WHERE path = ?`, path).Scan(&n)
	return n > 0, err
}

// InsertAlbum creates an album.
func (s *Store) InsertAlbum(ctx context.Context, a *Album, metaJSON []byte) (int64, error) {
	now := Now()
	res, err := s.q.ExecContext(ctx, `INSERT INTO albums(account_id, title, album_date, takeout_folder, kind, path, has_meta, metadata_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.AccountID, a.Title, a.AlbumDate, a.TakeoutFolder, a.Kind, a.Path, a.HasMeta, metaJSON, now, now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// UpdateAlbum rewrites an album's descriptor fields and path.
func (s *Store) UpdateAlbum(ctx context.Context, a *Album, metaJSON []byte) error {
	_, err := s.q.ExecContext(ctx, `UPDATE albums SET title = ?, album_date = ?, takeout_folder = ?, kind = ?, path = ?, has_meta = ?, metadata_json = COALESCE(?, metadata_json), updated_at = ? WHERE id = ?`,
		a.Title, a.AlbumDate, a.TakeoutFolder, a.Kind, a.Path, a.HasMeta, metaJSON, Now(), a.ID)
	return err
}

// AlbumItem is the membership row of (album, item).
type AlbumItem struct {
	AlbumID           int64
	ItemID            int64
	LinkPath          string
	Direct            bool
	ConfirmedExportID string
	ConfirmedBy       string
	ViaGroup          bool
	GroupStill        int64
}

const albumItemCols = `album_id, item_id, link_path, direct, confirmed_export_id, confirmed_by, via_group, group_still`

func scanAlbumItem(sc interface{ Scan(...any) error }) (*AlbumItem, error) {
	var ai AlbumItem
	var exp, by sql.NullString
	var still sql.NullInt64
	if err := sc.Scan(&ai.AlbumID, &ai.ItemID, &ai.LinkPath, &ai.Direct, &exp, &by, &ai.ViaGroup, &still); err != nil {
		return nil, err
	}
	ai.ConfirmedExportID, ai.ConfirmedBy, ai.GroupStill = exp.String, by.String, still.Int64
	return &ai, nil
}

// GetAlbumItem returns a membership row or nil.
func (s *Store) GetAlbumItem(ctx context.Context, albumID, itemID int64) (*AlbumItem, error) {
	ai, err := scanAlbumItem(s.q.QueryRowContext(ctx, `SELECT `+albumItemCols+` FROM album_items WHERE album_id = ? AND item_id = ?`, albumID, itemID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return ai, err
}

func (s *Store) queryAlbumItems(ctx context.Context, where string, args ...any) ([]*AlbumItem, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+albumItemCols+` FROM album_items WHERE `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AlbumItem
	for rows.Next() {
		ai, err := scanAlbumItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ai)
	}
	return out, rows.Err()
}

// AlbumItemsByItem returns all memberships of an item.
func (s *Store) AlbumItemsByItem(ctx context.Context, itemID int64) ([]*AlbumItem, error) {
	return s.queryAlbumItems(ctx, `item_id = ? ORDER BY album_id`, itemID)
}

// UnsupportedDirectMemberships returns an account's direct memberships that
// no active occurrence supports any more (after a replaced part was ingested
// again without the item in that album). A membership with an occurrence
// deactivated by a replacement still in progress (state 'reset') is left alone
// until that part completes; completingArchive is the part finishing right now.
func (s *Store) UnsupportedDirectMemberships(ctx context.Context, accountID, completingArchive int64) ([]*AlbumItem, error) {
	return s.queryAlbumItems(ctx, `direct = 1 AND album_id IN (SELECT id FROM albums WHERE account_id = ?)
		AND NOT EXISTS (SELECT 1 FROM occurrences o WHERE o.album_id = album_items.album_id AND o.item_id = album_items.item_id AND o.active = 1)
		AND NOT EXISTS (SELECT 1 FROM occurrences o JOIN archives a ON a.account_id = o.account_id AND a.export_id = o.export_id AND a.part_no = o.part_no
		                WHERE o.album_id = album_items.album_id AND o.item_id = album_items.item_id AND o.state = ? AND a.id != ?)
		ORDER BY album_id, item_id`, accountID, OccReset, completingArchive)
}

// ClearResetState ends the wait for a part's replacement: occurrences it
// dropped stay inactive but no longer hold reconciliation back.
func (s *Store) ClearResetState(ctx context.Context, accountID int64, exportID string, partNo int) error {
	_, err := s.q.ExecContext(ctx, `UPDATE occurrences SET state = ? WHERE account_id = ? AND export_id = ? AND part_no = ? AND state = ?`, OccIngested, accountID, exportID, partNo, OccReset)
	return err
}

// AlbumItemsByAlbum returns all memberships of an album.
func (s *Store) AlbumItemsByAlbum(ctx context.Context, albumID int64) ([]*AlbumItem, error) {
	return s.queryAlbumItems(ctx, `album_id = ? ORDER BY item_id`, albumID)
}

// UpsertAlbumItem writes a membership row.
func (s *Store) UpsertAlbumItem(ctx context.Context, ai *AlbumItem) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO album_items(album_id, item_id, link_path, direct, confirmed_export_id, confirmed_by, via_group, group_still)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(album_id, item_id) DO UPDATE SET link_path = excluded.link_path, direct = excluded.direct,
		  confirmed_export_id = excluded.confirmed_export_id, confirmed_by = excluded.confirmed_by, via_group = excluded.via_group, group_still = excluded.group_still`,
		ai.AlbumID, ai.ItemID, ai.LinkPath, ai.Direct, nullStr(ai.ConfirmedExportID), nullStr(ai.ConfirmedBy), ai.ViaGroup, nullInt(ai.GroupStill))
	return err
}

// DeleteAlbumItem removes a membership row.
func (s *Store) DeleteAlbumItem(ctx context.Context, albumID, itemID int64) error {
	_, err := s.q.ExecContext(ctx, `DELETE FROM album_items WHERE album_id = ? AND item_id = ?`, albumID, itemID)
	return err
}

// LinkPathTaken reports whether an album link path is used by another (album,item).
func (s *Store) LinkPathTaken(ctx context.Context, path string, albumID, itemID int64) (bool, error) {
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM album_items WHERE link_path = ? AND NOT (album_id = ? AND item_id = ?)`, path, albumID, itemID).Scan(&n)
	return n > 0, err
}

// ---- occurrences ----------------------------------------------------------

// Occurrence is one media entry seen in one part.
type Occurrence struct {
	ID        int64
	AccountID int64
	ExportID  string
	PartNo    int
	Folder    string
	EntryName string
	Variant   string
	SHA256    string
	AlbumID   int64
	ItemID    int64
	State     string
	Active    bool
}

// Occurrence states.
const (
	OccWaiting  = "waiting"
	OccIngested = "ingested"
	OccResolved = "resolved"
	OccReset    = "reset" // deactivated by a replacement of its part that is still being ingested
)

const occCols = `id, account_id, export_id, part_no, folder, entry_name, variant, sha256, album_id, item_id, state, active`

func scanOcc(sc interface{ Scan(...any) error }) (*Occurrence, error) {
	var o Occurrence
	var sha sql.NullString
	var album, item sql.NullInt64
	if err := sc.Scan(&o.ID, &o.AccountID, &o.ExportID, &o.PartNo, &o.Folder, &o.EntryName, &o.Variant, &sha, &album, &item, &o.State, &o.Active); err != nil {
		return nil, err
	}
	o.SHA256, o.AlbumID, o.ItemID = sha.String, album.Int64, item.Int64
	return &o, nil
}

// UpsertOccurrence writes an occurrence (keyed by account, export, folder, entry).
func (s *Store) UpsertOccurrence(ctx context.Context, o *Occurrence) (int64, error) {
	if _, err := s.q.ExecContext(ctx, `INSERT INTO occurrences(account_id, export_id, part_no, folder, entry_name, variant, sha256, album_id, item_id, state, active)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		ON CONFLICT(account_id, export_id, folder, entry_name) DO UPDATE SET part_no = excluded.part_no, variant = excluded.variant,
		  sha256 = COALESCE(excluded.sha256, sha256), album_id = COALESCE(excluded.album_id, album_id), item_id = COALESCE(excluded.item_id, item_id), state = excluded.state,
		  active = 1`,
		o.AccountID, o.ExportID, o.PartNo, o.Folder, o.EntryName, o.Variant, nullStr(o.SHA256), nullInt(o.AlbumID), nullInt(o.ItemID), o.State); err != nil {
		return 0, err
	}
	var id int64
	err := s.q.QueryRowContext(ctx, `SELECT id FROM occurrences WHERE account_id = ? AND export_id = ? AND folder = ? AND entry_name = ?`, o.AccountID, o.ExportID, o.Folder, o.EntryName).Scan(&id)
	return id, err
}

// FindOccurrence returns an occurrence by key or nil.
func (s *Store) FindOccurrence(ctx context.Context, accountID int64, exportID, folder, entry string) (*Occurrence, error) {
	o, err := scanOcc(s.q.QueryRowContext(ctx, `SELECT `+occCols+` FROM occurrences WHERE account_id = ? AND export_id = ? AND folder = ? AND entry_name = ?`, accountID, exportID, folder, entry))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return o, err
}

// SetOccurrenceItem re-points an occurrence to an item.
func (s *Store) SetOccurrenceItem(ctx context.Context, id, itemID int64, state string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE occurrences SET item_id = ?, state = ? WHERE id = ?`, itemID, state, id)
	return err
}

// MoveOccurrences re-points all occurrences of one item to another.
func (s *Store) MoveOccurrences(ctx context.Context, fromItem, toItem int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE occurrences SET item_id = ?, state = 'resolved' WHERE item_id = ?`, toItem, fromItem)
	return err
}

// ActiveAlbumsOfItem returns the album ids with an active direct occurrence for an item.
func (s *Store) ActiveAlbumsOfItem(ctx context.Context, itemID int64) ([]int64, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT DISTINCT album_id FROM occurrences WHERE item_id = ? AND album_id IS NOT NULL AND active = 1`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// HasActiveOccurrence reports whether (album, item) is supported by an active occurrence.
func (s *Store) HasActiveOccurrence(ctx context.Context, albumID, itemID int64) (bool, error) {
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM occurrences WHERE album_id = ? AND item_id = ? AND active = 1`, albumID, itemID).Scan(&n)
	return n > 0, err
}

// ---- pending sidecars -----------------------------------------------------

// PutPendingSidecar stores a sidecar whose media has not been seen yet.
func (s *Store) PutPendingSidecar(ctx context.Context, accountID int64, exportID string, partNo int, folder, key string, raw []byte) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO pending_sidecars(account_id, export_id, part_no, folder, media_key, json) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(account_id, export_id, folder, media_key) DO UPDATE SET json = excluded.json, part_no = excluded.part_no`, accountID, exportID, partNo, folder, key, raw)
	return err
}

// GetPendingSidecar returns a stored sidecar or nil.
// PendingSidecar is a stored sidecar with the part that stored it.
type PendingSidecar struct {
	Raw    []byte
	PartNo int
}

func (s *Store) GetPendingSidecar(ctx context.Context, accountID int64, exportID, folder, key string) (*PendingSidecar, error) {
	var p PendingSidecar
	var part sql.NullInt64
	err := s.q.QueryRowContext(ctx, `SELECT json, part_no FROM pending_sidecars WHERE account_id = ? AND export_id = ? AND folder = ? AND media_key = ?`, accountID, exportID, folder, key).Scan(&p.Raw, &part)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	p.PartNo = int(part.Int64)
	return &p, err
}

// DeletePendingSidecar removes a stored sidecar.
func (s *Store) DeletePendingSidecar(ctx context.Context, accountID int64, exportID, folder, key string) error {
	_, err := s.q.ExecContext(ctx, `DELETE FROM pending_sidecars WHERE account_id = ? AND export_id = ? AND folder = ? AND media_key = ?`, accountID, exportID, folder, key)
	return err
}

// CountPendingSidecars returns how many sidecars wait for media in an account.
func (s *Store) CountPendingSidecars(ctx context.Context, accountID int64) (int, error) {
	// rows are kept for the whole export (companions may still need them);
	// only those whose own media has not been ingested count as waiting
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_sidecars p WHERE p.account_id = ?
		AND NOT EXISTS (SELECT 1 FROM occurrences o WHERE o.account_id = p.account_id AND o.export_id = p.export_id
		                AND o.folder = p.folder AND o.entry_name = p.media_key COLLATE NOCASE AND o.item_id IS NOT NULL)`, accountID).Scan(&n)
	return n, err
}

// ---- journal --------------------------------------------------------------

// JournalRow is a pending or finished file operation.
type JournalRow struct {
	ID    int64
	Op    string
	Steps string
	State string
}

// JournalBegin records the intent to perform file steps.
func (s *Store) JournalBegin(ctx context.Context, op, stepsJSON string) (int64, error) {
	res, err := s.q.ExecContext(ctx, `INSERT INTO fs_journal(op, steps, state, ts) VALUES (?, ?, 'intent', ?)`, op, stepsJSON, Now())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// JournalDone marks a journal entry finished.
func (s *Store) JournalDone(ctx context.Context, id int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE fs_journal SET state = 'done' WHERE id = ?`, id)
	return err
}

// JournalOpen returns unfinished journal entries in order.
func (s *Store) JournalOpen(ctx context.Context) ([]JournalRow, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT id, op, steps, state FROM fs_journal WHERE state = 'intent' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JournalRow
	for rows.Next() {
		var j JournalRow
		if err := rows.Scan(&j.ID, &j.Op, &j.Steps, &j.State); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// JournalPrune deletes finished entries older than keep.
func (s *Store) JournalPrune(ctx context.Context, keep time.Duration) error {
	_, err := s.q.ExecContext(ctx, `DELETE FROM fs_journal WHERE state = 'done' AND ts < ?`, FormatTime(time.Now().Add(-keep)))
	return err
}

// ---- runs, errors, stats --------------------------------------------------

// BeginRun records the start of a command run.
func (s *Store) BeginRun(ctx context.Context, accountID int64, command string) (int64, error) {
	res, err := s.q.ExecContext(ctx, `INSERT INTO runs(account_id, command, started_at) VALUES (?, ?, ?)`, nullInt(accountID), command, Now())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// FinishRun records the end of a run.
func (s *Store) FinishRun(ctx context.Context, id int64, exitCode int, countsJSON string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE runs SET finished_at = ?, exit_code = ?, counts = ? WHERE id = ?`, Now(), exitCode, countsJSON, id)
	return err
}

// LogError stores an error for later reporting.
func (s *Store) LogError(ctx context.Context, accountID int64, ref, msg string) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO errors(ts, account_id, ref, message) VALUES (?, ?, ?, ?)`, Now(), nullInt(accountID), nullStr(ref), msg)
	return err
}

// Counts returns summary numbers for an account.
func (s *Store) Counts(ctx context.Context, accountID int64) (map[string]int, error) {
	out := map[string]int{}
	queries := map[string]string{
		"items":            `SELECT COUNT(*) FROM items WHERE account_id = ?`,
		"items_fallback":   `SELECT COUNT(*) FROM items WHERE account_id = ? AND identity_src = 'fallback'`,
		"items_archived":   `SELECT COUNT(*) FROM items WHERE account_id = ? AND archived_export_id IS NOT NULL`,
		"albums":           `SELECT COUNT(*) FROM albums WHERE account_id = ?`,
		"album_links":      `SELECT COUNT(*) FROM album_items ai JOIN albums a ON a.id = ai.album_id WHERE a.account_id = ?`,
		"archives":         `SELECT COUNT(*) FROM archives WHERE account_id = ?`,
		"pending_sidecars": `SELECT COUNT(*) FROM pending_sidecars WHERE account_id = ?`,
		"entries_failed":   `SELECT COUNT(*) FROM archive_entries e JOIN archives a ON a.id = e.archive_id WHERE a.account_id = ? AND e.status = 'failed'`,
		"entries_given_up": `SELECT COUNT(*) FROM archive_entries e JOIN archives a ON a.id = e.archive_id WHERE a.account_id = ? AND e.status = 'given_up'`,
	}
	for k, q := range queries {
		var n int
		if err := s.q.QueryRowContext(ctx, q, accountID).Scan(&n); err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		out[k] = n
	}
	var bytes sql.NullInt64
	if err := s.q.QueryRowContext(ctx, `SELECT SUM(b.size) FROM blobs b WHERE b.id IN (SELECT DISTINCT blob_id FROM items WHERE account_id = ?)`, accountID).Scan(&bytes); err != nil {
		return nil, err
	}
	out["bytes"] = int(bytes.Int64)
	return out, nil
}

// OccurrencesInFolder returns the occurrences of one folder of one export.
func (s *Store) OccurrencesInFolder(ctx context.Context, accountID int64, exportID, folder string) ([]*Occurrence, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+occCols+` FROM occurrences WHERE account_id = ? AND export_id = ? AND folder = ? ORDER BY id`, accountID, exportID, folder)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Occurrence
	for rows.Next() {
		o, err := scanOcc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// OccurrencesByItem returns all occurrences of an item.
func (s *Store) OccurrencesByItem(ctx context.Context, itemID int64) ([]*Occurrence, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+occCols+` FROM occurrences WHERE item_id = ? ORDER BY id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Occurrence
	for rows.Next() {
		o, err := scanOcc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// DeleteAlbumItemsOfItem removes every membership row of an item.
func (s *Store) DeleteAlbumItemsOfItem(ctx context.Context, itemID int64) error {
	_, err := s.q.ExecContext(ctx, `DELETE FROM album_items WHERE item_id = ?`, itemID)
	return err
}

// RenameLinkPrefix rewrites album link paths after an album folder rename.
func (s *Store) RenameLinkPrefix(ctx context.Context, albumID int64, oldPrefix, newPrefix string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE album_items SET link_path = ? || substr(link_path, ?) WHERE album_id = ? AND substr(link_path, 1, ?) = ?`,
		newPrefix, len(oldPrefix)+1, albumID, len(oldPrefix), oldPrefix)
	return err
}

// ---- telegram -------------------------------------------------------------

// Chat is a paired Telegram chat.
type Chat struct {
	ChatID      int64
	UserID      int64
	Username    string
	DisplayName string
	PairedAt    string
	RevokedAt   string
}

// PairChat stores (or re-activates) a paired chat.
func (s *Store) PairChat(ctx context.Context, chatID, userID int64, username, display string) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO telegram_chats(chat_id, user_id, username, display_name, paired_at, revoked_at) VALUES (?, ?, ?, ?, ?, NULL)
		ON CONFLICT(chat_id) DO UPDATE SET user_id = excluded.user_id, username = excluded.username, display_name = excluded.display_name, paired_at = excluded.paired_at, revoked_at = NULL`,
		chatID, nullInt(userID), nullStr(username), nullStr(display), Now())
	return err
}

// RevokeChat marks a chat as unsubscribed.
func (s *Store) RevokeChat(ctx context.Context, chatID int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE telegram_chats SET revoked_at = ? WHERE chat_id = ?`, Now(), chatID)
	return err
}

// PairedChats returns active chats.
func (s *Store) PairedChats(ctx context.Context) ([]Chat, error) {
	return s.chats(ctx, `revoked_at IS NULL`)
}

// AllChats returns every chat ever paired.
func (s *Store) AllChats(ctx context.Context) ([]Chat, error) {
	return s.chats(ctx, `1 = 1`)
}

func (s *Store) chats(ctx context.Context, where string) ([]Chat, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT chat_id, user_id, username, display_name, paired_at, revoked_at FROM telegram_chats WHERE `+where+` ORDER BY paired_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Chat
	for rows.Next() {
		var c Chat
		var uid sql.NullInt64
		var un, dn, rv sql.NullString
		if err := rows.Scan(&c.ChatID, &uid, &un, &dn, &c.PairedAt, &rv); err != nil {
			return nil, err
		}
		c.UserID, c.Username, c.DisplayName, c.RevokedAt = uid.Int64, un.String, dn.String, rv.String
		out = append(out, c)
	}
	return out, rows.Err()
}

// TelegramState reads a state value ("" if absent).
func (s *Store) TelegramState(ctx context.Context, key string) (string, error) {
	var v string
	err := s.q.QueryRowContext(ctx, `SELECT value FROM telegram_state WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// SetTelegramState writes a state value.
func (s *Store) SetTelegramState(ctx context.Context, key, value string) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO telegram_state(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// MoveVersions re-points kept versions of one item to another.
func (s *Store) MoveVersions(ctx context.Context, fromItem, toItem int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE item_versions SET item_id = ? WHERE item_id = ?`, toItem, fromItem)
	return err
}

// SetExportAuthoritative marks an export as a complete listing source.
func (s *Store) SetExportAuthoritative(ctx context.Context, accountID int64, exportID string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE exports SET authoritative = 1 WHERE account_id = ? AND export_id = ?`, accountID, exportID)
	return err
}

// SetExportPruned records that album pruning was applied for an export.
func (s *Store) SetExportPruned(ctx context.Context, accountID int64, exportID string) error {
	_, err := s.q.ExecContext(ctx, `UPDATE exports SET albums_pruned_at = ?, complete_at = COALESCE(complete_at, ?) WHERE account_id = ? AND export_id = ?`, Now(), Now(), accountID, exportID)
	return err
}

// AlbumMembersInExport returns, per album, the items with an active album
// occurrence in the given export.
func (s *Store) AlbumMembersInExport(ctx context.Context, accountID int64, exportID string) (map[int64]map[int64]bool, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT album_id, item_id FROM occurrences WHERE account_id = ? AND export_id = ? AND album_id IS NOT NULL AND item_id IS NOT NULL AND active = 1`, accountID, exportID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]map[int64]bool{}
	for rows.Next() {
		var a, i int64
		if err := rows.Scan(&a, &i); err != nil {
			return nil, err
		}
		if out[a] == nil {
			out[a] = map[int64]bool{}
		}
		out[a][i] = true
	}
	return out, rows.Err()
}

// OccurrencesForMembership returns the active occurrences supporting (album, item).
func (s *Store) OccurrencesForMembership(ctx context.Context, albumID, itemID int64) ([]*Occurrence, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+occCols+` FROM occurrences WHERE album_id = ? AND item_id = ? AND active = 1`, albumID, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Occurrence
	for rows.Next() {
		o, err := scanOcc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// DeactivateOccurrence marks an occurrence as superseded by a complete listing.
func (s *Store) DeactivateOccurrence(ctx context.Context, id int64) error {
	_, err := s.q.ExecContext(ctx, `UPDATE occurrences SET active = 0 WHERE id = ?`, id)
	return err
}

// UpsertAlbumFull records the newest complete listing applied to an album.
func (s *Store) UpsertAlbumFull(ctx context.Context, albumID int64, exportID string) error {
	_, err := s.q.ExecContext(ctx, `INSERT INTO albums_full(album_id, listing_export_id) VALUES (?, ?) ON CONFLICT(album_id) DO UPDATE SET listing_export_id = excluded.listing_export_id`, albumID, exportID)
	return err
}

// GetAlbumFull returns the newest complete listing applied to an album ("" if none).
func (s *Store) GetAlbumFull(ctx context.Context, albumID int64) (string, error) {
	var v string
	err := s.q.QueryRowContext(ctx, `SELECT listing_export_id FROM albums_full WHERE album_id = ?`, albumID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// ArchivesOfExport returns the parts of one export ordered by part number.
func (s *Store) ArchivesOfExport(ctx context.Context, accountID int64, exportID string) ([]*Archive, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT `+archiveCols+` FROM archives WHERE account_id = ? AND export_id = ? ORDER BY part_no`, accountID, exportID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Archive
	for rows.Next() {
		a, err := scanArchive(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// VersionByPath returns the kept version recorded at path, or nil.
func (s *Store) VersionByPath(ctx context.Context, path string) (*Version, error) {
	var v Version
	err := s.q.QueryRowContext(ctx, `SELECT id, item_id, blob_id, path FROM item_versions WHERE path = ?`, path).Scan(&v.ID, &v.ItemID, &v.BlobID, &v.Path)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &v, err
}

// Version is a kept superseded file.
type Version struct {
	ID     int64
	ItemID int64
	BlobID int64
	Path   string
}

// EntryNamesByStatus returns the entry names of a part in the given statuses.
func (s *Store) EntryNamesByStatus(ctx context.Context, archiveID int64, statuses ...string) (map[string]bool, error) {
	q := `SELECT entry_name FROM archive_entries WHERE archive_id = ? AND status IN (`
	args := []any{archiveID}
	for i, st := range statuses {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, st)
	}
	q += `)`
	rows, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out[n] = true
	}
	return out, rows.Err()
}

// AlbumPathTakenExcept reports whether an album path is used by another album.
func (s *Store) AlbumPathTakenExcept(ctx context.Context, path string, albumID int64) (bool, error) {
	var n int
	err := s.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM albums WHERE path = ? AND id != ?`, path, albumID).Scan(&n)
	return n > 0, err
}

// PendingSidecarsByPrefix returns stored sidecars whose media key (at least 8
// characters) is a prefix of mediaName, for truncated-name matching.
func (s *Store) PendingSidecarsByPrefix(ctx context.Context, accountID int64, exportID, folder, mediaName string) (map[string]PendingSidecar, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT media_key, json, part_no FROM pending_sidecars WHERE account_id = ? AND export_id = ? AND folder = ?
		AND length(media_key) >= 8 AND substr(lower(?), 1, length(media_key)) = lower(media_key)`, accountID, exportID, folder, mediaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]PendingSidecar{}
	for rows.Next() {
		var k string
		var p PendingSidecar
		var part sql.NullInt64
		if err := rows.Scan(&k, &p.Raw, &part); err != nil {
			return nil, err
		}
		p.PartNo = int(part.Int64)
		out[k] = p
	}
	return out, rows.Err()
}

// VersionsByItem returns the kept versions of an item.
func (s *Store) VersionsByItem(ctx context.Context, itemID int64) ([]Version, error) {
	rows, err := s.q.QueryContext(ctx, `SELECT id, item_id, blob_id, path FROM item_versions WHERE item_id = ? ORDER BY id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Version
	for rows.Next() {
		var v Version
		if err := rows.Scan(&v.ID, &v.ItemID, &v.BlobID, &v.Path); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
