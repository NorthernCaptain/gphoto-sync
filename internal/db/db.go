// Package db opens the SQLite state database and applies embedded migrations.
package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

//go:embed migrations/*.sql
var migrations embed.FS

// Open opens (creating if needed) the database at path with WAL mode and
// foreign keys enabled, then applies pending migrations.
func Open(ctx context.Context, path string) (*sql.DB, error) {
	d, err := openRaw(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := migrate(ctx, d); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

// OpenReadOnly opens an existing database without applying migrations, for
// commands that must not change state. It fails when the file is missing or
// when its schema is behind the embedded migrations (a writing command
// migrates it).
func OpenReadOnly(ctx context.Context, path string) (*sql.DB, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("no state database at %s yet: run a writing command (sync, ingest) first", path)
	}
	// mode=ro: SQLite refuses every write; no WAL request either (the file is
	// already in WAL mode after the first writing command)
	d, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite read-only: %w", err)
	}
	d.SetMaxOpenConns(1)
	if err := d.PingContext(ctx); err != nil {
		d.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	pending, err := pendingMigrations(ctx, d)
	if err != nil {
		d.Close()
		return nil, err
	}
	if len(pending) > 0 {
		d.Close()
		return nil, fmt.Errorf("state database schema is behind (%s pending): run sync or ingest to migrate it", strings.Join(pending, ", "))
	}
	return d, nil
}

// pendingMigrations lists embedded migrations not recorded as applied.
func pendingMigrations(ctx context.Context, d *sql.DB) ([]string, error) {
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var hasTable int
	if err := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&hasTable); err != nil {
		return nil, err
	}
	var pending []string
	for _, name := range names {
		base := strings.TrimPrefix(name, "migrations/")
		n := 0
		if hasTable > 0 {
			if err := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE name = ?`, base).Scan(&n); err != nil {
				return nil, err
			}
		}
		if n == 0 {
			pending = append(pending, base)
		}
	}
	return pending, nil
}

// openRaw opens the database without applying migrations.
func openRaw(ctx context.Context, path string) (*sql.DB, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)"
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite allows one writer; a single connection avoids SQLITE_BUSY churn.
	d.SetMaxOpenConns(1)
	if err := d.PingContext(ctx); err != nil {
		d.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return d, nil
}

// migrate applies every embedded migration not applied yet, in name order.
func migrate(ctx context.Context, d *sql.DB) error {
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		if err := applyMigration(ctx, d, strings.TrimPrefix(name, "migrations/")); err != nil {
			return err
		}
	}
	return nil
}

// applyMigration applies one embedded migration unless it was applied before.
func applyMigration(ctx context.Context, d *sql.DB, base string) error {
	if _, err := d.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	var n int
	if err := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE name = ?`, base).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	body, err := migrations.ReadFile("migrations/" + base)
	if err != nil {
		return err
	}
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, string(body)); err != nil {
		tx.Rollback()
		return fmt.Errorf("migration %s: %w", base, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(name, applied_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))`, base); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
