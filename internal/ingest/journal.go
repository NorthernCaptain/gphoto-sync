package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/store"
)

// Step is one idempotent file operation. Paths are relative to data_dir.
type Step struct {
	Op  string `json:"op"` // rename | link | symlink | copy | unlink
	Src string `json:"src,omitempty"`
	Dst string `json:"dst"`
	// SHA, for rename, is the content moved: when Dst already holds it the
	// step is complete even if Src has since been reused for other bytes.
	SHA string `json:"sha,omitempty"`
}

func (ing *Ingester) abs(rel string) string {
	return filepath.Join(ing.cfg.DataDir, filepath.FromSlash(rel))
}

func (ing *Ingester) apply(st Step) error {
	switch st.Op {
	case "rename":
		if st.SHA != "" {
			if dst := ing.abs(st.Dst); exists(dst) && fileSHA(dst) == st.SHA {
				return nil
			}
		}
		return fsops.Rename(ing.abs(st.Src), ing.abs(st.Dst))
	case "link":
		return fsops.Link(ing.abs(st.Src), ing.abs(st.Dst))
	case "symlink":
		return fsops.Symlink(ing.abs(st.Src), ing.abs(st.Dst))
	case "copy":
		return fsops.Copy(ing.abs(st.Src), ing.abs(st.Dst))
	case "unlink":
		return fsops.Unlink(ing.abs(st.Dst))
	}
	return fmt.Errorf("unknown journal op %q", st.Op)
}

// journaled records the intent, applies the file steps, then runs dbWork and
// marks the journal entry done in one transaction.
func (ing *Ingester) journaled(ctx context.Context, op string, steps []Step, dbWork func(tx *store.Store) error) error {
	if len(steps) == 0 {
		return ing.st.Tx(ctx, dbWork)
	}
	raw, err := json.Marshal(steps)
	if err != nil {
		return err
	}
	id, err := ing.st.JournalBegin(ctx, op, string(raw))
	if err != nil {
		return err
	}
	ing.journalCount.Add(1)
	for _, st := range steps {
		if err := ing.apply(st); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}
	return ing.st.Tx(ctx, func(tx *store.Store) error {
		if dbWork != nil {
			if err := dbWork(tx); err != nil {
				return err
			}
		}
		return tx.JournalDone(ctx, id)
	})
}

// RecoverJournal replays unfinished file operations from a previous run.
// Steps are idempotent, so replaying completed ones is harmless; the DB part
// of an interrupted operation is redone by the next ingest of the same entry.
func (ing *Ingester) RecoverJournal(ctx context.Context) error {
	open, err := ing.st.JournalOpen(ctx)
	if err != nil {
		return err
	}
	for _, j := range open {
		var steps []Step
		if err := json.Unmarshal([]byte(j.Steps), &steps); err != nil {
			return fmt.Errorf("journal %d: %w", j.ID, err)
		}
		ing.log.Warn("replaying unfinished file operation", "journal_id", j.ID, "op", j.Op, "steps", len(steps))
		for _, st := range steps {
			if err := ing.apply(st); err != nil {
				// leave the entry open: the database may point at a path this step was
				// meant to create, so nothing may proceed until it is resolved
				return fmt.Errorf("journal %d (%s) step %s %s -> %s failed: %w; fix the filesystem and start again", j.ID, j.Op, st.Op, st.Src, st.Dst, err)
			}
		}
		if err := ing.st.JournalDone(ctx, j.ID); err != nil {
			return err
		}
	}
	_ = ing.st.JournalPrune(ctx, 30*24*time.Hour)
	return nil
}

// setMtime sets a file's modification time, ignoring failures.
func (ing *Ingester) setMtime(rel string, t time.Time) {
	if t.IsZero() {
		return
	}
	_ = os.Chtimes(ing.abs(rel), t, t)
}
