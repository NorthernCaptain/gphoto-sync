package ingest

import (
	"context"
	"fmt"
	"os"
	"path"

	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/store"
)

// PruneResult summarises an album pruning pass.
type PruneResult struct {
	ExportID      string
	Albums        int
	Removed       int
	GroupAdjusted int
	Skipped       string // why nothing was done
	WouldRemove   []string
}

// Prune applies the complete, authoritative listing of an export to album
// membership (PLAN.md 3.3): a direct membership last confirmed by an older
// export and absent from the export's album folders is removed. It does
// nothing unless the export is authoritative, its part count is confirmed and
// every part is fully ingested.
func (ing *Ingester) Prune(ctx context.Context, account, exportID string, dryRun bool) (*PruneResult, error) {
	res := &PruneResult{ExportID: exportID}
	acct, err := ing.st.GetAccount(ctx, account)
	if err != nil || acct == nil {
		return nil, fmt.Errorf("unknown account %q", account)
	}
	exp, err := ing.st.GetExport(ctx, acct.ID, exportID)
	if err != nil || exp == nil {
		return nil, fmt.Errorf("unknown export %q", exportID)
	}
	switch {
	case !exp.Authoritative:
		res.Skipped = "export is not authoritative"
		return res, nil
	case exp.ConfirmedParts == 0:
		res.Skipped = "part count not confirmed (gphoto-sync exports confirm)"
		return res, nil
	case exp.AlbumsPrunedAt != "":
		res.Skipped = "already applied on " + exp.AlbumsPrunedAt
		return res, nil
	case exp.HighestPart != exp.ConfirmedParts:
		res.Skipped = fmt.Sprintf("%d parts seen but %d confirmed", exp.HighestPart, exp.ConfirmedParts)
		return res, nil
	}
	archs, err := ing.st.ArchivesOfExport(ctx, acct.ID, exportID)
	if err != nil {
		return nil, err
	}
	seen := map[int]bool{}
	for _, a := range archs {
		if a.Status != store.ArchiveIngested {
			res.Skipped = fmt.Sprintf("part %d is %s", a.PartNo, a.Status)
			return res, nil
		}
		seen[a.PartNo] = true
	}
	for p := 1; p <= exp.ConfirmedParts; p++ {
		if !seen[p] {
			res.Skipped = fmt.Sprintf("part %d of %d not ingested", p, exp.ConfirmedParts)
			return res, nil
		}
	}
	listing, err := ing.st.AlbumMembersInExport(ctx, acct.ID, exportID)
	if err != nil {
		return nil, err
	}
	albums, err := ing.st.ListAlbums(ctx, acct.ID)
	if err != nil {
		return nil, err
	}
	pr := &partRun{ing: ing, ctx: ctx, acct: acct}
	for _, album := range albums {
		if applied, _ := ing.st.GetAlbumFull(ctx, album.ID); applied != "" && !ExportOlder(applied, exportID) {
			continue // a newer complete listing was already applied
		}
		res.Albums++
		members := listing[album.ID]
		rows, err := ing.st.AlbumItemsByAlbum(ctx, album.ID)
		if err != nil {
			return nil, err
		}
		var stills []*store.Item
		for _, row := range rows {
			if !row.Direct || members[row.ItemID] {
				continue
			}
			if row.ConfirmedExportID != "" && !ExportOlder(row.ConfirmedExportID, exportID) {
				continue // confirmed by this or a newer export
			}
			item, err := ing.st.GetItem(ctx, row.ItemID)
			if err != nil || item == nil {
				continue
			}
			res.Removed++
			if dryRun {
				res.WouldRemove = append(res.WouldRemove, row.LinkPath)
				continue
			}
			if err := pr.removeDirect(album, item, row, exportID); err != nil {
				return nil, fmt.Errorf("%s: %w", row.LinkPath, err)
			}
			stills = append(stills, item)
		}
		if dryRun {
			continue
		}
		for _, still := range stills {
			n, err := pr.recomputeGroup(album, still)
			if err != nil {
				return nil, err
			}
			res.GroupAdjusted += n
		}
		if err := ing.st.UpsertAlbumFull(ctx, album.ID, exportID); err != nil {
			return nil, err
		}
		_ = os.Remove(ing.abs(album.Path)) // only succeeds when the folder is empty
	}
	if !dryRun {
		if err := ing.st.SetExportPruned(ctx, acct.ID, exportID); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// removeDirect clears the direct flag of a membership (deactivating the
// occurrences that supported it) and removes the link when no group link remains.
func (pr *partRun) removeDirect(album *store.Album, item *store.Item, row *store.AlbumItem, exportID string) error {
	ctx, st := pr.ctx, pr.ing.st
	occs, err := st.OccurrencesForMembership(ctx, album.ID, item.ID)
	if err != nil {
		return err
	}
	var steps []Step
	if !row.ViaGroup {
		steps = []Step{{Op: "unlink", Dst: row.LinkPath}}
	}
	return pr.ing.journaled(ctx, "prune", steps, func(tx *store.Store) error {
		for _, o := range occs {
			if ExportOlder(o.ExportID, exportID) {
				if err := tx.DeactivateOccurrence(ctx, o.ID); err != nil {
					return err
				}
			}
		}
		if row.ViaGroup {
			row.Direct, row.ConfirmedExportID, row.ConfirmedBy = false, "", ""
			return tx.UpsertAlbumItem(ctx, row)
		}
		return tx.DeleteAlbumItem(ctx, album.ID, item.ID)
	})
}

// recomputeGroup re-evaluates via_group links of a still's companions in one
// album after the still's direct membership changed.
func (pr *partRun) recomputeGroup(album *store.Album, still *store.Item) (int, error) {
	ctx, st := pr.ctx, pr.ing.st
	stillRow, err := st.GetAlbumItem(ctx, album.ID, still.ID)
	if err != nil {
		return 0, err
	}
	stillDirect := stillRow != nil && stillRow.Direct
	members, err := st.ItemsByGroup(ctx, pr.acct.ID, still.GroupID)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range members {
		if m.ID == still.ID {
			continue
		}
		row, err := st.GetAlbumItem(ctx, album.ID, m.ID)
		if err != nil || row == nil || !row.ViaGroup || row.GroupStill != still.ID {
			continue
		}
		if stillDirect {
			continue
		}
		n++
		var steps []Step
		if !row.Direct {
			steps = []Step{{Op: "unlink", Dst: row.LinkPath}}
		}
		err = pr.ing.journaled(ctx, "prune-group", steps, func(tx *store.Store) error {
			if row.Direct {
				row.ViaGroup, row.GroupStill = false, 0
				return tx.UpsertAlbumItem(ctx, row)
			}
			return tx.DeleteAlbumItem(ctx, album.ID, m.ID)
		})
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// PrunableExports lists exports of an account that are ready for pruning but
// have not been pruned yet.
func (ing *Ingester) PrunableExports(ctx context.Context, account string) ([]string, error) {
	acct, err := ing.st.GetAccount(ctx, account)
	if err != nil || acct == nil {
		return nil, err
	}
	exps, err := ing.st.ListExports(ctx, acct.ID)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range exps {
		if e.Authoritative && e.ConfirmedParts > 0 && e.AlbumsPrunedAt == "" {
			out = append(out, e.ExportID)
		}
	}
	return out, nil
}

var _ = fsops.Unlink
var _ = path.Base
