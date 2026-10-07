package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/store"
)

// VerifyResult summarises a verify run.
type VerifyResult struct {
	Items, MissingFiles, HashMismatches, Links, MissingLinks, BadLinks, Canonical, MissingCanonical int
	Versions, MissingVersions, VersionMismatches                                                    int
	Problems                                                                                        []string
}

func (v *VerifyResult) problem(format string, args ...any) {
	if len(v.Problems) < 200 {
		v.Problems = append(v.Problems, fmt.Sprintf(format, args...))
	}
}

// Verify checks that every item file and album link exists (and matches when
// rehash is set) and that blob canonical paths are valid.
func (ing *Ingester) Verify(ctx context.Context, account string, rehash bool) (*VerifyResult, error) {
	acct, err := ing.st.GetAccount(ctx, account)
	if err != nil {
		return nil, err
	}
	res := &VerifyResult{}
	if acct == nil {
		return res, nil
	}
	items, err := ing.st.ListItems(ctx, acct.ID)
	if err != nil {
		return nil, err
	}
	blobs := map[int64]*store.Blob{}
	for _, it := range items {
		res.Items++
		abs := ing.abs(it.LibPath)
		fi, err := os.Stat(abs)
		if err != nil {
			res.MissingFiles++
			res.problem("missing file: %s", it.LibPath)
			continue
		}
		b, ok := blobs[it.BlobID]
		if !ok {
			b, _ = ing.st.GetBlob(ctx, it.BlobID)
			blobs[it.BlobID] = b
		}
		if b != nil {
			if fi.Size() != b.Size {
				res.HashMismatches++
				res.problem("size differs: %s", it.LibPath)
			} else if rehash && fileSHA(abs) != b.SHA256 {
				res.HashMismatches++
				res.problem("hash differs: %s", it.LibPath)
			}
		}
		// kept versions hold bytes that may exist nowhere else
		versions, err := ing.st.VersionsByItem(ctx, it.ID)
		if err != nil {
			return nil, err
		}
		for _, v := range versions {
			res.Versions++
			vb, ok := blobs[v.BlobID]
			if !ok {
				vb, _ = ing.st.GetBlob(ctx, v.BlobID)
				blobs[v.BlobID] = vb
			}
			vfi, err := os.Stat(ing.abs(v.Path))
			if err != nil {
				res.MissingVersions++
				res.problem("missing kept version: %s", v.Path)
				continue
			}
			if vb != nil && (vfi.Size() != vb.Size || (rehash && fileSHA(ing.abs(v.Path)) != vb.SHA256)) {
				res.VersionMismatches++
				res.problem("kept version differs from its record: %s", v.Path)
			}
		}
		ais, err := ing.st.AlbumItemsByItem(ctx, it.ID)
		if err != nil {
			return nil, err
		}
		for _, ai := range ais {
			res.Links++
			labs := ing.abs(ai.LinkPath)
			if !exists(labs) {
				res.MissingLinks++
				res.problem("missing link: %s", ai.LinkPath)
				continue
			}
			switch ing.cfg.Layout.LinkMode {
			case "hardlink":
				if !fsops.SameInode(labs, abs) {
					res.BadLinks++
					res.problem("link is not the item's inode: %s", ai.LinkPath)
				}
			case "copy":
				if li, err := os.Stat(labs); err != nil || li.Size() != fi.Size() {
					res.BadLinks++
					res.problem("copy size differs: %s", ai.LinkPath)
				} else if rehash {
					if same, err := fsops.SameContent(labs, abs); err != nil || !same {
						res.BadLinks++
						res.problem("copy content differs: %s", ai.LinkPath)
					}
				}
			case "symlink":
				target, err := os.Readlink(labs)
				resolved := filepath.Clean(filepath.Join(filepath.Dir(labs), target))
				if err != nil || resolved != filepath.Clean(abs) {
					res.BadLinks++
					res.problem("symlink does not point at the item: %s -> %s", ai.LinkPath, target)
				}
			}
		}
	}
	for _, b := range blobs {
		if b == nil {
			continue
		}
		res.Canonical++
		if !exists(ing.abs(b.CanonicalPath)) {
			res.MissingCanonical++
			res.problem("missing canonical path: %s", b.CanonicalPath)
		}
	}
	return res, nil
}

func blobSHA(ctx context.Context, ing *Ingester, blobID int64) string {
	b, err := ing.st.GetBlob(ctx, blobID)
	if err != nil || b == nil {
		return ""
	}
	return b.SHA256
}

// Relink recreates every album link recorded in the database.
func (ing *Ingester) Relink(ctx context.Context, account string) (created int, err error) {
	acct, err := ing.st.GetAccount(ctx, account)
	if err != nil || acct == nil {
		return 0, err
	}
	albums, err := ing.st.ListAlbums(ctx, acct.ID)
	if err != nil {
		return 0, err
	}
	pr := &partRun{ing: ing, ctx: ctx, acct: acct}
	for _, a := range albums {
		ais, err := ing.st.AlbumItemsByAlbum(ctx, a.ID)
		if err != nil {
			return created, err
		}
		for _, ai := range ais {
			it, err := ing.st.GetItem(ctx, ai.ItemID)
			if err != nil || it == nil {
				continue
			}
			libAbs, linkAbs := ing.abs(it.LibPath), ing.abs(ai.LinkPath)
			if fsops.SameInode(linkAbs, libAbs) {
				continue
			}
			if !exists(libAbs) {
				// the library file is gone; a surviving album hardlink is the last copy
				if ing.cfg.Layout.LinkMode == "hardlink" && exists(linkAbs) && fileSHA(linkAbs) == blobSHA(ctx, ing, it.BlobID) {
					if err := ing.journaled(ctx, "restore", []Step{{Op: "link", Src: ai.LinkPath, Dst: it.LibPath}}, nil); err != nil {
						return created, fmt.Errorf("restore %s: %w", it.LibPath, err)
					}
					ing.log.Warn("restored missing library file from album link", "path", it.LibPath, "from", ai.LinkPath)
					created++
					continue
				}
				ing.log.Error("library file missing, album link left untouched", "path", it.LibPath, "link", ai.LinkPath)
				continue
			}
			steps := []Step{pr.linkStep(it.LibPath, ai.LinkPath)}
			if exists(linkAbs) {
				// a stale copy, a symlink with another target, or a hardlink to the
				// wrong inode is replaced; the source exists, so nothing is lost
				steps = append([]Step{{Op: "unlink", Dst: ai.LinkPath}}, steps...)
			}
			if err := ing.journaled(ctx, "relink", steps, nil); err != nil {
				return created, fmt.Errorf("%s: %w", ai.LinkPath, err)
			}
			created++
		}
	}
	return created, nil
}
