package ingest

import (
	"archive/zip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Source is an archive or directory holding a Takeout part.
type Source interface {
	Entries() []SourceEntry
	Open(e SourceEntry) (io.ReadCloser, error)
	Close() error
}

// SourceEntry is one file in a Source. Offset orders entries for sequential reads.
type SourceEntry struct {
	Name   string
	Size   int64
	Offset int64
}

type zipSource struct {
	rc      *zip.ReadCloser
	entries []SourceEntry
	files   map[string]*zip.File
}

// OpenZip opens a zip part.
func OpenZip(path string) (Source, error) {
	rc, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("open zip %s: %w", path, err)
	}
	zs := &zipSource{rc: rc, files: map[string]*zip.File{}}
	for _, f := range rc.File {
		if f.FileInfo().IsDir() {
			continue
		}
		off, _ := f.DataOffset()
		zs.entries = append(zs.entries, SourceEntry{Name: f.Name, Size: int64(f.UncompressedSize64), Offset: off})
		zs.files[f.Name] = f
	}
	sort.Slice(zs.entries, func(i, j int) bool { return zs.entries[i].Offset < zs.entries[j].Offset })
	return zs, nil
}

func (z *zipSource) Entries() []SourceEntry { return z.entries }

func (z *zipSource) Open(e SourceEntry) (io.ReadCloser, error) {
	f, ok := z.files[e.Name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return f.Open()
}

func (z *zipSource) Close() error { return z.rc.Close() }

type dirSource struct {
	root    string
	entries []SourceEntry
}

// OpenDir opens an already-extracted part. The directory must contain the
// "Takeout" folder (or be it).
func OpenDir(root string) (Source, error) {
	root = filepath.Clean(root)
	prefix := ""
	if filepath.Base(root) == "Takeout" {
		prefix = "Takeout/"
	}
	ds := &dirSource{root: root}
	var off int64
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		name := prefix + filepath.ToSlash(rel)
		ds.entries = append(ds.entries, SourceEntry{Name: name, Size: info.Size(), Offset: off})
		off++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func (d *dirSource) Entries() []SourceEntry { return d.entries }

func (d *dirSource) Open(e SourceEntry) (io.ReadCloser, error) {
	name := e.Name
	if filepath.Base(d.root) == "Takeout" {
		name = strings.TrimPrefix(name, "Takeout/")
	}
	return os.Open(filepath.Join(d.root, filepath.FromSlash(name)))
}

func (d *dirSource) Close() error { return nil }
