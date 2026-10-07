// Package takeout understands the layout of a Google Takeout "Google Photos"
// export: folders, media files, JSON sidecars, album descriptors, and the
// naming quirks that connect sidecars to media.
package takeout

import (
	"path"
	"strconv"
	"strings"
)

// FolderKind says what the third path component of an entry is.
type FolderKind int

const (
	FolderUnknown FolderKind = iota
	FolderYear               // "Photos from 2003"
	FolderArchive            // Google's Archive folder
	FolderTrash              // Google's Trash folder
	FolderAlbum              // anything else: an album
)

// EntryType says what a file is.
type EntryType int

const (
	TypeIgnore    EntryType = iota
	TypeMedia               // a photo or video (any non-JSON file)
	TypeSidecar             // a per-item JSON sidecar
	TypeAlbumMeta           // an album folder's metadata.json
)

// Entry is a classified archive entry.
type Entry struct {
	Name       string // full path inside the archive
	Folder     string // third path component, e.g. "Photos from 2003" or an album folder
	Base       string // file name
	FolderKind FolderKind
	Year       int // for FolderYear
	Type       EntryType
	Size       int64
}

// Rules configure classification.
type Rules struct {
	ProductFolders []string // the product folder under Takeout/, e.g. "Google Photos"
	YearPrefixes   []string
	ArchiveFolders []string
	TrashFolders   []string
}

// DefaultRules are the English Takeout names.
var DefaultRules = Rules{
	ProductFolders: []string{"Google Photos"},
	YearPrefixes:   []string{"Photos from "},
	ArchiveFolders: []string{"Archive"},
	TrashFolders:   []string{"Trash"},
}

func (r Rules) isProduct(name string) bool {
	folders := r.ProductFolders
	if len(folders) == 0 {
		folders = DefaultRules.ProductFolders
	}
	for _, p := range folders {
		if name == p {
			return true
		}
	}
	return false
}

var ignoredFiles = map[string]bool{
	"archive_browser.html":              true,
	"print-subscriptions.json":          true,
	"shared_album_comments.json":        true,
	"user-generated-memory-titles.json": true,
}

// Classify parses an archive entry name. ok is false for directories and
// entries outside the Takeout/<product>/<folder>/ layout.
func Classify(name string, size int64, r Rules) (e Entry, ok bool) {
	if strings.HasSuffix(name, "/") {
		return Entry{}, false
	}
	parts := strings.Split(path.Clean(name), "/")
	// Takeout / Google Photos / <folder> / <file>; other products in the same
	// export are not ours
	if len(parts) < 4 || parts[0] != "Takeout" || !r.isProduct(parts[1]) {
		if len(parts) >= 2 && ignoredFiles[parts[len(parts)-1]] {
			return Entry{Name: name, Base: parts[len(parts)-1], Type: TypeIgnore, Size: size}, true
		}
		return Entry{}, false
	}
	e = Entry{Name: name, Folder: parts[2], Base: parts[len(parts)-1], Size: size}
	e.FolderKind, e.Year = classifyFolder(e.Folder, r)
	switch {
	case ignoredFiles[e.Base]:
		e.Type = TypeIgnore
	case e.Base == "metadata.json":
		if e.FolderKind == FolderAlbum {
			e.Type = TypeAlbumMeta
		} else {
			e.Type = TypeIgnore
		}
	case strings.HasSuffix(strings.ToLower(e.Base), ".json"):
		e.Type = TypeSidecar
	default:
		e.Type = TypeMedia
	}
	return e, true
}

func classifyFolder(folder string, r Rules) (FolderKind, int) {
	for _, p := range r.YearPrefixes {
		if strings.HasPrefix(folder, p) {
			if y, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(folder, p))); err == nil && y > 1800 && y < 3000 {
				return FolderYear, y
			}
		}
	}
	for _, a := range r.ArchiveFolders {
		if folder == a {
			return FolderArchive, 0
		}
	}
	for _, t := range r.TrashFolders {
		if folder == t {
			return FolderTrash, 0
		}
	}
	return FolderAlbum, 0
}

// SplitExt splits "IMG_1(1).jpg" into "IMG_1(1)" and ".jpg". Names without a
// dot have an empty extension.
func SplitExt(name string) (root, ext string) {
	i := strings.LastIndex(name, ".")
	if i <= 0 {
		return name, ""
	}
	return name[:i], name[i:]
}

// IsEdited reports whether a media name is a Google "-edited" version and
// returns the original's name.
func IsEdited(base string) (orig string, edited bool) {
	root, ext := SplitExt(base)
	const suffix = "-edited"
	if strings.HasSuffix(root, suffix) {
		return strings.TrimSuffix(root, suffix) + ext, true
	}
	// "IMG_1-edited(1).jpg": the (n) can follow -edited
	if i := strings.LastIndex(root, suffix+"("); i >= 0 && strings.HasSuffix(root, ")") {
		return root[:i] + root[i+len(suffix):] + ext, true
	}
	return base, false
}
