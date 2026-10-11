// Package takeout understands the layout of a Google Takeout "Google Photos"
// export: folders, media files, JSON sidecars, album descriptors, and the
// naming quirks that connect sidecars to media.
package takeout

import (
	"fmt"
	"path"
	"sort"
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

// Rules configure classification: the Takeout names of one language (Takeout
// localizes its folder and file names to the account's Google language). An
// empty list means the English default.
type Rules struct {
	ProductFolders []string // the product folder under Takeout/, e.g. "Google Photos"
	YearPrefixes   []string // what precedes the year in a year folder, e.g. "Photos from "
	YearSuffixes   []string // what may follow it, e.g. "" or " г."
	ArchiveFolders []string
	TrashFolders   []string
	AlbumMetaFiles []string // the album descriptor, e.g. "metadata.json"
	EditedSuffixes []string // the edited-copy suffix before the extension, e.g. "-edited"
}

// DefaultRules are the English Takeout names.
var DefaultRules = Rules{
	ProductFolders: []string{"Google Photos"},
	YearPrefixes:   []string{"Photos from "},
	YearSuffixes:   []string{""},
	ArchiveFolders: []string{"Archive"},
	TrashFolders:   []string{"Trash"},
	AlbumMetaFiles: []string{"metadata.json"},
	EditedSuffixes: []string{"-edited"},
}

// Locales are the Takeout names per Google account language, each complete.
var Locales = map[string]Rules{
	"en": DefaultRules,
	"ru": { // seen in a 2026-10 export of a Russian-language account
		ProductFolders: []string{"Google Фото"},
		YearPrefixes:   []string{"Фото "},
		YearSuffixes:   []string{" г."},
		ArchiveFolders: []string{"Отправить в архив"},
		TrashFolders:   []string{"Корзина"},
		AlbumMetaFiles: []string{"метаданные.json"},
		EditedSuffixes: []string{"-отредактировано"},
	},
}

// KnownLanguages lists the languages with built-in names, sorted.
func KnownLanguages() []string {
	out := make([]string, 0, len(Locales))
	for l := range Locales {
		out = append(out, l)
	}
	sort.Strings(out)
	return out
}

// ForLanguage returns the names of one language. An account is classified
// with the names of its own language only: merging languages would let an
// album of one language pass for a system folder of another.
func ForLanguage(lang string) (Rules, error) {
	loc, ok := Locales[lang]
	if !ok {
		return Rules{}, fmt.Errorf("takeout language %q is not built in (known: %s)", lang, strings.Join(KnownLanguages(), ", "))
	}
	return loc, nil
}

// pick returns list, or def when list is empty.
func pick(list, def []string) []string {
	if len(list) == 0 {
		return def
	}
	return list
}

func (r Rules) isProduct(name string) bool {
	for _, p := range pick(r.ProductFolders, DefaultRules.ProductFolders) {
		if name == p {
			return true
		}
	}
	return false
}

func (r Rules) isAlbumMeta(name string) bool {
	for _, p := range pick(r.AlbumMetaFiles, DefaultRules.AlbumMetaFiles) {
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
	case r.isAlbumMeta(e.Base):
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

// ClassifyFolder tells the kind of a Takeout folder by its name.
func ClassifyFolder(folder string, r Rules) FolderKind {
	k, _ := classifyFolder(folder, r)
	return k
}

func classifyFolder(folder string, r Rules) (FolderKind, int) {
	for _, p := range pick(r.YearPrefixes, DefaultRules.YearPrefixes) {
		rest, ok := strings.CutPrefix(folder, p)
		if !ok {
			continue
		}
		for _, suffix := range pick(r.YearSuffixes, DefaultRules.YearSuffixes) {
			if y, ok := strings.CutSuffix(rest, suffix); ok {
				if n, err := strconv.Atoi(strings.TrimSpace(y)); err == nil && n > 1800 && n < 3000 {
					return FolderYear, n
				}
			}
		}
	}
	for _, a := range pick(r.ArchiveFolders, DefaultRules.ArchiveFolders) {
		if folder == a {
			return FolderArchive, 0
		}
	}
	for _, t := range pick(r.TrashFolders, DefaultRules.TrashFolders) {
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

// IsEdited reports whether a media name is a Google "-edited" version (in
// any configured language) and returns the original's name.
func (r Rules) IsEdited(base string) (orig string, edited bool) {
	root, ext := SplitExt(base)
	for _, suffix := range pick(r.EditedSuffixes, DefaultRules.EditedSuffixes) {
		if strings.HasSuffix(root, suffix) {
			return strings.TrimSuffix(root, suffix) + ext, true
		}
		// "IMG_1-edited(1).jpg": the (n) can follow -edited
		if i := strings.LastIndex(root, suffix+"("); i >= 0 && strings.HasSuffix(root, ")") {
			return root[:i] + root[i+len(suffix):] + ext, true
		}
	}
	return base, false
}
