package takeout

import (
	"regexp"
	"strings"
)

// Variant classifies a media file relative to its sidecar.
const (
	VariantOriginal = "original"
	VariantEdited   = "edited"
	VariantMotion   = "motion" // companion file (Pixel .MP, Live Photo .MOV) sharing the still's sidecar
)

// Match is the result of looking up a sidecar for a media file.
type Match struct {
	Sidecar *Sidecar
	Variant string
	// Still is the media name the sidecar primarily describes (differs from
	// the looked-up name for edited and motion variants).
	Still string
	// Heuristic is set when the sidecar relates to the media by a name
	// heuristic rather than its full name: a title-named sidecar (stem), a
	// sidecar whose title equals the media name, or a truncated-name prefix.
	// Such an identity is only trusted when it does not contradict a photo
	// already known under that Google id.
	Heuristic bool
	// Settled is set when the sidecar came from the pending store and had
	// already been consumed as an album-only membership of the photo it names.
	Settled bool
	// Key is the media key the sidecar was registered under (for pending storage).
	Key string
}

var (
	suppParenRe = regexp.MustCompile(`^(.*)\.supplemental-metadata\((\d+)\)$`)
	parenRe     = regexp.MustCompile(`^(.*)\((\d+)\)$`)
)

const suppSuffix = ".supplemental-metadata"

// SidecarKeys returns the media names a sidecar file name describes, most
// specific first, plus a prefix stem for truncated names ("" if none).
//
//	IMG_1.jpg.supplemental-metadata.json     -> IMG_1.jpg
//	IMG_1.jpg.json                           -> IMG_1.jpg
//	IMG_1.jpg.supplemental-metadata(1).json  -> IMG_1(1).jpg
//	IMG_1.jpg(1).json                        -> IMG_1(1).jpg
//	LongName.jpg.supplemental-metad.json     -> LongName.jpg   (truncated suffix)
//	LongNameTrunc.json                       -> stem "LongNameTrunc" (truncated media name)
func SidecarKeys(sidecarBase string) (keys []string, stem string) {
	s := sidecarBase
	if strings.HasSuffix(strings.ToLower(s), ".json") {
		s = s[:len(s)-len(".json")]
	}
	if m := suppParenRe.FindStringSubmatch(s); m != nil {
		root, ext := SplitExt(m[1])
		return []string{root + "(" + m[2] + ")" + ext}, ""
	}
	if strings.HasSuffix(s, suppSuffix) {
		return []string{strings.TrimSuffix(s, suppSuffix)}, ""
	}
	if m := parenRe.FindStringSubmatch(s); m != nil {
		if _, ext := SplitExt(m[1]); ext != "" {
			root, ext := SplitExt(m[1])
			return []string{root + "(" + m[2] + ")" + ext}, ""
		}
	}
	// truncated ".supplemental-metadata" suffix: "IMG_1.jpg.supplemental-met"
	if i := strings.LastIndex(s, "."); i > 0 {
		seg := s[i+1:]
		if len(seg) >= 1 && len(seg) < len(suppSuffix)-1 && strings.HasPrefix(suppSuffix[1:], seg) {
			base := s[:i]
			return []string{base}, base
		}
	}
	return []string{s}, s
}

type stemEntry struct {
	stem string
	sc   *Sidecar
	key  string
}

// Index maps the media files of one folder to their sidecars.
type Index struct {
	exact   map[string]indexed // lower(media name) -> sidecar
	byTitle map[string][]indexed
	stems   []stemEntry
	used    map[*Sidecar]bool
	all     []indexed
	media   map[string][]string // lower(stem) -> media names of the folder sharing it
}

// AddMedia registers a media file name of the folder, for the rules that
// need to know what else shares a name.
func (ix *Index) AddMedia(mediaBase string) {
	root, _ := SplitExt(mediaBase)
	ix.media[strings.ToLower(root)] = append(ix.media[strings.ToLower(root)], mediaBase)
}

// StemCandidates lists the folder's media files whose stem is key.
func (ix *Index) StemCandidates(key string) []string { return ix.media[strings.ToLower(key)] }

// HasTitleKeys reports whether any registered sidecar is named after a title
// rather than a media file.
func (ix *Index) HasTitleKeys() bool {
	for _, e := range ix.all {
		if IsTitleKey(e.key) {
			return true
		}
	}
	return false
}

type indexed struct {
	sc  *Sidecar
	key string
}

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{exact: map[string]indexed{}, byTitle: map[string][]indexed{}, used: map[*Sidecar]bool{}, media: map[string][]string{}}
}

// Add registers a sidecar under the media names it describes.
func (ix *Index) Add(sidecarBase string, sc *Sidecar) {
	keys, stem := SidecarKeys(sidecarBase)
	for _, k := range keys {
		lk := strings.ToLower(k)
		if _, dup := ix.exact[lk]; !dup {
			ix.exact[lk] = indexed{sc: sc, key: k}
		}
	}
	if stem != "" {
		ix.stems = append(ix.stems, stemEntry{stem: strings.ToLower(stem), sc: sc, key: keys[0]})
	}
	if sc.Title != "" {
		lt := strings.ToLower(sc.Title)
		ix.byTitle[lt] = append(ix.byTitle[lt], indexed{sc: sc, key: keys[0]})
	}
	ix.all = append(ix.all, indexed{sc: sc, key: keys[0]})
}

var stillExts = []string{".jpg", ".jpeg", ".heic", ".png", ".dng", ".webp", ".cr2", ".nef", ".arw"}

// Lookup finds the sidecar for a media file name.
func (ix *Index) Lookup(mediaBase string) (Match, bool) {
	lb := strings.ToLower(mediaBase)
	if e, ok := ix.exact[lb]; ok {
		ix.used[e.sc] = true
		// an edited file is the edited variant even with a sidecar of its own:
		// that sidecar usually carries the original's Google id
		if orig, edited := IsEdited(mediaBase); edited {
			return Match{Sidecar: e.sc, Variant: VariantEdited, Still: orig, Key: e.key}, true
		}
		return Match{Sidecar: e.sc, Variant: VariantOriginal, Still: mediaBase, Key: e.key}, true
	}
	if orig, edited := IsEdited(mediaBase); edited {
		if e, ok := ix.exact[strings.ToLower(orig)]; ok {
			ix.used[e.sc] = true
			return Match{Sidecar: e.sc, Variant: VariantEdited, Still: orig, Key: e.key}, true
		}
	}
	// a sidecar named after a title that has no extension ("11_16_12 - 1.json"
	// for "11_16_12 - 1.jpg": Takeout appends the extension to the media only)
	if root, ext := SplitExt(mediaBase); ext != "" {
		if e, ok := ix.exact[strings.ToLower(root)]; ok && IsTitleKey(e.key) {
			if variant, still, ok := StemRole(mediaBase, ix.StemCandidates(root)); ok {
				ix.used[e.sc] = true
				return Match{Sidecar: e.sc, Variant: variant, Still: still, Key: e.key, Heuristic: true}, true
			}
		}
	}
	// companion: PXL_x.MP -> PXL_x.MP.jpg ; IMG_1.MOV -> IMG_1.HEIC
	for _, still := range CompanionStills(mediaBase) {
		if e, ok := ix.exact[strings.ToLower(still)]; ok {
			ix.used[e.sc] = true
			return Match{Sidecar: e.sc, Variant: VariantMotion, Still: e.key, Key: e.key}, true
		}
	}
	// title equals the media name and is unique
	if cands := ix.byTitle[lb]; len(cands) == 1 && !ix.used[cands[0].sc] {
		ix.used[cands[0].sc] = true
		return Match{Sidecar: cands[0].sc, Variant: VariantOriginal, Still: mediaBase, Key: cands[0].key, Heuristic: true}, true
	}
	// truncated media name: the sidecar stem is a prefix of the media name
	var best *stemEntry
	n := 0
	for i := range ix.stems {
		st := &ix.stems[i]
		if len(st.stem) >= 8 && strings.HasPrefix(lb, st.stem) && !ix.used[st.sc] {
			n++
			if best == nil || len(st.stem) > len(best.stem) {
				best = st
			}
		}
	}
	if best != nil && n == 1 {
		ix.used[best.sc] = true
		return Match{Sidecar: best.sc, Variant: VariantOriginal, Still: mediaBase, Key: best.key, Heuristic: true}, true
	}
	return Match{}, false
}

// Exact returns the sidecar registered under the media name itself (or, for
// an edited file, under its original's name), marking it used; it never uses
// the title or stem heuristics.
func (ix *Index) Exact(mediaBase string) (Match, bool) {
	lb := strings.ToLower(mediaBase)
	if e, ok := ix.exact[lb]; ok {
		ix.used[e.sc] = true
		if orig, edited := IsEdited(mediaBase); edited {
			return Match{Sidecar: e.sc, Variant: VariantEdited, Still: orig, Key: e.key}, true
		}
		return Match{Sidecar: e.sc, Variant: VariantOriginal, Still: mediaBase, Key: e.key}, true
	}
	if orig, edited := IsEdited(mediaBase); edited {
		if e, ok := ix.exact[strings.ToLower(orig)]; ok {
			ix.used[e.sc] = true
			return Match{Sidecar: e.sc, Variant: VariantEdited, Still: orig, Key: e.key}, true
		}
	}
	return Match{}, false
}

// CompanionStills lists the still-image names a companion file could belong to.
func CompanionStills(mediaBase string) []string {
	root, ext := SplitExt(mediaBase)
	lext := strings.ToLower(ext)
	var out []string
	switch lext {
	case ".mp", ".mv", ".mov", ".mp4":
	default:
		return nil
	}
	for _, se := range stillExts {
		out = append(out, mediaBase+se) // PXL_x.MP + .jpg
		if lext == ".mov" || lext == ".mp4" {
			out = append(out, root+se) // IMG_1.MOV -> IMG_1.heic (case-insensitive lookup)
		}
	}
	return out
}

// Unused returns the sidecars that were never matched, with their keys.
func (ix *Index) Unused() []struct {
	Key string
	SC  *Sidecar
} {
	var out []struct {
		Key string
		SC  *Sidecar
	}
	seen := map[*Sidecar]bool{}
	for _, e := range ix.all {
		if ix.used[e.sc] || seen[e.sc] {
			continue
		}
		seen[e.sc] = true
		out = append(out, struct {
			Key string
			SC  *Sidecar
		}{e.key, e.sc})
	}
	return out
}

// LookupKeys returns the media keys under which a sidecar for mediaBase could
// have been stored by an earlier part (for pending_sidecars lookups), each with
// the variant it would imply.
func LookupKeys(mediaBase string) []struct {
	Key     string
	Variant string
	Still   string
} {
	type k = struct {
		Key     string
		Variant string
		Still   string
	}
	orig, edited := IsEdited(mediaBase)
	own := VariantOriginal
	if edited {
		own = VariantEdited // an edited file with a sidecar of its own is still the edited variant
	}
	out := []k{{mediaBase, own, mediaBase}}
	if edited {
		out = append(out, k{orig, VariantEdited, orig})
	}
	for _, s := range CompanionStills(mediaBase) {
		out = append(out, k{s, VariantMotion, s})
	}
	if root, ext := SplitExt(mediaBase); ext != "" {
		// a sidecar named after a title without extension; the caller settles
		// the variant with StemRole against the folder's media
		out = append(out, k{root, VariantOriginal, mediaBase})
	}
	return out
}

// IsStemKey reports whether a lookup key is the media's stem, i.e. the
// candidate for a sidecar named after a title without extension.
func IsStemKey(mediaBase, key string) bool {
	root, ext := SplitExt(mediaBase)
	return ext != "" && strings.EqualFold(root, key)
}

var videoExts = map[string]bool{".mp4": true, ".mov": true, ".mp": true, ".mv": true, ".m4v": true, ".3gp": true, ".avi": true, ".webm": true, ".mkv": true}
var otherMediaExts = map[string]bool{".gif": true, ".tif": true, ".tiff": true, ".bmp": true, ".heif": true, ".avif": true, ".jpe": true, ".jfif": true, ".raw": true, ".orf": true, ".rw2": true, ".pef": true, ".srw": true}

// HasMediaExt reports whether a name ends in a known media extension.
func HasMediaExt(name string) bool {
	return isStill(name) || isVideo(name) || otherMediaExts[strings.ToLower(func() string { _, e := SplitExt(name); return e }())]
}

// IsStillName and IsVideoName classify a media file name by its extension.
func IsStillName(name string) bool { return isStill(name) }
func IsVideoName(name string) bool { return isVideo(name) }

// IsTitleKey reports whether a sidecar key names a title rather than a media
// file: it does not end in a media extension (a title may contain dots).
func IsTitleKey(key string) bool { return !HasMediaExt(key) }

func isStill(name string) bool {
	_, ext := SplitExt(name)
	for _, se := range stillExts {
		if strings.EqualFold(ext, se) {
			return true
		}
	}
	return false
}

func isVideo(name string) bool {
	_, ext := SplitExt(name)
	return videoExts[strings.ToLower(ext)]
}

// StemRole tells what a media file is when a sidecar named after a title
// without extension describes it, given every file of the folder sharing
// that stem (cands, which includes mediaBase): alone, the file is the
// original (or the edited variant); a still with one video are the original
// and its motion companion, whichever the archive lists first; anything else
// (two stills, two videos, three files) is ambiguous and not matched.
func StemRole(mediaBase string, cands []string) (variant, still string, ok bool) {
	switch len(cands) {
	case 0, 1:
		if orig, edited := IsEdited(mediaBase); edited {
			return VariantEdited, orig, true
		}
		return VariantOriginal, mediaBase, true
	case 2:
		var stillName, videoName string
		for _, c := range cands {
			switch {
			case isStill(c) && stillName == "":
				stillName = c
			case isVideo(c) && videoName == "":
				videoName = c
			default:
				return "", "", false
			}
		}
		if stillName == "" || videoName == "" {
			return "", "", false
		}
		if strings.EqualFold(mediaBase, videoName) {
			return VariantMotion, stillName, true
		}
		return VariantOriginal, stillName, true
	}
	return "", "", false
}

// MarkUsed records that a sidecar was consumed (e.g. by promoting an item).
func (ix *Index) MarkUsed(sc *Sidecar) { ix.used[sc] = true }

// Each calls fn for every registered sidecar with its primary key.
func (ix *Index) Each(fn func(key string, sc *Sidecar)) {
	seen := map[*Sidecar]bool{}
	for _, e := range ix.all {
		if seen[e.sc] {
			continue
		}
		seen[e.sc] = true
		fn(e.key, e.sc)
	}
}
