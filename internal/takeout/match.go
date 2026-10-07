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
}

type indexed struct {
	sc  *Sidecar
	key string
}

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{exact: map[string]indexed{}, byTitle: map[string][]indexed{}, used: map[*Sidecar]bool{}}
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
		return Match{Sidecar: cands[0].sc, Variant: VariantOriginal, Still: mediaBase, Key: cands[0].key}, true
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
		return Match{Sidecar: best.sc, Variant: VariantOriginal, Still: mediaBase, Key: best.key}, true
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
	return out
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
