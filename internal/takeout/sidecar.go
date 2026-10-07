package takeout

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TimeVal is Takeout's {"timestamp": "1695394176", "formatted": "..."} shape.
type TimeVal struct {
	Timestamp string `json:"timestamp"`
	Formatted string `json:"formatted"`
}

// Time converts the epoch-seconds timestamp; ok is false when absent or zero.
func (t *TimeVal) Time() (time.Time, bool) {
	if t == nil || t.Timestamp == "" {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(t.Timestamp, 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(n, 0).UTC(), true
}

// Geo is Takeout's geoData / geoDataExif shape.
type Geo struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude"`
}

// Valid reports whether the coordinates carry a location (Takeout writes zeros for none).
func (g *Geo) Valid() bool {
	return g != nil && (g.Latitude != 0 || g.Longitude != 0)
}

// Person is one entry of the sidecar "people" list.
type Person struct {
	Name string `json:"name"`
}

// Sidecar is a per-item JSON sidecar.
type Sidecar struct {
	Title          string   `json:"title"`
	Description    string   `json:"description"`
	ImageViews     string   `json:"imageViews"`
	CreationTime   *TimeVal `json:"creationTime"`
	PhotoTakenTime *TimeVal `json:"photoTakenTime"`
	GeoData        *Geo     `json:"geoData"`
	GeoDataExif    *Geo     `json:"geoDataExif"`
	People         []Person `json:"people"`
	URL            string   `json:"url"`
	Favorited      bool     `json:"favorited"`
	Archived       bool     `json:"archived"`
	Trashed        bool     `json:"trashed"`

	Raw []byte `json:"-"` // the exact bytes as exported
	// SourcePart is the part that carried this sidecar when it came from the
	// pending store (0 when it is in the part being ingested).
	SourcePart int    `json:"-"`
	SHA256     string `json:"-"` // hash of Raw
}

// ParseSidecar parses a sidecar; unknown fields are kept in Raw only.
func ParseSidecar(b []byte) (*Sidecar, error) {
	var s Sidecar
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	s.Raw = append([]byte(nil), b...)
	sum := sha256.Sum256(b)
	s.SHA256 = hex.EncodeToString(sum[:])
	return &s, nil
}

var photoURLRe = regexp.MustCompile(`photos\.google\.com/photo/([A-Za-z0-9_-]+)`)

// GoogleID extracts the media item id from the sidecar url, or "".
func (s *Sidecar) GoogleID() string {
	if s == nil {
		return ""
	}
	m := photoURLRe.FindStringSubmatch(s.URL)
	if m == nil {
		return ""
	}
	return m[1]
}

// Taken returns photoTakenTime if present.
func (s *Sidecar) Taken() (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	return s.PhotoTakenTime.Time()
}

// Location returns the best location: geoDataExif, then geoData.
func (s *Sidecar) Location() *Geo {
	if s == nil {
		return nil
	}
	if s.GeoDataExif.Valid() {
		return s.GeoDataExif
	}
	if s.GeoData.Valid() {
		return s.GeoData
	}
	return nil
}

// PeopleNames returns the tagged people.
func (s *Sidecar) PeopleNames() []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, len(s.People))
	for _, p := range s.People {
		if p.Name != "" {
			out = append(out, p.Name)
		}
	}
	return out
}

// AlbumMeta is an album folder's metadata.json.
type AlbumMeta struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Access      string   `json:"access"`
	Date        *TimeVal `json:"date"`
	Shared      bool     `json:"-"`
	Raw         []byte   `json:"-"`
}

// ParseAlbumMeta parses metadata.json, including the older {"albumData": {...}} wrapper.
func ParseAlbumMeta(b []byte) (*AlbumMeta, error) {
	var probe struct {
		AlbumData           json.RawMessage `json:"albumData"`
		SharedAlbumComments json.RawMessage `json:"sharedAlbumComments"`
	}
	if err := json.Unmarshal(b, &probe); err != nil {
		return nil, err
	}
	var m AlbumMeta
	src := b
	if len(probe.AlbumData) > 0 {
		src = probe.AlbumData
	}
	if err := json.Unmarshal(src, &m); err != nil {
		return nil, err
	}
	m.Raw = append([]byte(nil), b...)
	m.Title = strings.TrimSpace(m.Title)
	m.Shared = len(probe.SharedAlbumComments) > 0 || (m.Access != "" && !strings.EqualFold(m.Access, "protected"))
	return &m, nil
}

// DateKey returns the album date as a stable string key ("" if absent).
func (m *AlbumMeta) DateKey() string {
	if m == nil {
		return ""
	}
	if t, ok := m.Date.Time(); ok {
		return t.Format(time.RFC3339)
	}
	return ""
}
