// Package media extracts capture times from media files: EXIF for images,
// the mvhd box for MP4/MOV videos.
package media

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/evanoberholster/imagemeta"
)

// Sources of a capture time.
const (
	SrcExif  = "exif"
	SrcVideo = "video"
)

var imageExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".heic": true, ".heif": true, ".png": true, ".tif": true, ".tiff": true,
	".dng": true, ".cr2": true, ".cr3": true, ".arw": true, ".nef": true, ".webp": true, ".avif": true,
}

var videoExts = map[string]bool{".mp4": true, ".mov": true, ".m4v": true, ".3gp": true, ".mp": true}

// CaptureTime returns the capture time stored inside the file, and its
// source. EXIF times are wall-clock values with no zone and are returned in
// UTC as-is; callers re-anchor them in the local zone. Video times are UTC.
// A zero time means none was found.
func CaptureTime(path string) (time.Time, string) {
	ext := strings.ToLower(extOf(path))
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, ""
	}
	defer f.Close()
	switch {
	case imageExts[ext]:
		return exifTime(f)
	case videoExts[ext]:
		if t, err := mvhdCreation(f); err == nil && !t.IsZero() {
			return t, SrcVideo
		}
		return time.Time{}, ""
	}
	return time.Time{}, ""
}

func extOf(p string) string {
	i := strings.LastIndex(p, ".")
	if i < 0 {
		return ""
	}
	return p[i:]
}

func exifTime(r io.ReadSeeker) (t time.Time, src string) {
	defer func() {
		if rec := recover(); rec != nil {
			t, src = time.Time{}, ""
		}
	}()
	e, err := imagemeta.Decode(r)
	if err != nil {
		return time.Time{}, ""
	}
	if d := e.OriginalDate(); !d.IsZero() && d.Year() > 1900 {
		return d, SrcExif
	}
	if d := e.SelectedDate(); !d.IsZero() && d.Year() > 1900 {
		return d, SrcExif
	}
	return time.Time{}, ""
}

var mp4Epoch = time.Date(1904, 1, 1, 0, 0, 0, 0, time.UTC)

// mvhdCreation walks the top-level ISO BMFF boxes, finds moov/mvhd and
// returns its creation time.
func mvhdCreation(f *os.File) (time.Time, error) {
	st, err := f.Stat()
	if err != nil {
		return time.Time{}, err
	}
	end := st.Size()
	var off int64
	for off+8 <= end {
		size, typ, hdr, err := readBoxHeader(f, off)
		if err != nil {
			return time.Time{}, err
		}
		if size == 0 {
			size = end - off
		}
		if typ == "moov" {
			return findMvhd(f, off+hdr, off+size)
		}
		if size < hdr {
			return time.Time{}, errors.New("bad box size")
		}
		off += size
	}
	return time.Time{}, errors.New("no moov box")
}

func readBoxHeader(f *os.File, off int64) (size int64, typ string, hdr int64, err error) {
	var b [16]byte
	if _, err := f.ReadAt(b[:8], off); err != nil {
		return 0, "", 0, err
	}
	size = int64(binary.BigEndian.Uint32(b[:4]))
	typ = string(b[4:8])
	hdr = 8
	if size == 1 {
		if _, err := f.ReadAt(b[8:16], off+8); err != nil {
			return 0, "", 0, err
		}
		size = int64(binary.BigEndian.Uint64(b[8:16]))
		hdr = 16
	}
	return size, typ, hdr, nil
}

func findMvhd(f *os.File, off, end int64) (time.Time, error) {
	for off+8 <= end {
		size, typ, hdr, err := readBoxHeader(f, off)
		if err != nil {
			return time.Time{}, err
		}
		if size == 0 {
			size = end - off
		}
		if typ == "mvhd" {
			var b [12]byte
			if _, err := f.ReadAt(b[:], off+hdr); err != nil {
				return time.Time{}, err
			}
			version := b[0]
			var secs uint64
			if version == 1 {
				secs = binary.BigEndian.Uint64(b[4:12])
			} else {
				secs = uint64(binary.BigEndian.Uint32(b[4:8]))
			}
			if secs == 0 {
				return time.Time{}, errors.New("zero creation time")
			}
			t := mp4Epoch.Add(time.Duration(secs) * time.Second)
			if t.Year() < 1970 || t.Year() > 2200 {
				return time.Time{}, errors.New("implausible creation time")
			}
			return t, nil
		}
		if size < hdr {
			return time.Time{}, errors.New("bad box size")
		}
		off += size
	}
	return time.Time{}, errors.New("no mvhd box")
}
