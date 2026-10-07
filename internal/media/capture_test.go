package media

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func box(typ string, payload []byte) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(b[:4], uint32(len(b)))
	copy(b[4:8], typ)
	copy(b[8:], payload)
	return b
}

func TestMvhd(t *testing.T) {
	want := time.Date(2022, 7, 10, 12, 0, 0, 0, time.UTC)
	secs := uint32(want.Sub(mp4Epoch).Seconds())
	mvhd := make([]byte, 100)
	binary.BigEndian.PutUint32(mvhd[4:8], secs)
	data := append(box("ftyp", []byte("isom")), box("moov", box("mvhd", mvhd))...)
	p := filepath.Join(t.TempDir(), "v.mp4")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, src := CaptureTime(p)
	if !got.Equal(want) || src != SrcVideo {
		t.Errorf("got %v %s want %v", got, src, want)
	}
}

func TestNoTime(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.jpg")
	os.WriteFile(p, []byte("not a jpeg"), 0o644)
	if got, _ := CaptureTime(p); !got.IsZero() {
		t.Errorf("expected zero, got %v", got)
	}
}
