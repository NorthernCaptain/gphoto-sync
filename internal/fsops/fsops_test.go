package fsops

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSanitizeName(t *testing.T) {
	cases := map[string]string{
		"Mortgage payments ": "Mortgage payments",
		"a/b\\c":             "a_b_c",
		"  .hidden. ":        ".hidden",
		"":                   "_",
		"..":                 "_",
		"4 июля в Техасе. ":  "4 июля в Техасе",
	}
	for in, want := range cases {
		if got := SanitizeName(in, 255); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
	long := SanitizeName("ééééééééééééééééééééééééé.jpg", 20)
	if len(long) > 20 || filepath.Ext(long) != ".jpg" {
		t.Errorf("cap failed: %q", long)
	}
}

func TestIdempotentOps(t *testing.T) {
	d := t.TempDir()
	src := filepath.Join(d, "a")
	dst := filepath.Join(d, "sub", "b")
	if err := os.WriteFile(src, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Rename(src, dst); err != nil {
		t.Fatal(err)
	}
	if err := Rename(src, dst); err != nil { // already done
		t.Fatal(err)
	}
	lnk := filepath.Join(d, "l", "c")
	if err := Link(dst, lnk); err != nil {
		t.Fatal(err)
	}
	if err := Link(dst, lnk); err != nil {
		t.Fatal(err)
	}
	if !SameInode(dst, lnk) {
		t.Error("not same inode")
	}
	if err := Unlink(lnk); err != nil {
		t.Fatal(err)
	}
	if err := Unlink(lnk); err != nil {
		t.Fatal(err)
	}
	if n, err := FreeBytes(d); err != nil || n == 0 {
		t.Errorf("free bytes: %d %v", n, err)
	}
}
