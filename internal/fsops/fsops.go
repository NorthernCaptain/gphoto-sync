// Package fsops holds filesystem helpers: name sanitizing, inode checks,
// free-space queries and idempotent file operations.
package fsops

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode"
)

// SanitizeName makes a file or folder name safe for Linux filesystems while
// keeping it readable: trims spaces/dots, replaces path separators and control
// characters, and caps the length (bytes) keeping the extension.
func SanitizeName(name string, maxBytes int) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r == '/' || r == '\\' || r == 0:
			b.WriteRune('_')
		case unicode.IsControl(r):
			b.WriteRune('_')
		default:
			b.WriteRune(r)
		}
	}
	s := strings.TrimSpace(b.String())
	s = strings.TrimRight(s, ". ")
	s = strings.TrimLeft(s, " ")
	if s == "" || s == "." || s == ".." {
		s = "_"
	}
	if maxBytes > 0 && len(s) > maxBytes {
		ext := filepath.Ext(s)
		if len(ext) > 16 {
			ext = ""
		}
		root := s[:len(s)-len(ext)]
		keep := maxBytes - len(ext)
		for keep > 0 && !isRuneStart(root, keep) {
			keep--
		}
		s = root[:keep] + ext
	}
	return s
}

func isRuneStart(s string, i int) bool {
	return i >= len(s) || (s[i]&0xC0) != 0x80
}

// SameInode reports whether two paths are hardlinks of the same file.
func SameInode(a, b string) bool {
	sa, err := os.Stat(a)
	if err != nil {
		return false
	}
	sb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(sa, sb)
}

// FreeBytes returns the free space on the filesystem holding path.
func FreeBytes(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

// Rename moves src to dst idempotently: if src is gone and dst exists, it is
// treated as already done. Parent directories of dst are created.
func Rename(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if _, err := os.Lstat(src); errors.Is(err, os.ErrNotExist) {
		if _, err2 := os.Lstat(dst); err2 == nil {
			return nil
		}
		return fmt.Errorf("rename %s: source missing and destination absent", src)
	}
	if SameInode(src, dst) {
		return os.Remove(src)
	}
	if _, err := os.Lstat(dst); err == nil {
		return fmt.Errorf("rename %s -> %s: destination exists", src, dst)
	}
	return os.Rename(src, dst)
}

// Link creates a hardlink dst -> src idempotently.
func Link(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if SameInode(src, dst) {
		return nil
	}
	if _, err := os.Lstat(dst); err == nil {
		return fmt.Errorf("link %s -> %s: destination exists", src, dst)
	}
	return os.Link(src, dst)
}

// Symlink creates a relative symlink dst -> src idempotently.
func Symlink(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	rel, err := filepath.Rel(filepath.Dir(dst), src)
	if err != nil {
		return err
	}
	if cur, err := os.Readlink(dst); err == nil {
		if cur == rel {
			return nil
		}
		return fmt.Errorf("symlink %s: exists with another target", dst)
	}
	if _, err := os.Lstat(dst); err == nil {
		return fmt.Errorf("symlink %s: destination exists", dst)
	}
	return os.Symlink(rel, dst)
}

// Copy copies src to dst idempotently: an existing destination with the same
// size and content is left alone, anything else is replaced.
func Copy(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	ss, err := os.Stat(src)
	if err != nil {
		return err
	}
	if ds, err := os.Stat(dst); err == nil && ds.Size() == ss.Size() {
		if same, err := SameContent(src, dst); err == nil && same {
			return nil
		}
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

// SameContent reports whether two files have identical bytes.
func SameContent(a, b string) (bool, error) {
	fa, err := os.Open(a)
	if err != nil {
		return false, err
	}
	defer fa.Close()
	fb, err := os.Open(b)
	if err != nil {
		return false, err
	}
	defer fb.Close()
	ba, bb := make([]byte, 1<<20), make([]byte, 1<<20)
	for {
		na, ea := io.ReadFull(fa, ba)
		nb, eb := io.ReadFull(fb, bb)
		if na != nb || !bytes.Equal(ba[:na], bb[:nb]) {
			return false, nil
		}
		if ea == io.EOF || ea == io.ErrUnexpectedEOF || eb == io.EOF || eb == io.ErrUnexpectedEOF {
			return (ea == io.EOF || ea == io.ErrUnexpectedEOF) && (eb == io.EOF || eb == io.ErrUnexpectedEOF), nil
		}
		if ea != nil {
			return false, ea
		}
		if eb != nil {
			return false, eb
		}
	}
}

// Unlink removes a path idempotently.
func Unlink(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// RemoveEmptyDirs removes dir and its now-empty parents up to (not including) stop.
func RemoveEmptyDirs(dir, stop string) {
	for dir != stop && strings.HasPrefix(dir, stop) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// WriteFileAtomic writes data to path via a temp file and rename.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
