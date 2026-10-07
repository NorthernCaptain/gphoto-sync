package fsops

import (
	"fmt"
	"os"
	"syscall"
)

// Lock takes an exclusive, non-blocking advisory lock on path (created if
// missing). The lock lives as long as the returned file is open: callers keep
// it for the whole process and let exit release it. A held lock yields an
// error naming the path.
func Lock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("another gphoto-sync writer holds %s", path)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return f, nil
}
