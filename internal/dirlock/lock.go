package dirlock

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

var ErrLocked = errors.New("directory is in use")

// Acquire locks a persistent file inode until the returned file is closed.
// Callers must retain the file rather than replace it during publication.
func Acquire(filename string) (*os.File, error) {
	fd, err := syscall.Open(filename, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filename)
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("%w: %s: %v", ErrLocked, filename, err)
	}
	return file, nil
}
