//go:build linux

package storage

import "golang.org/x/sys/unix"

func filesystemBlockUnit(status unix.Statfs_t) int64 {
	if status.Frsize > 0 {
		return int64(status.Frsize)
	}
	return int64(status.Bsize)
}
