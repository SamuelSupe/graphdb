//go:build darwin

package storage

import "golang.org/x/sys/unix"

func filesystemBlockUnit(status unix.Statfs_t) int64 { return int64(status.Bsize) }
