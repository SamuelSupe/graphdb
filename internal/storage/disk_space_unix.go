//go:build linux || darwin

package storage

import (
	"fmt"
	"golang.org/x/sys/unix"
)

func filesystemSpace(path string) (DiskSpaceStatus, error) {
	var status unix.Statfs_t
	if err := unix.Statfs(path, &status); err != nil {
		return DiskSpaceStatus{}, err
	}
	return DiskSpaceStatus{
		FilesystemID:   fmt.Sprint(status.Fsid),
		TotalBytes:     int64(status.Blocks) * filesystemBlockUnit(status),
		AvailableBytes: int64(status.Bavail) * filesystemBlockUnit(status),
	}, nil
}
