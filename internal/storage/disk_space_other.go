//go:build !linux && !darwin

package storage

import "fmt"

func filesystemSpace(path string) (DiskSpaceStatus, error) {
	return DiskSpaceStatus{}, fmt.Errorf("disk space inspection is supported on Linux and macOS")
}
