//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd

package storage

import "os"

func sameFileChangeTime(_, _ os.FileInfo) bool { return false }
