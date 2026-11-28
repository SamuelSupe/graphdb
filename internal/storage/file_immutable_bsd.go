//go:build darwin || freebsd || netbsd || openbsd

package storage

import (
	"os"
	"syscall"
)

func sameFileChangeTime(a, b os.FileInfo) bool {
	left, leftOK := a.Sys().(*syscall.Stat_t)
	right, rightOK := b.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && left.Ctimespec == right.Ctimespec
}
