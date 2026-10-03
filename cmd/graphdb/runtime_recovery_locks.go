package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/dirlock"
)

func runtimeLockObject(role, relative string) bool { return role != "data" && relative == ".lock" }

func runtimeStateFiles(names []string) []string {
	files := make([]string, 0, len(names))
	for _, name := range names {
		role, relative, _ := strings.Cut(name, "/")
		if !runtimeLockObject(role, relative) {
			files = append(files, name)
		}
	}
	return files
}

func closeRuntimeLocks(locks []*os.File) {
	for _, lock := range locks {
		lock.Close()
	}
}

func lockRuntimeRaftDB(root string) (*os.File, error) {
	return dirlock.Acquire(filepath.Join(root, "raft.db"))
}

func lockRuntimeAuxiliaryRoots(roots map[string]string, restore bool) (locks []*os.File, err error) {
	defer func() {
		if err != nil {
			closeRuntimeLocks(locks)
		}
	}()
	for _, role := range []string{"raft", "wal"} {
		root := roots[role]
		if root == "" {
			continue
		}
		if restore {
			if err := os.MkdirAll(root, 0700); err != nil {
				return locks, err
			}
		}
		info, err := os.Lstat(root)
		if err != nil {
			return locks, err
		}
		if !info.IsDir() {
			return locks, fmt.Errorf("runtime %s root is not a directory", role)
		}
		lock, err := dirlock.Acquire(filepath.Join(root, ".lock"))
		if err != nil {
			return locks, fmt.Errorf("lock runtime %s root: %w", role, err)
		}
		locks = append(locks, lock)
		if role == "raft" {
			if _, err := os.Lstat(filepath.Join(root, "raft.db")); err == nil {
				lock, err := lockRuntimeRaftDB(root)
				if err != nil {
					return locks, fmt.Errorf("lock runtime Raft database: %w", err)
				}
				locks = append(locks, lock)
			} else if !os.IsNotExist(err) {
				return locks, err
			}
		}
	}
	return locks, nil
}
