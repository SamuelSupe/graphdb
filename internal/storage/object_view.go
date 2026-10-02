package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type ObjectView struct {
	Store   *FileStore
	root    string
	release func()
	once    sync.Once
}

const objectViewMarker = ".graphdb-migration-view"

// CaptureObjectView pins selected immutable files. The caller excludes
// application during capture; later reads and temporary writes use the view.
func (s *FileStore) CaptureObjectView(ctx context.Context, keys []string, budget int64) (*ObjectView, error) {
	release, err := s.beginLifecycleOperation(ctx)
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp(s.root, ".snapshot-view-")
	if err != nil {
		release()
		return nil, err
	}
	view := &ObjectView{root: root, release: release, Store: NewFileStore(filepath.Join(root, "objects"))}
	if err := os.WriteFile(filepath.Join(root, objectViewMarker), []byte("1\n"), 0600); err != nil {
		view.Close()
		return nil, err
	}
	var total int64
	for _, key := range keys {
		if err = ctx.Err(); err != nil {
			break
		}
		var source, destination string
		source, err = s.path(key)
		if err == nil {
			err = s.verifySafeParent(source)
		}
		var info os.FileInfo
		if err == nil {
			info, err = os.Lstat(source)
		}
		if err == nil && !info.Mode().IsRegular() {
			err = fmt.Errorf("captured object is not a regular file: %s", key)
		}
		if err == nil && info.Size() > budget-total {
			err = ErrReplicationSnapshotTooLarge
		}
		if err == nil {
			total += info.Size()
			destination, err = view.Store.path(key)
		}
		if err == nil {
			err = os.MkdirAll(filepath.Dir(destination), 0700)
		}
		if err == nil {
			err = os.Link(source, destination)
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		view.Close()
		return nil, err
	}
	return view, nil
}

func (v *ObjectView) CreateTemp() (*os.File, error) { return os.CreateTemp(v.root, "transfer-") }

func (v *ObjectView) Close() error {
	var err error
	v.once.Do(func() { err = os.RemoveAll(v.root); v.release() })
	return err
}

func removeAbandonedObjectViews(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), ".snapshot-view-") {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		marker := filepath.Join(dir, objectViewMarker)
		info, err := os.Lstat(marker)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != 2 {
			continue
		}
		data, err := os.ReadFile(marker)
		if err != nil {
			return err
		}
		if bytes.Equal(data, []byte("1\n")) {
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
		}
	}
	return nil
}
