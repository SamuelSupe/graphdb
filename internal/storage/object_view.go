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
const objectViewPrefix = ".snapshot-view-graphdb-v1-"

// Every temporary file is beneath the ownership marker directory. Any link
// surviving a crash is therefore identifiable without fsync in the capture lock.
func (s *FileStore) newObjectView(ctx context.Context) (*ObjectView, error) {
	release, err := s.beginLifecycleOperation(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.verifySafeParent(filepath.Join(s.root, ".snapshot-view")); err != nil {
		release()
		return nil, err
	}
	root, err := os.MkdirTemp(s.root, objectViewPrefix)
	if err != nil {
		release()
		return nil, err
	}
	work := filepath.Join(root, objectViewMarker)
	view := &ObjectView{root: root, release: release, Store: NewFileStore(filepath.Join(work, "objects"))}
	if err := os.Mkdir(work, 0700); err != nil {
		view.Close()
		return nil, err
	}
	return view, nil
}

// CaptureObjectView pins selected immutable files. The caller excludes
// application during capture; later reads and temporary writes use the view.
func (s *FileStore) CaptureObjectView(ctx context.Context, keys []string, budget int64) (*ObjectView, error) {
	view, err := s.newObjectView(ctx)
	if err != nil {
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

func (v *ObjectView) workspace() string { return filepath.Join(v.root, objectViewMarker) }

func (v *ObjectView) CreateTemp() (*os.File, error) { return os.CreateTemp(v.workspace(), "transfer-") }

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
		if strings.HasPrefix(entry.Name(), objectViewPrefix) && info.IsDir() {
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
			continue
		}
		// Older migration exports use a regular marker with a small signature.
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
