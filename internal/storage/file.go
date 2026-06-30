package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type FileStore struct {
	root        string
	lockMu      sync.Mutex
	objectLocks map[string]*fileObjectLock
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root, objectLocks: map[string]*fileObjectLock{}}
}

func (s *FileStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, err
	}
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	if err := s.verifySafeParent(path); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrNotFound
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return data, err
}

func (s *FileStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	data, err := s.Get(ctx, key)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return data, ObjectMeta{Key: key, ETag: sha256Hex(data), Exists: true}, nil
}

func (s *FileStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	path, err := s.path(key)
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := s.verifySafeParent(path); err != nil {
		return ObjectMeta{Key: key}, err
	}
	etag, exists, err := readFileStoreObjectState(path, true)
	if err != nil {
		return ObjectMeta{Key: key}, err
	}
	if !exists {
		return ObjectMeta{Key: key}, ErrNotFound
	}
	return ObjectMeta{Key: key, ETag: etag, Exists: true}, nil
}

func (s *FileStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *FileStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	path, err := s.path(key)
	if err != nil {
		return ObjectMeta{}, err
	}
	unlock := s.lockObject(key)
	defer unlock()
	if err := s.ensureSafeParent(path); err != nil {
		return ObjectMeta{}, err
	}
	currentETag, exists, err := readFileStoreObjectState(path, fileStorePutNeedsCurrentETag(condition))
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := checkCondition(condition, currentETag, exists); err != nil {
		return ObjectMeta{Key: key, ETag: currentETag, Exists: exists}, err
	}
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key, ETag: currentETag, Exists: exists}, err
	}
	if err := writeFileAtomic(path, data); err != nil {
		return ObjectMeta{}, err
	}
	return ObjectMeta{Key: key, ETag: sha256Hex(data), Exists: true}, nil
}

func (s *FileStore) Delete(ctx context.Context, key string) error {
	return s.DeleteConditional(ctx, key, PutCondition{})
}

func (s *FileStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	path, err := s.path(key)
	if err != nil {
		return err
	}
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	unlock := s.lockObject(key)
	defer unlock()
	if err := s.verifySafeParent(path); err != nil {
		if errors.Is(err, ErrNotFound) {
			if checkErr := checkCondition(condition, "", false); checkErr != nil {
				return checkErr
			}
			return nil
		}
		return err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if checkErr := checkCondition(condition, "", false); checkErr != nil {
			return checkErr
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		if checkErr := checkCondition(condition, "", false); checkErr != nil {
			return checkErr
		}
		return nil
	}
	currentETag, exists, err := readFileStoreObjectState(path, condition.IfMatch != "")
	if err != nil {
		return err
	}
	if err := checkCondition(condition, currentETag, exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	err = os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *FileStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	root, err := s.path("")
	if err != nil {
		return nil, err
	}
	walkRoot, err := s.listWalkRoot(root, prefix)
	if err != nil {
		return nil, err
	}
	items := make([]ObjectInfo, 0)
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return items, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("object directory %q is a symlink", root)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("object root %q is not a directory", root)
	}
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	info, err = os.Lstat(walkRoot)
	if os.IsNotExist(err) {
		return items, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("object list prefix %q is a symlink", prefix)
	}
	if !info.IsDir() {
		return items, nil
	}
	if err := s.walkSafeDir(walkRoot, false); err != nil {
		return nil, err
	}
	err = filepath.WalkDir(walkRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if err := objectContextErr(ctx); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if isFileStoreTemp(path) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if err := objectContextErr(ctx); err != nil {
			return err
		}
		items = append(items, ObjectInfo{Key: key, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})
	return items, nil
}

func (s *FileStore) listWalkRoot(root string, prefix string) (string, error) {
	if prefix == "" {
		return root, nil
	}
	if strings.Contains(prefix, "\\") || filepath.IsAbs(filepath.FromSlash(prefix)) {
		return "", fmt.Errorf("invalid object prefix %q", prefix)
	}
	cleanPrefix := strings.Trim(prefix, "/")
	if cleanPrefix == "" {
		return root, nil
	}
	parts := strings.Split(cleanPrefix, "/")
	for _, part := range parts {
		if part == "." || part == ".." || part == "" {
			return "", fmt.Errorf("invalid object prefix %q", prefix)
		}
	}
	base := cleanPrefix
	if !strings.HasSuffix(prefix, "/") {
		base = pathPrefixDir(cleanPrefix)
	}
	if base == "" {
		return root, nil
	}
	return filepath.Join(root, filepath.FromSlash(base)), nil
}

func pathPrefixDir(prefix string) string {
	index := strings.LastIndex(prefix, "/")
	if index < 0 {
		return ""
	}
	return prefix[:index]
}

func (s *FileStore) path(key string) (string, error) {
	if err := validateFileStoreKey(key); err != nil {
		return "", err
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == "." {
		clean = ""
	}
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("invalid object key %q", key)
	}
	return filepath.Join(s.root, clean), nil
}

func validateFileStoreKey(key string) error {
	if key == "" {
		return nil
	}
	if strings.Contains(key, "\\") || filepath.IsAbs(filepath.FromSlash(key)) {
		return fmt.Errorf("invalid object key %q", key)
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid object key %q", key)
		}
	}
	return nil
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-")
	if err != nil {
		return err
	}
	temp := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(temp)
		}
	}()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temp, 0o644); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	cleanup = false
	return syncDir(dir)
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func isFileStoreTemp(path string) bool {
	return strings.HasPrefix(filepath.Base(path), ".tmp-")
}

func fileStorePutNeedsCurrentETag(condition PutCondition) bool {
	return condition.IfMatch != "" || condition.IfNoneMatch
}
