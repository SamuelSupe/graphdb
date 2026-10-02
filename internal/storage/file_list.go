package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type fileListBeforeKey struct{}
type fileListDirectoriesKey struct{}
type fileListDirectories map[string][]os.DirEntry

// ListPage retains only one page and the entries of directories on its path.
// Completed subtrees are skipped using the exclusive object-key cursor.
func (s *FileStore) ListPage(ctx context.Context, prefix, after string, limit int) (result []ObjectInfo, next string, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	release, err := s.beginOperation(ctx, prefix)
	if err != nil {
		return nil, "", err
	}
	defer release()
	root, err := s.path("")
	if err != nil {
		return nil, "", err
	}
	walkRoot, err := s.listWalkRoot(root, prefix)
	if err != nil {
		return nil, "", err
	}
	if err := s.walkSafeDir(walkRoot, false, nil); err != nil {
		if errors.Is(err, ErrNotFound) || os.IsNotExist(err) {
			return []ObjectInfo{}, "", nil
		}
		return nil, "", err
	}
	before, _ := ctx.Value(fileListBeforeKey{}).(time.Time)
	// A GC sweep reuses sorted entries along the current path. This avoids
	// rescanning a large flat directory for every page without caching a tree.
	directories, _ := ctx.Value(fileListDirectoriesKey{}).(fileListDirectories)
	if limit <= 0 {
		directories = nil
	}
	for dir := range directories {
		if dir != walkRoot && !strings.HasPrefix(dir, walkRoot+string(filepath.Separator)) && !strings.HasPrefix(walkRoot, dir+string(filepath.Separator)) {
			delete(directories, dir)
		}
	}
	items := make([]ObjectInfo, 0)
	var walk func(string) error
	walk = func(dir string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A directory's slash participates in object-key ordering: a.parquet
		// must precede a/part.parquet, even though ReadDir sorts "a" first.
		name := func(e os.DirEntry) string {
			if e.IsDir() {
				return e.Name() + "/"
			}
			return e.Name()
		}
		entries, cached := directories[dir]
		if !cached {
			entries, err = os.ReadDir(dir)
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			sort.Slice(entries, func(i, j int) bool { return name(entries[i]) < name(entries[j]) })
			if directories != nil {
				directories[dir] = entries
			}
		}
		relDir, err := filepath.Rel(root, dir)
		if err != nil {
			return err
		}
		dirPrefix := ""
		if relDir != "." {
			dirPrefix = filepath.ToSlash(relDir) + "/"
		}
		start := 0
		if strings.HasPrefix(after, dirPrefix) {
			seek := strings.TrimPrefix(after, dirPrefix)
			if slash := strings.IndexByte(seek, '/'); slash >= 0 {
				seek = seek[:slash+1]
			}
			start = sort.Search(len(entries), func(i int) bool { return name(entries[i]) >= seek })
		}
		for _, entry := range entries[start:] {
			if err := ctx.Err(); err != nil {
				return err
			}
			path := filepath.Join(dir, entry.Name())
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			key := filepath.ToSlash(rel)
			if fileStoreInternalPath(key) || (!entry.IsDir() && isFileStoreTemp(path)) {
				continue
			}
			if entry.IsDir() {
				subtree := key + "/"
				if (!strings.HasPrefix(subtree, prefix) && !strings.HasPrefix(prefix, subtree)) ||
					(subtree < after && !strings.HasPrefix(after, subtree)) {
					continue
				}
				if err := walk(path); err != nil {
					return err
				}
				continue
			}
			if key <= after || !strings.HasPrefix(key, prefix) {
				continue
			}
			info, err := entry.Info()
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || (!before.IsZero() && !info.ModTime().Before(before)) {
				continue
			}
			items = append(items, ObjectInfo{Key: key, Size: info.Size()})
			if limit > 0 && len(items) > limit {
				return filepath.SkipAll
			}
		}
		delete(directories, dir)
		return nil
	}
	if err := walk(walkRoot); err != nil && !errors.Is(err, filepath.SkipAll) {
		return nil, "", fmt.Errorf("list %q: %w", prefix, err)
	}
	if limit > 0 && len(items) > limit {
		return items[:limit], items[limit-1].Key, nil
	}
	return items, "", nil
}
