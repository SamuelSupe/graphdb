package storage

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

// Pruning runs after object operations and their publication barriers. Taking
// all IO permits excludes the gap between another writer's mkdir and rename.
// Replication retains its directories until journal recovery/commit completes.
func (s *FileStore) pruneEmptyObjectParents(ctx context.Context, keys []string) (result error) {
	if s.runtime == nil || len(keys) == 0 || IsReplicatedContext(ctx) {
		return nil
	}
	release, err := s.beginLifecycleOperation(ctx)
	if err != nil {
		return err
	}
	defer release()
	var unlock func()
	dirty := make(map[string]struct{})
	syncParents := func() error {
		parents := make([]string, 0, len(dirty))
		for dir := range dirty {
			parents = append(parents, dir)
		}
		sort.Strings(parents)
		err := runIndexWriteJobs(context.Background(), len(parents), func(_ context.Context, index int) error {
			return syncDir(parents[index])
		})
		clear(dirty)
		return err
	}
	defer func() {
		if unlock != nil {
			result = errors.Join(result, syncParents())
			unlock()
		}
	}()
	root, err := filepath.Abs(s.root)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	var started time.Time
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		filename, err := s.path(key)
		if err != nil {
			return err
		}
		// Share the IO gate with foreground work even when a GC batch retires
		// many directories. Never leave a pending directory barrier behind.
		if unlock != nil && (index%64 == 0 || time.Since(started) >= 50*time.Millisecond) {
			if err := syncParents(); err != nil {
				return err
			}
			unlock()
			unlock = nil
			clear(seen)
		}
		if seen[filepath.Dir(filename)] {
			continue
		}
		if unlock == nil {
			// Most control-file deletes leave a populated parent. Avoid queuing
			// a global barrier (which also blocks new readers) in that case.
			empty, err := s.emptyObjectParent(ctx, filename)
			if err != nil {
				return err
			}
			if !empty {
				continue
			}
			unlock, err = s.lockDirectoryIOWeight(ctx, directoryIOCapacity)
			if err != nil {
				return err
			}
			if err := s.syncPendingDirectories(); err != nil {
				return err
			}
			started = time.Now()
			clear(seen)
		}
		for dir := filepath.Dir(filename); dir != root; dir = filepath.Dir(dir) {
			if seen[dir] {
				break
			}
			rel, err := filepath.Rel(root, dir)
			if err != nil {
				return err
			}
			if fileStoreInternalPath(filepath.ToSlash(rel)) {
				break
			}
			s.runtime.mu.Lock()
			changing := s.directoryChangeLocked(filepath.ToSlash(rel)+"/") != nil
			s.runtime.mu.Unlock()
			if changing {
				break
			}
			if err := s.walkSafeDir(dir, false, nil); err != nil {
				if errors.Is(err, ErrNotFound) || os.IsNotExist(err) {
					continue
				}
				return err
			}
			if err := syscall.Rmdir(dir); err != nil {
				if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST) {
					break
				}
				if os.IsNotExist(err) {
					continue
				}
				return err
			}
			seen[dir] = true
			s.caseMu.Lock()
			for _, path := range []string{dir, filepath.Dir(dir)} {
				if cached, ok := s.directorySpellings[path]; ok {
					s.directorySpellingBytes -= cached.bytes
					delete(s.directorySpellings, path)
				}
			}
			s.caseMu.Unlock()
			delete(dirty, dir)
			dirty[filepath.Dir(dir)] = struct{}{}
		}
	}
	return nil
}

// Large replicated deletions should not spend their application deadline on
// empty directories. The bounded queue applies backpressure if pruning falls
// behind. A stopped process can recover abandoned directories on startup.
func (s *FileStore) pruneReplicatedObjectParents(ctx context.Context, keys []string) {
	if s.runtime == nil || len(keys) == 0 {
		return
	}
	if len(keys) <= 8 {
		pruneCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		err := s.pruneEmptyObjectParents(pruneCtx, keys)
		cancel()
		if err == nil {
			return
		}
	}
	sort.Strings(keys)
	runtime := s.runtime
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return
	}
	if runtime.parentCleanup == nil {
		runtime.parentCleanup = make(chan []string, 64)
		cleanup, cancel := context.WithCancel(context.Background())
		runtime.parentCleanupStop = cancel
		runtime.active.Add(1)
		go s.runParentCleanup(cleanup, runtime.parentCleanup)
	}
	select {
	case runtime.parentCleanup <- keys:
		runtime.mu.Unlock()
	default:
		runtime.mu.Unlock()
		pruneCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		err := s.pruneEmptyObjectParents(pruneCtx, keys)
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("replica directory cleanup queue saturated", "error", err)
		}
	}
}

func (s *FileStore) runParentCleanup(ctx context.Context, queue <-chan []string) {
	defer s.runtime.active.Done()
	for keys := range queue {
		for start := 0; start < len(keys) && ctx.Err() == nil; {
			end := min(len(keys), start+32)
			// A newer uncommitted application may have deleted another
			// file here. Its journal still needs the parent directory.
			s.runtime.replicationMu.Lock()
			step, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			err := s.pruneEmptyObjectParents(step, keys[start:end])
			cancel()
			s.runtime.replicationMu.Unlock()
			if errors.Is(err, context.DeadlineExceeded) {
				select {
				case <-ctx.Done():
				case <-time.After(20 * time.Millisecond):
				}
				continue
			}
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrFileStoreClosed) {
				slog.Warn("prune empty replica object directories", "error", err)
			}
			start = end
		}
	}
}

// This is only a hint: rmdir rechecks emptiness under the exclusive IO gate.
// Reading one entry keeps the check bounded even in a very large directory.
func (s *FileStore) emptyObjectParent(ctx context.Context, filename string) (bool, error) {
	unlock, err := s.lockDirectoryIO(ctx)
	if err != nil {
		return false, err
	}
	defer unlock()
	dir := filepath.Dir(filename)
	if err := s.walkSafeDir(dir, false, nil); err != nil {
		if errors.Is(err, ErrNotFound) || os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	file, err := os.Open(dir)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	_, err = file.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
}

// The process owns the data-directory lock and no writers have started yet.
// Walk in bounded batches, do not follow symlinks, and preserve all recovery
// namespaces. This also reclaims directories left by older binaries.
func cleanAbandonedObjectFiles(root string) error {
	var walk func(string, string) error
	walk = func(dir, prefix string) (result error) {
		file, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer file.Close()
		changed := false
		defer func() {
			if changed {
				result = errors.Join(result, syncDir(dir))
			}
		}()
		for {
			entries, err := file.ReadDir(256)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			for _, entry := range entries {
				key := prefix + entry.Name()
				if fileStoreInternalPath(key) {
					continue
				}
				path := filepath.Join(dir, entry.Name())
				if entry.IsDir() {
					if err := walk(path, key+"/"); err != nil {
						return err
					}
					if err := syscall.Rmdir(path); err == nil {
						changed = true
					} else if !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, syscall.EEXIST) {
						return err
					}
				} else if entry.Type().IsRegular() && isFileStoreTemp(path) {
					if err := os.Remove(path); err != nil {
						return err
					}
					changed = true
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
		}
	}
	return walk(root, "")
}
