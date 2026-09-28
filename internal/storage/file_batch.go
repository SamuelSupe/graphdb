package storage

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type fileBatchKey struct{}

type fileBatchBytesKey struct{}

// Budgets are checked between complete files. At most four in-flight files may
// cross a budget; publishing a catalog still waits for all directory barriers.
func (s *TenantStore) runFileWriteJobs(ctx context.Context, count int, fn func(context.Context, int) error) error {
	files := s.localFileStore()
	if files == nil {
		return runIndexWriteJobs(ctx, count, fn)
	}
	for start := 0; start < count; {
		started := time.Now()
		var bytes atomic.Int64
		batchCtx := context.WithValue(ctx, fileBatchKey{}, files)
		batchCtx = context.WithValue(batchCtx, fileBatchBytesKey{}, &bytes)
		var mu sync.Mutex
		next := start
		end := min(count, start+64)
		err := runIndexWriteJobs(batchCtx, min(indexWriteConcurrency, end-start), func(ctx context.Context, _ int) error {
			for {
				mu.Lock()
				if next >= end || (next > start && (bytes.Load() >= 16<<20 || time.Since(started) >= 50*time.Millisecond)) {
					mu.Unlock()
					return nil
				}
				index := next
				next++
				mu.Unlock()
				err := func() error {
					if !acquireTaskSlot(ctx, s.maintenance.encodes) {
						return ctx.Err()
					}
					defer releaseTaskSlot(s.maintenance.encodes)
					return fn(ctx, index)
				}()
				if err != nil {
					return err
				}
			}
		})
		syncStarted := time.Now()
		err = errors.Join(err, files.syncPendingDirectories())
		s.recordMaintenance("file_batch_sync", syncStarted)
		if err != nil {
			return err
		}
		start = next
	}
	return objectContextErr(ctx)
}

func (s *FileStore) syncPendingDirectories() error {
	if s.runtime == nil {
		return nil
	}
	r := s.runtime
	r.publicationMu.Lock()
	defer r.publicationMu.Unlock()
	dirs := make([]string, 0, len(r.pendingDirectories))
	for dir := range r.pendingDirectories {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	var result error
	for _, dir := range dirs {
		if err := syncDir(dir); err != nil {
			result = errors.Join(result, err)
		} else {
			delete(r.pendingDirectories, dir)
		}
	}
	return result
}
