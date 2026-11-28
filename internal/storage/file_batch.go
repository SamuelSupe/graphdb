package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type fileBatchKey struct{}

type gcDeleteBatchKey struct{}

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
	return s.runtime.pendingDirectories.drain(&s.runtime.publicationMu, false)
}

func (s *FileStore) syncPendingDeleteDirectories() error {
	if s.runtime == nil {
		return nil
	}
	return s.runtime.pendingDeleteDirectories.drain(&s.runtime.publicationMu, true)
}

type dirSyncWaiter struct {
	done chan struct{}
	err  error
}

type dirSyncState struct {
	dirty    uint64
	synced   uint64
	inFlight *dirSyncWaiter
}

// directorySyncBarrier executes syncDir syscalls outside publicationMu while
// guaranteeing that every concurrent drain() waits for all directories dirtied
// before it started—even if another goroutine already popped them into flight.
type directorySyncBarrier struct {
	mu   sync.Mutex
	dirs map[string]*dirSyncState
}

func (b *directorySyncBarrier) mark(dir string) {
	b.mu.Lock()
	if b.dirs == nil {
		b.dirs = make(map[string]*dirSyncState)
	}
	st := b.dirs[dir]
	if st == nil {
		st = &dirSyncState{}
		b.dirs[dir] = st
	}
	st.dirty++
	b.mu.Unlock()
}

func (b *directorySyncBarrier) pendingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := 0
	for _, st := range b.dirs {
		if st.dirty > st.synced {
			count++
		}
	}
	return count
}

func (b *directorySyncBarrier) drain(publicationMu *sync.Mutex, allowMissing bool) error {
	// Acquire publicationMu briefly only for the initial snapshot so any
	// batched os.Rename/os.Remove that already ran before this drain is
	// guaranteed to have executed mark(dir) before targets are collected.
	// Lock order strictly matches file.go: publicationMu -> b.mu.
	if publicationMu != nil {
		publicationMu.Lock()
	}
	b.mu.Lock()
	var targets map[string]uint64
	for dir, st := range b.dirs {
		if st.dirty > st.synced {
			if targets == nil {
				targets = make(map[string]uint64)
			}
			targets[dir] = st.dirty
		}
	}
	if publicationMu != nil {
		publicationMu.Unlock()
	}
	if len(targets) == 0 {
		b.mu.Unlock()
		return nil
	}

	type syncItem struct {
		dir    string
		gen    uint64
		waiter *dirSyncWaiter
	}
	type waitItem struct {
		dir    string
		waiter *dirSyncWaiter
	}
	var result error
	for len(targets) > 0 {
		var toSync []syncItem
		var toWait []waitItem
		for dir, targetGen := range targets {
			st := b.dirs[dir]
			if st == nil || st.synced >= targetGen {
				delete(targets, dir)
				continue
			}
			if st.inFlight != nil {
				toWait = append(toWait, waitItem{dir: dir, waiter: st.inFlight})
				continue
			}
			w := &dirSyncWaiter{done: make(chan struct{})}
			st.inFlight = w
			toSync = append(toSync, syncItem{dir: dir, gen: st.dirty, waiter: w})
		}
		b.mu.Unlock()

		if len(toSync) > 0 {
			sort.Slice(toSync, func(i, j int) bool { return toSync[i].dir < toSync[j].dir })
			var syncErrMu sync.Mutex
			_ = runIndexWriteJobs(context.Background(), len(toSync), func(_ context.Context, index int) error {
				item := toSync[index]
				err := syncDir(item.dir)
				if allowMissing && os.IsNotExist(err) {
					// For delete barriers, a concurrently pruned/replaced directory
					// is already gone; sync its parent so the removal is durable
					// without permanently poisoning GC.
					err = nil
					if parent := filepath.Dir(item.dir); parent != "" && parent != item.dir {
						if parentErr := syncDir(parent); parentErr != nil && !os.IsNotExist(parentErr) {
							err = parentErr
						}
					}
				}
				item.waiter.err = err
				b.mu.Lock()
				if st := b.dirs[item.dir]; st != nil {
					if err == nil && item.gen > st.synced {
						st.synced = item.gen
					}
					if st.inFlight == item.waiter {
						st.inFlight = nil
					}
					if st.dirty == st.synced && st.inFlight == nil {
						delete(b.dirs, item.dir)
					}
				}
				close(item.waiter.done)
				b.mu.Unlock()
				if err != nil {
					syncErrMu.Lock()
					result = errors.Join(result, err)
					syncErrMu.Unlock()
				}
				return nil
			})
			for _, item := range toSync {
				delete(targets, item.dir)
			}
		}
		for _, item := range toWait {
			<-item.waiter.done
			if item.waiter.err != nil {
				result = errors.Join(result, item.waiter.err)
				delete(targets, item.dir)
			}
		}

		b.mu.Lock()
	}
	b.mu.Unlock()
	return result
}
