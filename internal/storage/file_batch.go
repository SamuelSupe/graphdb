package storage

import (
	"context"
	"errors"
	"sort"
)

type fileBatchKey struct{}

// File data is synced before rename. Directory barriers may be shared by up to
// 64 jobs; a catalog can be published only after all of its data is durable.
func (s *TenantStore) runFileWriteJobs(ctx context.Context, count int, fn func(context.Context, int) error) error {
	files := s.localFileStore()
	if files == nil {
		return runIndexWriteJobs(ctx, count, fn)
	}
	for start := 0; start < count; start += 64 {
		batchCtx := context.WithValue(ctx, fileBatchKey{}, files)
		err := runIndexWriteJobs(batchCtx, min(64, count-start), func(ctx context.Context, index int) error { return fn(ctx, start+index) })
		// Flush successful renames even if another job failed or was canceled.
		err = errors.Join(err, files.syncPendingDirectories())
		if err != nil {
			return err
		}
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
