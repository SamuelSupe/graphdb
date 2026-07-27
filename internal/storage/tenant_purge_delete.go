package storage

import (
	"context"
	"sync"
)

const tenantPurgeDeleteConcurrency = 8
const tenantPurgeDeletedKeySampleLimit = 100

func (s *TenantStore) deleteTenantPurgePage(
	ctx context.Context,
	tenantID string,
	objects []ObjectInfo,
	generation int64,
) ([]string, error) {
	if len(objects) == 0 {
		return nil, nil
	}
	deleteCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	deleted := make([]bool, len(objects))
	var workers sync.WaitGroup
	var errorMu sync.Mutex
	var firstErr error
	workerCount := min(tenantPurgeDeleteConcurrency, len(objects))
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				if err := s.deleteTenantPurgeObject(
					deleteCtx, tenantID, objects[index], generation,
				); err != nil {
					errorMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errorMu.Unlock()
					continue
				}
				deleted[index] = true
			}
		}()
	}
sendLoop:
	for index := range objects {
		select {
		case jobs <- index:
		case <-deleteCtx.Done():
			break sendLoop
		}
	}
	close(jobs)
	workers.Wait()
	keys := make([]string, 0, len(objects))
	for index, ok := range deleted {
		if ok {
			keys = append(keys, objects[index].Key)
		}
	}
	return keys, firstErr
}

func (r *TenantPurgeReport) recordDeletedKeys(keys []string) {
	remaining := tenantPurgeDeletedKeySampleLimit - len(r.DeletedKeys)
	if remaining > 0 {
		r.DeletedKeys = append(r.DeletedKeys, keys[:min(remaining, len(keys))]...)
	}
	if r.Deleted > len(r.DeletedKeys) {
		r.DeletedKeysTruncated = true
	}
}
