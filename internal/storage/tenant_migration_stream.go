package storage

import (
	"context"
	"strings"
	"sync"
)

type tenantMigrationObjectWriter func(context.Context, string, []byte) error

const tenantMigrationCopyConcurrency = 8

type tenantMigrationPendingObject struct {
	object      ObjectInfo
	targetKey   string
	sampleIndex int
}

func (s *TenantStore) copyTenantMigrationSegments(
	ctx context.Context,
	tenantID string,
	targetPrefix string,
	manifest Manifest,
	write tenantMigrationObjectWriter,
) (map[string]string, map[string]string, error) {
	sourcePrefix := s.tenantObjectPrefix(tenantID)
	segmentHashes := make(map[string]string, len(manifest.CommitSegments))
	objectHashes := make(map[string]string, len(manifest.CommitSegments))
	for _, ref := range manifest.CommitSegments {
		if _, copied := segmentHashes[ref.Key]; copied {
			continue
		}
		data, err := s.Objects.Get(ctx, ref.Key)
		if err != nil {
			return nil, nil, err
		}
		rewritten, hash, err := rewriteCommitSegmentObject(
			ctx, data, tenantID, sourcePrefix, targetPrefix,
		)
		if err != nil {
			return nil, nil, err
		}
		targetKey := rewriteTenantObjectKey(ref.Key, sourcePrefix, targetPrefix)
		if err := write(ctx, targetKey, rewritten); err != nil {
			return nil, nil, err
		}
		segmentHashes[ref.Key] = hash
		objectHashes[ref.Key] = objectContentHash(rewritten)
	}
	return segmentHashes, objectHashes, nil
}

func (s *TenantStore) tenantMigrationObjectPayload(
	ctx context.Context,
	object ObjectInfo,
	tenantID string,
	targetTenantID string,
	targetPrefix string,
	segmentHashes map[string]string,
	targetFence writerFenceRef,
) ([]byte, error) {
	data, err := s.Objects.Get(ctx, object.Key)
	if err != nil {
		return nil, err
	}
	sourcePrefix := s.tenantObjectPrefix(tenantID)
	relative := strings.TrimPrefix(object.Key, sourcePrefix)
	if !tenantMigrationObjectNeedsRewrite(relative) {
		return data, nil
	}
	rewritten, changed, err := s.rewriteTenantMigrationObject(
		ctx,
		data,
		object.Key,
		tenantID,
		targetTenantID,
		sourcePrefix,
		targetPrefix,
		segmentHashes,
		targetFence,
	)
	if err != nil {
		return nil, err
	}
	if changed {
		return rewritten, nil
	}
	return data, nil
}

func (s *TenantStore) copyTenantMigrationPage(
	ctx context.Context,
	tenantID string,
	targetTenantID string,
	targetPrefix string,
	segmentHashes map[string]string,
	targetFence writerFenceRef,
	objects []tenantMigrationPendingObject,
	write tenantMigrationObjectWriter,
) ([]string, []bool, error) {
	if len(objects) == 0 {
		return nil, nil, nil
	}
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	hashes := make([]string, len(objects))
	copied := make([]bool, len(objects))
	var workers sync.WaitGroup
	var errorMu sync.Mutex
	var firstErr error
	workerCount := min(tenantMigrationCopyConcurrency, len(objects))
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				pending := objects[index]
				data, err := s.tenantMigrationObjectPayload(
					copyCtx,
					pending.object,
					tenantID,
					targetTenantID,
					targetPrefix,
					segmentHashes,
					targetFence,
				)
				if err == nil {
					err = write(copyCtx, pending.targetKey, data)
				}
				if err != nil {
					errorMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errorMu.Unlock()
					continue
				}
				hashes[index] = objectContentHash(data)
				copied[index] = true
			}
		}()
	}
sendLoop:
	for index := range objects {
		select {
		case jobs <- index:
		case <-copyCtx.Done():
			break sendLoop
		}
	}
	close(jobs)
	workers.Wait()
	return hashes, copied, firstErr
}
