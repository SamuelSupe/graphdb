package storage

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type concurrentSegmentReadStore struct {
	ObjectStore
	entered chan struct{}
	release chan struct{}
	active  atomic.Int64
	max     atomic.Int64
}

type concurrentCommitTailReadStore struct {
	ObjectStore
	firstKey     string
	entered      chan string
	releaseFirst chan struct{}
	releaseLater chan struct{}
	completed    chan string
	active       atomic.Int64
	max          atomic.Int64
}

func (s *concurrentSegmentReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if !strings.Contains(key, "/commits/segments/") {
		return s.ObjectStore.Get(ctx, key)
	}
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		current := s.max.Load()
		if active <= current || s.max.CompareAndSwap(current, active) {
			break
		}
	}
	select {
	case s.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *concurrentCommitTailReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if !strings.Contains(key, "/commits/") || strings.Contains(key, "/commits/segments/") {
		return s.ObjectStore.Get(ctx, key)
	}
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		current := s.max.Load()
		if active <= current || s.max.CompareAndSwap(current, active) {
			break
		}
	}
	select {
	case s.entered <- key:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := s.releaseLater
	if key == s.firstKey {
		release = s.releaseFirst
	}
	select {
	case <-release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	data, err := s.ObjectStore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	select {
	case s.completed <- key:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return data, nil
}

func TestTenantStoreSegmentsCommitTailAndLoadsAfterLooseCleanup(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host", Fields: graph.Fields{"seq": i},
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(manifest.CommitSegments) != 1 || len(manifest.CommitKeys) != 0 {
		t.Fatalf("manifest segments=%#v keys=%#v", manifest.CommitSegments, manifest.CommitKeys)
	}
	if got := ManifestCommitTailLength(manifest); got != commitSegmentTargetCount {
		t.Fatalf("tail length=%d want %d", got, commitSegmentTargetCount)
	}
	if manifest.CommitSegments[0].Codec != commitSegmentCodecParquet ||
		!strings.HasSuffix(manifest.CommitSegments[0].Key, ".parquet") ||
		manifest.CommitSegments[0].Count != commitSegmentTargetCount ||
		manifest.CommitSegments[0].ContentHash == "" {
		t.Fatalf("segment ref=%#v", manifest.CommitSegments[0])
	}
	gc, err := store.RunGC(ctx, "tenant-a", GCOptions{})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if gc.CommitCleanup.Deleted == 0 {
		t.Fatalf("gc did not remove loose commit objects: %#v", gc.CommitCleanup)
	}
	store.deleteWriteCache("tenant-a")
	loaded, loadedManifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loadedManifest.Version != int64(commitSegmentTargetCount) {
		t.Fatalf("loaded version=%d", loadedManifest.Version)
	}
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, ok := loaded.GetEntity(fmt.Sprintf("host:%03d", i)); !ok {
			t.Fatalf("missing host:%03d", i)
		}
	}
}

func TestCommitTailSegmentationReusesDecodedWriterTail(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	for i := 0; i < commitSegmentTargetCount-1; i++ {
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: fmt.Sprintf("host:%03d", i), Kind: "host",
			}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	objects.Reset()
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:boundary", Kind: "host",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("segment boundary commit: %v", err)
	}
	if got := objects.CountContains("/commits/"); got != 0 {
		t.Fatalf("segment boundary reread %d loose commit objects, want 0", got)
	}
}

func TestCommitTailSegmentationReusesTailLoadedAfterCacheMiss(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	for i := 0; i < commitSegmentTargetCount-1; i++ {
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: fmt.Sprintf("host:%03d", i), Kind: "host",
			}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	store.deleteWriteCache("tenant-a")
	objects.Reset()
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:boundary", Kind: "host",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("segment boundary commit: %v", err)
	}
	if got := objects.CountContains("/commits/"); got != commitSegmentTargetCount-1 {
		t.Fatalf(
			"cache-miss boundary read %d loose commit objects, want one load of %d",
			got, commitSegmentTargetCount-1,
		)
	}
}

func TestTenantStoreLoadsCommitSegmentsConcurrentlyAndAppliesInOrder(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	for i := 0; i < commitSegmentTargetCount*2; i++ {
		if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: "host:ordered", Kind: "host",
				Fields: graph.Fields{"sequence": fmt.Sprintf("%03d", i)},
			}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	objects := &concurrentSegmentReadStore{
		ObjectStore: base,
		entered:     make(chan struct{}, 2),
		release:     make(chan struct{}),
	}
	reader := NewTenantStore(objects, "test")
	wantSequence := fmt.Sprintf("%03d", commitSegmentTargetCount*2-1)
	loadCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		g, manifest, err := reader.Load(loadCtx, "tenant-a")
		if err == nil {
			ordered := g.Entities["host:ordered"]
			if manifest.Version != int64(commitSegmentTargetCount*2) || ordered.Fields["sequence"] != wantSequence {
				err = fmt.Errorf("loaded version/final sequence = %d/%v", manifest.Version, ordered.Fields["sequence"])
			}
		}
		done <- err
	}()
	for range 2 {
		select {
		case <-objects.entered:
		case <-loadCtx.Done():
			close(objects.release)
			t.Fatal("commit segments were not loaded concurrently")
		}
	}
	close(objects.release)
	if err := <-done; err != nil {
		t.Fatalf("load: %v", err)
	}
	if objects.max.Load() < 2 {
		t.Fatalf("max concurrent segment reads = %d, want at least 2", objects.max.Load())
	}
}

func TestTenantStoreLoadsCommitTailConcurrentlyAndAppliesInOrder(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	commitCount := commitTailLoadConcurrency * 2
	for i := 0; i < commitCount; i++ {
		if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: "host:ordered", Kind: "host",
				Fields: graph.Fields{"sequence": fmt.Sprintf("%03d", i)},
			}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	manifest, err := writer.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(manifest.CommitSegments) != 0 || len(manifest.CommitKeys) != commitCount {
		t.Fatalf("manifest segments/tail=%#v/%#v", manifest.CommitSegments, manifest.CommitKeys)
	}
	objects := &concurrentCommitTailReadStore{
		ObjectStore:  base,
		firstKey:     manifest.CommitKeys[0],
		entered:      make(chan string, commitCount),
		releaseFirst: make(chan struct{}),
		releaseLater: make(chan struct{}),
		completed:    make(chan string, commitCount),
	}
	reader := NewTenantStore(objects, "test")
	loadCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		g, loadedManifest, err := reader.Load(loadCtx, "tenant-a")
		if err == nil {
			ordered, ok := g.GetEntity("host:ordered")
			if !ok {
				err = fmt.Errorf("ordered entity is missing")
			} else if loadedManifest.Version != int64(commitCount) ||
				ordered.Fields["sequence"] != fmt.Sprintf("%03d", commitCount-1) {
				err = fmt.Errorf(
					"loaded version/final sequence = %d/%v",
					loadedManifest.Version,
					ordered.Fields["sequence"],
				)
			}
		}
		done <- err
	}()
	for range commitTailLoadConcurrency {
		select {
		case <-objects.entered:
		case <-loadCtx.Done():
			close(objects.releaseFirst)
			t.Fatal("commit tail reads did not reach the concurrency window")
		}
	}
	close(objects.releaseLater)
	for range commitTailLoadConcurrency - 1 {
		select {
		case <-objects.completed:
		case <-loadCtx.Done():
			close(objects.releaseFirst)
			t.Fatal("out-of-order commit tail reads did not complete")
		}
	}
	if got := objects.max.Load(); got > int64(commitTailLoadConcurrency) {
		t.Fatalf("max concurrent commit tail reads = %d, want at most %d", got, commitTailLoadConcurrency)
	}
	if got := objects.max.Load(); got < 2 {
		t.Fatalf("max concurrent commit tail reads = %d, want at least 2", got)
	}
	select {
	case err := <-done:
		t.Fatalf("load completed before the first manifest commit was released: %v", err)
	default:
	}
	close(objects.releaseFirst)
	if err := <-done; err != nil {
		t.Fatalf("load: %v", err)
	}
}

func TestNonParquetCommitSegmentIsRejected(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < 2; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	nonParquetKey := store.commitSegmentPrefix("tenant-a") + "non-parquet-segment.parquet"
	data := []byte(`{"kind":"commit-segment","codec":"commit-segment-ndjson-v1"}`)
	if err := store.Objects.Put(ctx, nonParquetKey, data); err != nil {
		t.Fatalf("put non-parquet segment: %v", err)
	}
	_, err = store.loadCommitSegment(ctx, "tenant-a", CommitSegmentRef{
		Key:          nonParquetKey,
		Codec:        "commit-segment-ndjson-v1",
		FirstVersion: manifest.Version - 1,
		LastVersion:  manifest.Version,
		Count:        len(manifest.CommitKeys),
	})
	if err == nil || !strings.Contains(err.Error(), "only parquet segments") {
		t.Fatalf("load non-parquet segment err=%v", err)
	}
}

func TestIndexInspectIncludesCommitSegmentsWithoutCatalog(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host", Fields: graph.Fields{"seq": i},
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	inspection, err := store.InspectIndex(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspection.TenantID != "tenant-a" || inspection.Version != int64(commitSegmentTargetCount) {
		t.Fatalf("inspection header = %#v", inspection)
	}
	var segment IndexInspectionObject
	for _, object := range inspection.Objects {
		if object.Role == "commit_segment" {
			segment = object
			break
		}
	}
	if segment.Key == "" {
		t.Fatalf("inspection has no commit segment: %#v", inspection.Objects)
	}
	if segment.ObjectKind != "commit-segment" ||
		segment.Format != IndexFormatParquet ||
		segment.Codec != commitSegmentCodecParquet ||
		segment.RowCount != commitSegmentTargetCount ||
		segment.FirstVersion != 1 ||
		segment.LastVersion != int64(commitSegmentTargetCount) ||
		segment.ExpectedHash == "" ||
		segment.ContentHash != segment.ExpectedHash ||
		!segment.HashMatches ||
		segment.PayloadBytes == 0 {
		t.Fatalf("segment inspection = %#v", segment)
	}
}

func TestCompactClearsCommitSegmentsAndGCDeletesSegment(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	before, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(before.CommitSegments) != 1 {
		t.Fatalf("segments before compact=%#v", before.CommitSegments)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	after, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest after compact: %v", err)
	}
	if len(after.CommitSegments) != 0 || len(after.CommitKeys) != 0 {
		t.Fatalf("tail after compact segments=%#v keys=%#v", after.CommitSegments, after.CommitKeys)
	}
	gc, err := store.RunGC(ctx, "tenant-a", GCOptions{})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	foundSegmentDelete := false
	for _, key := range gc.DeletedKeys {
		if key == before.CommitSegments[0].Key {
			foundSegmentDelete = true
			break
		}
	}
	if !foundSegmentDelete {
		t.Fatalf("segment %q was not deleted by gc: %#v", before.CommitSegments[0].Key, gc.DeletedKeys)
	}
}
