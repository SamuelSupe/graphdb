package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestDurableBatchPublishesOneSegmentAndManifest(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.reset()

	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.CommitSegments) != 1 || len(manifest.CommitKeys) != 0 || manifest.CommitSegments[0].Count != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	if puts := objects.count(store.manifestKey("tenant-a")); puts != 1 {
		t.Fatalf("manifest PUTs = %d, want 1", puts)
	}
	if puts := objects.countPrefix(store.commitSegmentPrefix("tenant-a")); puts != 1 {
		t.Fatalf("segment PUTs = %d, want 1", puts)
	}
	if puts := objects.countLooseCommits(store.commitPrefix("tenant-a"), store.commitSegmentPrefix("tenant-a")); puts != 0 {
		t.Fatalf("loose commit PUTs = %d, want 0", puts)
	}
}

func TestIngestDurableBatchMetadataAggregatesCollectorStatusUpdates(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	store.MaterializeCollectorStatus = true
	ctx := context.Background()
	started := time.Unix(10, 0).UTC()
	candidates := []*ingestBatchCandidate{
		{
			request: IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "batch-a-1", Cursor: "cursor-a-1",
			},
			result:  IngestResult{BatchID: "batch-a-1", Version: 1, Applied: 2},
			started: started,
		},
		{
			request: IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "batch-a-2", Cursor: "cursor-a-2",
			},
			result:  IngestResult{BatchID: "batch-a-2", Version: 2, Applied: 1},
			started: started.Add(time.Second),
		},
		{
			request: IngestRequest{
				Source: "agent", CollectorID: "collector-b", BatchID: "batch-b-1", Cursor: "cursor-b-1",
			},
			result:  IngestResult{BatchID: "batch-b-1", Version: 3, Applied: 4},
			started: started.Add(2 * time.Second),
		},
	}
	if err := store.saveIngestBatchResultMetadata(ctx, "tenant-a", candidates); err != nil {
		t.Fatalf("save aggregated ingest metadata: %v", err)
	}

	for _, candidate := range candidates {
		key := store.ingestBatchKey("tenant-a", candidate.request.Source, candidate.request.CollectorID, candidate.request.BatchID)
		if got := objects.count(key); got != 1 {
			t.Fatalf("ingest record %q PUTs = %d, want 1", key, got)
		}
	}
	collectorAKey := store.collectorStatusKey("tenant-a", "agent", "collector-a")
	collectorBKey := store.collectorStatusKey("tenant-a", "agent", "collector-b")
	if got := objects.count(collectorAKey); got != 1 {
		t.Fatalf("collector-a status PUTs = %d, want one aggregated write", got)
	}
	if got := objects.count(collectorBKey); got != 1 {
		t.Fatalf("collector-b status PUTs = %d, want one write", got)
	}

	statusA, err := decodeParquetCollectorStatus(ctx, mustGetObject(t, ctx, store, collectorAKey))
	if err != nil {
		t.Fatalf("decode collector-a status: %v", err)
	}
	if statusA.LastBatchID != "batch-a-2" || statusA.LastCursor != "cursor-a-2" ||
		statusA.LastVersion != 2 || statusA.AppliedTotal != 3 || statusA.FailedTotal != 0 {
		t.Fatalf("collector-a status = %#v, want aggregate through version 2", statusA)
	}
	statusB, err := decodeParquetCollectorStatus(ctx, mustGetObject(t, ctx, store, collectorBKey))
	if err != nil {
		t.Fatalf("decode collector-b status: %v", err)
	}
	if statusB.LastBatchID != "batch-b-1" || statusB.LastCursor != "cursor-b-1" ||
		statusB.LastVersion != 3 || statusB.AppliedTotal != 4 || statusB.FailedTotal != 0 {
		t.Fatalf("collector-b status = %#v, want version 3", statusB)
	}
}

func TestIngestDurableBatchMetadataDAGOverlapsIndependentCollectorStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	objects := &ingestMetadataDAGStore{
		ObjectStore:  NewMemoryStore(),
		slowStarted:  make(chan struct{}),
		releaseSlow:  make(chan struct{}),
		statusWrites: make(chan string, 4),
	}
	store := NewTenantStore(objects, "test")
	store.MaterializeCollectorStatus = true
	slow := &ingestBatchCandidate{
		request: IngestRequest{
			Source: "agent", CollectorID: "collector-slow", BatchID: "batch-slow", Cursor: "cursor-slow",
		},
		result:  IngestResult{BatchID: "batch-slow", Version: 1, Applied: 1},
		started: time.Unix(30, 0).UTC(),
	}
	fast := &ingestBatchCandidate{
		request: IngestRequest{
			Source: "agent", CollectorID: "collector-fast", BatchID: "batch-fast", Cursor: "cursor-fast",
		},
		result:  IngestResult{BatchID: "batch-fast", Version: 1, Applied: 1},
		started: time.Unix(31, 0).UTC(),
	}
	slowRecordKey := store.ingestBatchKey("tenant-a", slow.request.Source, slow.request.CollectorID, slow.request.BatchID)
	slowStatusKey := store.collectorStatusKey("tenant-a", slow.request.Source, slow.request.CollectorID)
	fastStatusKey := store.collectorStatusKey("tenant-a", fast.request.Source, fast.request.CollectorID)
	objects.slowKey = slowRecordKey

	done := make(chan error, 1)
	go func() {
		done <- store.saveIngestBatchResultMetadata(ctx, "tenant-a", []*ingestBatchCandidate{slow, fast})
	}()
	select {
	case <-objects.slowStarted:
	case <-ctx.Done():
		t.Fatalf("slow record did not start: %v", ctx.Err())
	}

	fastStatusObserved := false
	for !fastStatusObserved {
		select {
		case key := <-objects.statusWrites:
			switch key {
			case slowStatusKey:
				t.Fatalf("slow collector status was written before its record completed")
			case fastStatusKey:
				fastStatusObserved = true
			}
		case <-ctx.Done():
			t.Fatalf("fast collector status did not overlap slow record: %v", ctx.Err())
		}
	}
	select {
	case err := <-done:
		t.Fatalf("metadata DAG completed while slow record was blocked: %v", err)
	default:
	}
	close(objects.releaseSlow)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("metadata DAG: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("metadata DAG did not finish: %v", ctx.Err())
	}

	for key, want := range map[string]string{
		slowStatusKey: "batch-slow",
		fastStatusKey: "batch-fast",
	} {
		status, err := decodeParquetCollectorStatus(ctx, mustGetObject(t, ctx, store, key))
		if err != nil {
			t.Fatalf("decode collector status %s: %v", key, err)
		}
		if status.LastBatchID != want || status.LastVersion != 1 || status.AppliedTotal != 1 || status.FailedTotal != 0 {
			t.Fatalf("collector status %s = %#v, want completed version 1", key, status)
		}
	}
}

func TestIngestDurableBatchMetadataSkipsCollectorStatusAfterRecordFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	objects := &ingestMetadataDAGStore{
		ObjectStore:  NewMemoryStore(),
		statusWrites: make(chan string, 1),
	}
	store := NewTenantStore(objects, "test")
	store.MaterializeCollectorStatus = true
	candidate := &ingestBatchCandidate{
		request: IngestRequest{
			Source: "agent", CollectorID: "collector-failed", BatchID: "batch-failed", Cursor: "cursor-failed",
		},
		result:  IngestResult{BatchID: "batch-failed", Version: 1, Applied: 1},
		started: time.Unix(40, 0).UTC(),
	}
	recordKey := store.ingestBatchKey("tenant-a", candidate.request.Source, candidate.request.CollectorID, candidate.request.BatchID)
	statusKey := store.collectorStatusKey("tenant-a", candidate.request.Source, candidate.request.CollectorID)
	objects.failKey = recordKey
	if err := store.saveIngestBatchResultMetadata(ctx, "tenant-a", []*ingestBatchCandidate{candidate}); err == nil ||
		!strings.Contains(err.Error(), "save ingest batch") {
		t.Fatalf("failed record metadata err = %v, want record failure", err)
	}
	if _, err := objects.ObjectStore.Get(ctx, statusKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("collector status after failed record err = %v, want ErrNotFound", err)
	}
	select {
	case key := <-objects.statusWrites:
		t.Fatalf("collector status %s was written after failed record", key)
	default:
	}
}

type ingestMetadataDAGStore struct {
	ObjectStore
	slowKey      string
	failKey      string
	slowStarted  chan struct{}
	releaseSlow  chan struct{}
	statusWrites chan string
	slowOnce     sync.Once
}

func (s *ingestMetadataDAGStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	if key == s.slowKey {
		s.slowOnce.Do(func() { close(s.slowStarted) })
		select {
		case <-s.releaseSlow:
		case <-ctx.Done():
			return ObjectMeta{Key: key}, ctx.Err()
		}
	}
	if key == s.failKey {
		return ObjectMeta{Key: key}, errors.New("injected ingest record failure")
	}
	if strings.Contains(key, "/collectors/") && s.statusWrites != nil {
		s.statusWrites <- key
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

type ingestPublishSlotObjectStore struct {
	ObjectStore
	mu           sync.Mutex
	gets         map[string]int
	puts         []string
	canceledPuts int
}

func (s *ingestPublishSlotObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.recordGet(key)
	return s.ObjectStore.Get(ctx, key)
}

func (s *ingestPublishSlotObjectStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.recordGet(key)
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *ingestPublishSlotObjectStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	s.mu.Lock()
	s.puts = append(s.puts, key)
	if ctx != nil && ctx.Err() != nil {
		s.canceledPuts++
	}
	s.mu.Unlock()
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *ingestPublishSlotObjectStore) recordGet(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets[key]++
}

func (s *ingestPublishSlotObjectStore) reset() {
	s.mu.Lock()
	s.gets = map[string]int{}
	s.puts = nil
	s.canceledPuts = 0
	s.mu.Unlock()
}

func (s *ingestPublishSlotObjectStore) getCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

func (s *ingestPublishSlotObjectStore) putCountPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.puts {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func (s *ingestPublishSlotObjectStore) canceledPutCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canceledPuts
}

func TestIngestDurableBatchPublishedHookRunsAfterManifestPublish(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	var hookCalls int
	var hookManifest Manifest
	var hookErr error
	results, err := store.IngestDurableBatchWithHooks(ctx, "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
	}, IngestBatchHooks{
		Published: func() {
			hookCalls++
			hookManifest, hookErr = store.CurrentManifest(ctx, "tenant-a")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Version != 1 {
		t.Fatalf("results = %#v", results)
	}
	if hookCalls != 1 || hookErr != nil || hookManifest.Version != 1 {
		t.Fatalf("published hook calls=%d manifest=%#v err=%v, want one post-publish callback", hookCalls, hookManifest, hookErr)
	}
}

func TestIngestDurableBatchMergesExistingLooseTail(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	before, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(before.CommitKeys) != 1 {
		t.Fatalf("seed manifest = %#v, want one loose commit", before)
	}
	if _, err := store.RebuildIndexes(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 2 || results[1].Version != 3 {
		t.Fatalf("results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.CommitKeys) != 0 || len(manifest.CommitSegments) != 1 {
		t.Fatalf("manifest tail = %#v", manifest)
	}
	ref := manifest.CommitSegments[0]
	if ref.FirstVersion != 1 || ref.LastVersion != 3 || ref.Count != 3 {
		t.Fatalf("segment ref = %#v", ref)
	}
	reloaded := NewTenantStore(store.Objects, "test")
	loaded, loadedManifest, err := reloaded.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if loadedManifest.Version != 3 || len(loaded.Entities) != 3 {
		t.Fatalf("reloaded graph/manifest = %d entities, version %d", len(loaded.Entities), loadedManifest.Version)
	}
	catalog, err := reloaded.GetIndexCatalog(context.Background(), "tenant-a")
	if err != nil || catalog.Version != manifest.Version {
		t.Fatalf("batch index version = %d, want %d: %v", catalog.Version, manifest.Version, err)
	}
	lookup := &PersistedIndexLookup{Store: reloaded, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	for _, id := range []string{"host:seed", "host:1", "host:2"} {
		entity, ok, err := lookup.GetEntity(context.Background(), id, nil)
		if err != nil || !ok || entity.ID != id {
			t.Fatalf("batch indexed entity %s: entity=%v available=%v err=%v", id, entity, ok, err)
		}
	}
	reverse, _, err := reloaded.getReverseIndexCatalogWithMeta(context.Background(), "tenant-a")
	if err != nil || reverse.Version != manifest.Version {
		t.Fatalf("batch reverse index version = %d, want %d: %v", reverse.Version, manifest.Version, err)
	}
}

func TestLocalIngestPublishesWhileIndexesCatchUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "test")
	store.WriteEntityRecords = false
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	blocked := &blockOncePutStore{ObjectStore: files, substring: "/indexes/parquet/", paused: make(chan struct{}), resume: make(chan struct{})}
	resume := sync.OnceFunc(func() { close(blocked.resume) })
	t.Cleanup(resume)
	store.Objects = blocked
	if _, err := store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked.paused:
	case <-ctx.Done():
		t.Fatal("index update did not start")
	}
	for i, id := range []string{"host:3", "host:4", "host:3"} {
		request := ingestEntityRequest("next-"+id, id)
		request.BatchID += string(rune('a' + i))
		request.Items[0].Entity.Fields["revision"] = i
		if i == 0 {
			// Cross the pending-delta bound, then merge another update. The
			// resulting rebuild must include both the large batch and its tail.
			for n := 0; n < maxPendingIndexChanges; n++ {
				id := fmt.Sprintf("host:bulk-%d", n)
				request.Items = append(request.Items, IngestItem{ExternalID: id, Entity: &graph.Entity{ID: id, Kind: "host"}})
			}
		}
		var results []IngestResult
		var err error
		if i == 1 {
			var result IngestResult
			result, err = store.Ingest(ctx, "tenant-a", request)
			results = []IngestResult{result}
		} else {
			results, err = store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{{Request: request}})
		}
		if err != nil || results[0].Version != int64(4+i) {
			t.Fatalf("WAL publication while indexes are blocked: %v, %v", results, err)
		}
	}
	store.indexUpdateMu.Lock()
	done := store.indexUpdateTails["tenant-a"]
	store.indexUpdateMu.Unlock()
	resume()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("indexes did not catch up after publication")
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil || catalog.Version != 6 {
		t.Fatalf("caught-up catalog: %d, %v", catalog.Version, err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	for _, id := range []string{"host:1", "host:2", "host:3", "host:4", "host:bulk-8191"} {
		entity, ok, err := lookup.GetEntity(ctx, id, nil)
		if err != nil || !ok || entity.ID != id {
			t.Fatalf("coalesced indexed entity %s: available=%v err=%v", id, ok, err)
		}
		if id == "host:3" && entity.Fields["revision"] != float64(2) {
			t.Fatalf("coalesced update lost the last revision: %v", entity.Fields)
		}
	}
}

func TestLocalIngestBoundsBackgroundIndexWorkAcrossTenants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "test")
	store.WriteEntityRecords = false
	var objects ObjectStore = files
	blocked := make([]*blockOncePutStore, maxBackgroundIndexUpdates+1)
	resumes := make([]func(), len(blocked))
	for i := range blocked {
		tenant := fmt.Sprintf("tenant-%d", i)
		if _, err := store.Commit(ctx, tenant, indexMutations(), CommitOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RebuildIndexes(ctx, tenant); err != nil {
			t.Fatal(err)
		}
		blocked[i] = &blockOncePutStore{ObjectStore: objects, substring: "/tenants/" + tenant + "/indexes/parquet/", paused: make(chan struct{}), resume: make(chan struct{})}
		objects = blocked[i]
		resumes[i] = sync.OnceFunc(func() { close(blocked[i].resume) })
		t.Cleanup(resumes[i])
	}
	store.Objects = objects
	for i := 0; i < maxBackgroundIndexUpdates; i++ {
		if _, err := store.Ingest(ctx, fmt.Sprintf("tenant-%d", i), ingestEntityRequest("batch", "host:new")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-blocked[i].paused:
		case <-ctx.Done():
			t.Fatal("background index did not start")
		}
	}
	tenant := fmt.Sprintf("tenant-%d", maxBackgroundIndexUpdates)
	completed := make(chan error, 1)
	go func() {
		_, err := store.Ingest(ctx, tenant, ingestEntityRequest("batch", "host:new"))
		completed <- err
	}()
	select {
	case <-blocked[maxBackgroundIndexUpdates].paused:
	case <-ctx.Done():
		t.Fatal("foreground fallback did not start")
	}
	select {
	case err := <-completed:
		t.Fatalf("ingest escaped background limit before finishing indexes: %v", err)
	default:
	}
	// Capacity pressure must not hold the publication lock while indexing.
	unlock, err := store.lockTenantForeground(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
	resumes[maxBackgroundIndexUpdates]()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("foreground fallback did not finish")
	}
}

func TestIngestDurableBatchIsolatesBadRequestAndContinues(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	bad := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "bad",
		Items: []IngestItem{{
			Edge: &graph.Edge{Type: "runs_on", From: "service:missing", To: "host:missing"},
		}},
	}
	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: bad},
		{Request: ingestEntityRequest("good", "host:1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Applied != 0 || results[0].Failed != 1 || results[1].Version != 1 {
		t.Fatalf("results = %#v", results)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:1"); !ok {
		t.Fatal("valid request after bad request was not applied")
	}
}

func TestIngestDurableBatchExpectedVersionCohortPublishesAllInWALOrder(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.reset()
	expected := int64(0)
	var stats IngestBatchStats
	results, err := store.IngestDurableBatchWithHooks(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: IngestRequest{
			Source:          "agent",
			CollectorID:     "collector-a",
			BatchID:         "cas-batch-1",
			ExpectedVersion: &expected,
			Items: []IngestItem{
				{ExternalID: "shared", Entity: &graph.Entity{ID: "host:shared", Kind: "host", Fields: graph.Fields{"name": "first", "sequence": 1}}},
				{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"name": "one"}}},
			},
		}},
		{Request: IngestRequest{
			Source:          "agent",
			CollectorID:     "collector-a",
			BatchID:         "cas-batch-2",
			ExpectedVersion: &expected,
			Items: []IngestItem{
				{ExternalID: "shared", Entity: &graph.Entity{ID: "host:shared", Kind: "host", Fields: graph.Fields{"name": "second", "sequence": 2}}},
				{ExternalID: "host-2", Entity: &graph.Entity{ID: "host:2", Kind: "host", Fields: graph.Fields{"name": "two"}}},
			},
		}},
	}, IngestBatchHooks{
		Stats: func(got IngestBatchStats) { stats = got },
	})
	if err != nil {
		t.Fatalf("batch ingest: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %#v, want two results", results)
	}
	if results[0].Applied != 2 || results[0].Failed != 0 || results[0].Version != 1 || results[0].ErrorCode != "" {
		t.Fatalf("first result = %#v, want version 1 success", results[0])
	}
	if results[1].Applied != 2 || results[1].Failed != 0 || results[1].Version != 2 || results[1].ErrorCode != "" {
		t.Fatalf("second result = %#v, want version 2 success", results[1])
	}
	if stats.LogicalCommits != 2 || stats.CASMerged != 2 || stats.Fallback || stats.Segments != 1 || stats.ManifestPublishes != 1 {
		t.Fatalf("batch stats = %#v, want two merged commits, one segment and one manifest", stats)
	}
	if puts := objects.count(store.manifestKey("tenant-a")); puts != 1 {
		t.Fatalf("manifest PUTs = %d, want 1", puts)
	}
	if puts := objects.countPrefix(store.commitSegmentPrefix("tenant-a")); puts != 1 {
		t.Fatalf("segment PUTs = %d, want 1", puts)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load after batch: %v", err)
	}
	if manifest.Version != 2 || loaded.Version != 2 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 2 {
		t.Fatalf("graph/manifest = %d/%#v, want two-version segment", loaded.Version, manifest)
	}
	for _, id := range []string{"host:shared", "host:1", "host:2"} {
		if _, ok := loaded.GetEntity(id); !ok {
			t.Fatalf("entity %q was not published", id)
		}
	}
	shared, ok := loaded.GetEntity("host:shared")
	if !ok || shared.Fields["name"] != "second" || shared.Fields["sequence"] != float64(2) {
		t.Fatalf("shared entity = %#v, want second WAL value", shared)
	}
}

func TestIngestDurableBatchCASCohortPreconditionFailureUsesCommonBase(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.reset()
	expected := int64(0)
	var stats IngestBatchStats
	results, err := store.IngestDurableBatchWithHooks(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-precondition-pass-1", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "shared", Entity: &graph.Entity{ID: "host:shared", Kind: "host", Fields: graph.Fields{"state": "ready"}}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-precondition-fail", ExpectedVersion: &expected,
			Preconditions: []IngestPrecondition{{ResourceType: "entity", ID: "host:shared", Field: "state", Op: "eq", Value: "blocked"}},
			Items:         []IngestItem{{ExternalID: "failed", Entity: &graph.Entity{ID: "host:failed", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-precondition-pass-2", ExpectedVersion: &expected,
			Preconditions: []IngestPrecondition{{ResourceType: "entity", ID: "host:shared", Field: "state", Op: "not_exists"}},
			Items:         []IngestItem{{ExternalID: "third", Entity: &graph.Entity{ID: "host:third", Kind: "host"}}},
		}},
	}, IngestBatchHooks{
		Stats: func(got IngestBatchStats) { stats = got },
	})
	if err != nil {
		t.Fatalf("batch ingest: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results = %#v, want three results", results)
	}
	if results[0].Applied != 1 || results[0].Failed != 0 || results[0].Version != 1 {
		t.Fatalf("first result = %#v, want version 1 success", results[0])
	}
	if results[1].Applied != 0 || results[1].Failed != 1 || results[1].Version != 0 || results[1].ErrorCode != IngestErrorPreconditionFailed {
		t.Fatalf("failed precondition result = %#v", results[1])
	}
	if len(results[1].Failures) != 1 || len(results[1].Conflicts) != 1 || !strings.Contains(results[1].Failures[0].Error, "condition 0") {
		t.Fatalf("failed precondition details = %#v", results[1])
	}
	if results[2].Applied != 1 || results[2].Failed != 0 || results[2].Version != 2 {
		t.Fatalf("third result = %#v, want version 2 success from common base", results[2])
	}
	if stats.LogicalCommits != 2 || stats.CASMerged != 2 || stats.Fallback || stats.Segments != 1 || stats.ManifestPublishes != 1 {
		t.Fatalf("batch stats = %#v, want two accepted CAS commits", stats)
	}
	if puts := objects.count(store.manifestKey("tenant-a")); puts != 1 {
		t.Fatalf("manifest PUTs = %d, want 1", puts)
	}
	if puts := objects.countPrefix(store.commitSegmentPrefix("tenant-a")); puts != 1 {
		t.Fatalf("segment PUTs = %d, want 1", puts)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load after batch: %v", err)
	}
	if manifest.Version != 2 || loaded.Version != 2 {
		t.Fatalf("graph/manifest version = %d/%d, want 2", loaded.Version, manifest.Version)
	}
	for _, id := range []string{"host:shared", "host:third"} {
		if _, ok := loaded.GetEntity(id); !ok {
			t.Fatalf("entity %q was not published", id)
		}
	}
	if _, ok := loaded.GetEntity("host:failed"); ok {
		t.Fatal("failed precondition request was published")
	}
}

func TestIngestDurableBatchExpiredCASCohortFailsAllWithoutPublication(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host", Fields: graph.Fields{"state": "seed"}}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	objects.reset()
	expected := int64(0)
	var stats IngestBatchStats
	results, err := store.IngestDurableBatchWithHooks(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-expired-1", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "expired-1", Entity: &graph.Entity{ID: "host:expired-1", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-expired-2", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "expired-2", Entity: &graph.Entity{ID: "host:expired-2", Kind: "host"}}},
		}},
	}, IngestBatchHooks{
		Stats: func(got IngestBatchStats) { stats = got },
	})
	if err != nil {
		t.Fatalf("expired cohort ingest: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %#v, want two results", results)
	}
	for index, result := range results {
		if result.Applied != 0 || result.Failed != 1 || result.Version != 0 || result.ErrorCode != IngestErrorVersionConflict {
			t.Fatalf("expired result %d = %#v, want version conflict", index, result)
		}
		if len(result.Failures) != 1 || !strings.Contains(result.Failures[0].Error, "expected version 0, current version 1") {
			t.Fatalf("expired result %d failures = %#v", index, result.Failures)
		}
		if len(result.Conflicts) != 1 || !strings.Contains(result.Conflicts[0].Message, "expected version 0, current version 1") {
			t.Fatalf("expired result %d conflicts = %#v", index, result.Conflicts)
		}
	}
	if stats.LogicalCommits != 0 || stats.CASMerged != 0 || stats.Fallback || stats.Segments != 0 || stats.ManifestPublishes != 0 {
		t.Fatalf("expired cohort stats = %#v, want zero publication", stats)
	}
	if puts := objects.count(store.manifestKey("tenant-a")); puts != 0 {
		t.Fatalf("manifest PUTs after expired cohort = %d, want 0", puts)
	}
	if puts := objects.countPrefix(store.commitSegmentPrefix("tenant-a")); puts != 0 {
		t.Fatalf("segment PUTs after expired cohort = %d, want 0", puts)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load after expired cohort: %v", err)
	}
	if manifest.Version != 1 || loaded.Version != 1 {
		t.Fatalf("graph/manifest version = %d/%d, want seed version 1", loaded.Version, manifest.Version)
	}
	for _, id := range []string{"host:expired-1", "host:expired-2"} {
		if _, ok := loaded.GetEntity(id); ok {
			t.Fatalf("expired entity %q was published", id)
		}
	}
}

func TestIngestDurableBatchCASCohortFallbackKeepsPrevalidatedExpectedVersion(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:shared", Kind: "host", Source: "agent", ExternalID: "shared", Fields: graph.Fields{"name": "stable"}}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	objects.reset()
	expected := int64(1)
	var stats IngestBatchStats
	results, err := store.IngestDurableBatchWithHooks(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-fallback-noop", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "shared", Entity: &graph.Entity{ID: "host:shared", Kind: "host", Fields: graph.Fields{"name": "stable"}}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-fallback-next", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "next", Entity: &graph.Entity{ID: "host:next", Kind: "host", Fields: graph.Fields{"name": "next"}}}},
		}},
	}, IngestBatchHooks{
		Stats: func(got IngestBatchStats) { stats = got },
	})
	if err != nil {
		t.Fatalf("fallback cohort ingest: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %#v, want two results", results)
	}
	if results[0].Applied != 1 || results[0].Failed != 0 || results[0].Version != 1 || !results[0].Skipped || results[0].SkipReason != IngestSkipReasonLogicalNoop {
		t.Fatalf("logical-noop result = %#v", results[0])
	}
	if results[1].Applied != 1 || results[1].Failed != 0 || results[1].Version != 2 || results[1].ErrorCode != "" {
		t.Fatalf("post-fallback result = %#v, want version 2 success", results[1])
	}
	if stats.LogicalCommits != 1 || stats.CASMerged != 1 || !stats.Fallback || stats.Segments != 1 || stats.ManifestPublishes != 1 {
		t.Fatalf("fallback cohort stats = %#v, want isolated fallback with one commit", stats)
	}
	if puts := objects.count(store.manifestKey("tenant-a")); puts != 1 {
		t.Fatalf("manifest PUTs after fallback = %d, want 1", puts)
	}
	if puts := objects.countPrefix(store.commitSegmentPrefix("tenant-a")); puts != 1 {
		t.Fatalf("segment PUTs after fallback = %d, want 1", puts)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load after fallback cohort: %v", err)
	}
	if manifest.Version != 2 || loaded.Version != 2 {
		t.Fatalf("graph/manifest version = %d/%d, want 2", loaded.Version, manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:next"); !ok {
		t.Fatal("post-fallback request was not published")
	}
}

func TestIngestDurableBatchCASCohortPreservesWALGroupsAroundAtomicBarrier(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.reset()
	expected := int64(0)
	var stats IngestBatchStats
	results, err := store.IngestDurableBatchWithHooks(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-group-1", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "group-1", Entity: &graph.Entity{ID: "host:group-1", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "cas-group-2", ExpectedVersion: &expected,
			Items: []IngestItem{{ExternalID: "group-2", Entity: &graph.Entity{ID: "host:group-2", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "atomic-barrier", FailureMode: IngestFailureModeAtomic,
			Items: []IngestItem{{ExternalID: "atomic", Entity: &graph.Entity{ID: "host:atomic", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "ordinary-after-barrier",
			Items: []IngestItem{{ExternalID: "ordinary", Entity: &graph.Entity{ID: "host:ordinary", Kind: "host"}}},
		}},
	}, IngestBatchHooks{
		Stats: func(got IngestBatchStats) { stats = got },
	})
	if err != nil {
		t.Fatalf("mixed batch ingest: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("results = %#v, want four results", results)
	}
	for index, result := range results {
		if result.Applied != 1 || result.Failed != 0 || result.Version != int64(index+1) || result.ErrorCode != "" {
			t.Fatalf("result %d = %#v, want successful version %d", index, result, index+1)
		}
	}
	if stats.LogicalCommits != 4 || stats.CASMerged != 2 || stats.Segments != 1 || stats.ManifestPublishes != 1 {
		t.Fatalf("mixed batch stats = %#v, want cohort plus isolated groups in one publish", stats)
	}
	if puts := objects.count(store.manifestKey("tenant-a")); puts != 1 {
		t.Fatalf("manifest PUTs = %d, want 1", puts)
	}
	if puts := objects.countPrefix(store.commitSegmentPrefix("tenant-a")); puts != 1 {
		t.Fatalf("segment PUTs = %d, want 1", puts)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load after mixed batch: %v", err)
	}
	if manifest.Version != 4 || loaded.Version != 4 {
		t.Fatalf("graph/manifest version = %d/%d, want 4", loaded.Version, manifest.Version)
	}
	for _, id := range []string{"host:group-1", "host:group-2", "host:atomic", "host:ordinary"} {
		if _, ok := loaded.GetEntity(id); !ok {
			t.Fatalf("entity %q was not published", id)
		}
	}
}

func BenchmarkIngestCASCohortApply(b *testing.B) {
	for _, test := range []struct {
		name     string
		requests int
	}{
		{name: "N=2", requests: 2},
		{name: "N=8", requests: 8},
	} {
		for _, mode := range []string{"cohort", "sequential"} {
			b.Run(test.name+"/"+mode, func(b *testing.B) {
				ctx := context.Background()
				store := NewTenantStore(NewMemoryStore(), "bench-cas")
				if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
					b.Fatal(err)
				}

				expected := int64(0)
				warmup := make([]IngestBatchEntry, test.requests)
				for requestIndex := range test.requests {
					request := benchmarkIngestRequest(-1, requestIndex)
					request.ExpectedVersion = &expected
					warmup[requestIndex].Request = request
				}
				if _, err := store.IngestDurableBatch(ctx, "tenant-a", warmup); err != nil {
					b.Fatal(err)
				}
				expected = int64(test.requests)

				b.ReportAllocs()
				b.ReportMetric(float64(test.requests), "requests/op")
				b.ResetTimer()
				for iteration := range b.N {
					if mode == "cohort" {
						entries := make([]IngestBatchEntry, test.requests)
						for requestIndex := range test.requests {
							request := benchmarkIngestRequest(iteration, requestIndex)
							request.ExpectedVersion = &expected
							entries[requestIndex].Request = request
						}
						if _, err := store.IngestDurableBatch(ctx, "tenant-a", entries); err != nil {
							b.Fatal(err)
						}
						expected += int64(test.requests)
						continue
					}

					for requestIndex := range test.requests {
						request := benchmarkIngestRequest(iteration, requestIndex)
						request.ExpectedVersion = &expected
						if _, err := store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{{Request: request}}); err != nil {
							b.Fatal(err)
						}
						expected++
					}
				}
			})
		}
	}
}

func TestIngestDurableBatchAtomicInvalidRequestDoesNotPublish(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{{
		Request: IngestRequest{
			Source:      "agent",
			CollectorID: "collector-a",
			BatchID:     "atomic-batch-invalid",
			FailureMode: IngestFailureModeAtomic,
			Items: []IngestItem{
				{ExternalID: "valid", Entity: &graph.Entity{ID: "host:valid", Kind: "host"}},
				{ExternalID: "invalid"},
			},
		},
	}})
	if err != nil {
		t.Fatalf("atomic batch ingest: %v", err)
	}
	if len(results) != 1 || results[0].Applied != 0 || results[0].Failed != 2 || results[0].Version != 0 || results[0].ErrorCode != IngestErrorAtomicValidation {
		t.Fatalf("result = %#v, want whole request rejected", results)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load after atomic batch rejection: %v", err)
	}
	if manifest.Version != 0 || loaded.Version != 0 {
		t.Fatalf("graph/manifest version = %d/%d, want 0", loaded.Version, manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:valid"); ok {
		t.Fatal("valid item from rejected atomic batch was published")
	}
}

func TestIngestDurableBatchAtomicSuppressionDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}); err != nil {
		t.Fatalf("put source policy: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"owner": "platform"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("seed manual entity: %v", err)
	}

	results, err := store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{{
		Request: IngestRequest{
			Source:      "agent",
			CollectorID: "collector-a",
			BatchID:     "atomic-batch-suppressed",
			FailureMode: IngestFailureModeAtomic,
			Items: []IngestItem{
				{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "collector"}}},
				{ExternalID: "host-2", Entity: &graph.Entity{ID: "host:2", Kind: "host", Fields: graph.Fields{"owner": "collector"}}},
			},
		},
	}})
	if err != nil {
		t.Fatalf("atomic suppressed batch ingest: %v", err)
	}
	if len(results) != 1 || results[0].Applied != 0 || results[0].Failed != 2 || results[0].Version != 0 || results[0].ErrorCode != IngestErrorAtomicSuppressed {
		t.Fatalf("result = %#v, want whole request rejected on suppression", results)
	}
	loaded, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load after atomic suppression: %v", err)
	}
	if manifest.Version != 1 || loaded.Version != 1 {
		t.Fatalf("graph/manifest version = %d/%d, want unchanged seed version 1", loaded.Version, manifest.Version)
	}
	entity, ok := loaded.GetEntity("host:1")
	if !ok || entity.Fields["owner"] != "platform" {
		t.Fatalf("seed entity after rejection = %#v, want owner platform", entity)
	}
	if _, ok := loaded.GetEntity("host:2"); ok {
		t.Fatal("independent valid item from rejected atomic request was published")
	}
}

func TestIngestDurableBatchDuplicateContentDoesNotConsumeVersion(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	first := ingestEntityRequest("batch-1", "host:1")
	second := ingestEntityRequest("batch-2", "host:1")
	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: first, AcceptedAt: time.Unix(1, 0).UTC()},
		{Request: second, AcceptedAt: time.Unix(2, 0).UTC()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 1 || results[1].Version != 1 || !results[1].Skipped || results[1].SkipReason != IngestSkipReasonLogicalNoop {
		t.Fatalf("results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 1 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestIngestDurableBatchRejectsProjectedCommitTailOverflow(t *testing.T) {
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 1})
	_, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	})
	assertBackpressureReason(t, err, "commit_tail_too_long")
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 0 || len(manifest.CommitSegments) != 0 {
		t.Fatalf("manifest after rejected flush = %#v", manifest)
	}
}

func TestIngestDurableBatchMetadataRetryDoesNotRepublishManifest(t *testing.T) {
	objects := &failIngestRecordOnceStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	first := ingestEntityRequest("batch-1", "host:1")
	second := ingestEntityRequest("batch-2", "host:2")
	objects.failKey = store.ingestBatchKey("tenant-a", first.Source, first.CollectorID, first.BatchID)
	entries := []IngestBatchEntry{{Request: first}, {Request: second}}
	var plans []*IngestPreparedRequest
	results, err := store.IngestDurableBatchWithHooks(
		context.Background(),
		"tenant-a",
		entries,
		IngestBatchHooks{
			Prepared: func(_ context.Context, prepared []*IngestPreparedRequest) error {
				plans = prepared
				return nil
			},
		},
	)
	if err == nil || results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("first flush results/err = %#v / %v", results, err)
	}
	if len(plans) != len(entries) || plans[0] == nil || plans[1] == nil {
		t.Fatalf("prepared plans = %#v", plans)
	}
	for index := range entries {
		entries[index].Prepared = plans[index]
	}
	results, err = store.IngestDurableBatch(context.Background(), "tenant-a", entries)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("retry results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.CommitSegments) != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestIngestDurableBatchReplaysPreparedPlanAfterManifestFailure(t *testing.T) {
	base := NewMemoryStore()
	objects := &failIngestRecordOnceStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.failKey = store.manifestKey("tenant-a")
	entries := []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	}
	var plans []*IngestPreparedRequest
	results, err := store.IngestDurableBatchWithHooks(
		context.Background(),
		"tenant-a",
		entries,
		IngestBatchHooks{
			Prepared: func(_ context.Context, prepared []*IngestPreparedRequest) error {
				plans = prepared
				return nil
			},
		},
	)
	if err == nil {
		t.Fatalf("first flush unexpectedly succeeded: %#v", results)
	}
	for index := range entries {
		if plans[index] == nil {
			t.Fatalf("prepared plan %d is nil", index)
		}
		entries[index].Prepared = plans[index]
	}
	if plans[0].Result.Version != 1 || plans[1].Result.Version != 2 {
		t.Fatalf("prepared results = %#v", plans)
	}
	results, err = store.IngestDurableBatch(context.Background(), "tenant-a", entries)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("retry results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	segments, err := base.List(context.Background(), store.commitSegmentPrefix("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Fatalf("commit segment objects = %#v", segments)
	}
}

func ingestEntityRequest(batchID string, entityID string) IngestRequest {
	return IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     batchID,
		Items: []IngestItem{{
			ExternalID: entityID,
			Entity: &graph.Entity{
				ID: entityID, Kind: "host", Fields: graph.Fields{"name": entityID},
			},
		}},
	}
}

type ingestBatchCountingStore struct {
	ObjectStore
	mu   sync.Mutex
	puts []string
}

func newIngestBatchCountingStore(inner ObjectStore) *ingestBatchCountingStore {
	return &ingestBatchCountingStore{ObjectStore: inner}
}

func (s *ingestBatchCountingStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	s.mu.Lock()
	s.puts = append(s.puts, key)
	s.mu.Unlock()
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *ingestBatchCountingStore) reset() {
	s.mu.Lock()
	s.puts = nil
	s.mu.Unlock()
}

func (s *ingestBatchCountingStore) count(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, current := range s.puts {
		if current == key {
			count++
		}
	}
	return count
}

func (s *ingestBatchCountingStore) countPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.puts {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func (s *ingestBatchCountingStore) countLooseCommits(commitPrefix string, segmentPrefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.puts {
		if strings.HasPrefix(key, commitPrefix) && !strings.HasPrefix(key, segmentPrefix) {
			count++
		}
	}
	return count
}
