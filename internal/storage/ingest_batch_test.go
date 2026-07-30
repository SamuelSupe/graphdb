package storage

import (
	"context"
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
