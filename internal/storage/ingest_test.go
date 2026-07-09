package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestBatchPartialFailureIdempotencyAndCollectorStatus(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Cursor:         "cursor-42",
		Items: []IngestItem{
			{ExternalID: "i-1", Entity: &graph.Entity{ID: "host:i-1", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
			{ExternalID: "bad-empty"},
		},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Applied != 1 || result.Failed != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want applied=1 failed=1 version=1", result)
	}
	if len(result.Failures) != 1 || result.Failures[0].ExternalID != "bad-empty" {
		t.Fatalf("failures = %#v", result.Failures)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entity, ok := g.GetEntity("host:i-1")
	if !ok {
		t.Fatal("valid entity was not committed")
	}
	if entity.Source != "aws" || entity.ExternalID != "i-1" {
		t.Fatalf("source metadata = %#v", entity)
	}

	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Version != result.Version {
		t.Fatalf("replayed = %#v, want skipped previous result", replayed)
	}
	status, err := store.GetCollectorStatus(ctx, "tenant-a", "aws", "collector-a")
	if err != nil {
		t.Fatalf("collector status: %v", err)
	}
	if status.LastCursor != "cursor-42" || status.AppliedTotal != 1 || status.FailedTotal != 1 {
		t.Fatalf("collector status = %#v", status)
	}
	assertParquetIngestRecord(t, ctx, store, store.ingestBatchKey("tenant-a", "aws", "collector-a", "batch-1"), request)
	assertParquetIngestRecord(t, ctx, store, store.ingestIdempotencyKey("tenant-a", "aws", "collector-a", "idem-1"), request)
	assertParquetCollectorStatus(t, ctx, store, store.collectorStatusKey("tenant-a", "aws", "collector-a"), "tenant-a", "aws", "collector-a")
	assertParquetDeadLetter(t, ctx, store, store.deadLetterKey("tenant-a", "aws", "collector-a/batch-1"), "tenant-a", "aws", "collector-a/batch-1")
}

func TestIngestCollectorStatusCacheAvoidsHotStatusRead(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	first := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"hostname": "host-1"}},
		}},
	}
	if _, err := store.Ingest(ctx, "tenant-a", first); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	objects.Reset()
	second := first
	second.BatchID = "batch-2"
	second.Items[0].ExternalID = "host-2"
	second.Items[0].Entity = &graph.Entity{ID: "host:2", Kind: "host", Fields: graph.Fields{"hostname": "host-2"}}
	if _, err := store.Ingest(ctx, "tenant-a", second); err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if got := objects.CountContains("/ingest/agent/collectors/collector-a.parquet"); got != 0 {
		t.Fatalf("collector status GET count = %d, want 0", got)
	}
}

func TestIngestCollectorStatusCanBeDerivedWhenMaterializationDisabled(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.MaterializeCollectorStatus = false
	first := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Cursor:      "cursor-1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"hostname": "host-1"}},
		}},
	}
	second := first
	second.BatchID = "batch-2"
	second.Cursor = "cursor-2"
	second.Items = []IngestItem{{
		ExternalID: "host-2",
		Entity:     &graph.Entity{ID: "host:2", Kind: "host", Fields: graph.Fields{"hostname": "host-2"}},
	}}
	if _, err := store.Ingest(ctx, "tenant-a", first); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if _, err := store.Ingest(ctx, "tenant-a", second); err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	statusKey := store.collectorStatusKey("tenant-a", "agent", "collector-a")
	if _, err := objects.Get(ctx, statusKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("collector status object err = %v, want not found", err)
	}
	status, err := store.GetCollectorStatus(ctx, "tenant-a", "agent", "collector-a")
	if err != nil {
		t.Fatalf("cached collector status: %v", err)
	}
	if status.LastBatchID != "batch-2" || status.LastCursor != "cursor-2" || status.AppliedTotal != 2 || status.FailedTotal != 0 {
		t.Fatalf("cached collector status = %#v", status)
	}

	restarted := NewTenantStore(objects, "test")
	restarted.MaterializeCollectorStatus = false
	status, err = restarted.GetCollectorStatus(ctx, "tenant-a", "agent", "collector-a")
	if err != nil {
		t.Fatalf("derived collector status: %v", err)
	}
	if status.LastBatchID != "batch-2" || status.LastCursor != "cursor-2" || status.AppliedTotal != 2 || status.FailedTotal != 0 {
		t.Fatalf("derived collector status = %#v", status)
	}
}

func TestIngestRecordKeyCacheAvoidsHotMissReads(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	first := IngestRequest{
		Source:         "loadtest",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}
	if _, err := store.Ingest(ctx, "tenant-a", first); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	objects.Reset()
	second := first
	second.BatchID = "batch-2"
	second.IdempotencyKey = "idem-2"
	second.Items = []IngestItem{{
		ExternalID: "host-2",
		Entity:     &graph.Entity{ID: "host:2", Kind: "host"},
	}}
	if _, err := store.Ingest(ctx, "tenant-a", second); err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	for _, fragment := range []string{"/ingest/loadtest/idempotency/", "/ingest/loadtest/batches/"} {
		if got := objects.CountContains(fragment); got != 0 {
			t.Fatalf("ingest record GET count for %s = %d, want 0", fragment, got)
		}
	}
}

func TestIngestCoalescesSameBatchAndIdempotencyRecord(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	request := IngestRequest{
		Source:         "loadtest",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "batch-1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host"},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Version != 1 {
		t.Fatalf("version = %d, want 1", result.Version)
	}
	if _, err := objects.Get(ctx, store.ingestIdempotencyKey("tenant-a", "loadtest", "collector-a", "batch-1")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("idempotency object err = %v, want not found", err)
	}
	assertParquetIngestRecord(t, ctx, store, store.ingestBatchKey("tenant-a", "loadtest", "collector-a", "batch-1"), request)

	replay := request
	replay.BatchID = "batch-retry"
	replayed, err := store.Ingest(ctx, "tenant-a", replay)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Version != result.Version || replayed.BatchID != "batch-1" {
		t.Fatalf("replayed = %#v, want original coalesced record", replayed)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not create another commit", manifest.Version)
	}
}

func TestIngestNormalizesControlScopeFields(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:         " aws ",
		CollectorID:    " collector-a ",
		BatchID:        " batch-1 ",
		IdempotencyKey: " idem-1 ",
		Items: []IngestItem{
			{ExternalID: "i-1", Entity: &graph.Entity{ID: "host:i-1", Kind: "host"}},
			{ExternalID: "bad-empty"},
		},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.BatchID != "batch-1" || result.Applied != 1 || result.Failed != 1 {
		t.Fatalf("result = %#v, want normalized batch with partial failure", result)
	}
	status, err := store.GetCollectorStatus(ctx, "tenant-a", "aws", "collector-a")
	if err != nil {
		t.Fatalf("collector status: %v", err)
	}
	if status.Source != "aws" || status.CollectorID != "collector-a" || status.LastBatchID != "batch-1" {
		t.Fatalf("collector status = %#v, want normalized scope fields", status)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "aws")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].Status != "pending" || letters[0].Request.Source != "aws" || letters[0].Request.CollectorID != "collector-a" {
		t.Fatalf("deadletters = %#v, want normalized pending record", letters)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Version != result.Version {
		t.Fatalf("replayed = %#v, want skipped normalized idempotency result", replayed)
	}
}

func TestIngestRejectsAmbiguousItemPayload(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "ambiguous-item",
		Items: []IngestItem{{
			ExternalID: "ambiguous-1",
			Entity:     &graph.Entity{ID: "host:ambiguous", Kind: "host"},
			Edge:       &graph.Edge{ID: "edge:ambiguous", Type: "runs_on", From: "service:api", To: "host:ambiguous"},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Applied != 0 || result.Failed != 1 || result.Version != 0 {
		t.Fatalf("result = %#v, want failed ambiguous item without commit", result)
	}
	if len(result.Failures) != 1 || !strings.Contains(result.Failures[0].Error, "more than one") {
		t.Fatalf("failures = %#v", result.Failures)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 0 {
		t.Fatalf("manifest version = %d, want no commit", manifest.Version)
	}
	if _, ok := g.GetEntity("host:ambiguous"); ok {
		t.Fatal("ambiguous entity was committed")
	}
	if len(g.Edges) != 0 {
		t.Fatal("ambiguous edge was committed")
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "aws")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].Request.Items[0].ExternalID != "ambiguous-1" {
		t.Fatalf("deadletters = %#v, want ambiguous item", letters)
	}
}

func TestConcurrentIngestIsSerializedPerTenant(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	const batches = 20
	var wg sync.WaitGroup
	errs := make(chan error, batches)
	for batch := 0; batch < batches; batch++ {
		wg.Add(1)
		go func(batch int) {
			defer wg.Done()
			result, err := store.Ingest(ctx, "tenant-a", concurrentIngestRequest(batch))
			if err != nil {
				errs <- err
				return
			}
			if result.Failed != 0 || result.Applied != 1 {
				errs <- fmt.Errorf("batch %d result = %#v", batch, result)
			}
		}(batch)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != batches {
		t.Fatalf("version = %d, want %d", manifest.Version, batches)
	}
	if len(g.Entities) != batches {
		t.Fatalf("entities = %d, want %d", len(g.Entities), batches)
	}
}

func TestIngestReturnsMetadataPersistenceErrors(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(&failPutStore{ObjectStore: objects, contains: "/ingest/aws/idempotency/"}, "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save ingest batch") {
		t.Fatalf("err = %v, want save ingest batch error", err)
	}
	if result.Applied != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want committed result", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:i-1"); !ok {
		t.Fatal("entity should remain committed when ingest metadata persistence fails")
	}
}

func TestIngestReplayUsesBatchRecordWhenIdempotencyRecordIsMissing(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(&failPutStore{ObjectStore: objects, contains: "/ingest/aws/idempotency/"}, "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save ingest batch") {
		t.Fatalf("err = %v, want idempotency metadata error", err)
	}
	if result.Version != 1 {
		t.Fatalf("result version = %d, want 1", result.Version)
	}

	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Version != 1 {
		t.Fatalf("replayed = %#v, want skipped version 1", replayed)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not create another commit", manifest.Version)
	}
}

func TestIngestReplayWithGeneratedBatchIDUsesStableIdempotencyBatch(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(&failPutStore{ObjectStore: objects, contains: "/ingest/aws/idempotency/"}, "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		IdempotencyKey: "idem-generated-batch",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save ingest batch") {
		t.Fatalf("err = %v, want idempotency metadata error", err)
	}
	if result.BatchID == "" || result.Version != 1 {
		t.Fatalf("result = %#v, want generated batch version 1", result)
	}

	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.BatchID != result.BatchID || replayed.Version != 1 {
		t.Fatalf("replayed = %#v, want skipped original batch %#v", replayed, result)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not create another commit", manifest.Version)
	}
}

func TestIngestDeleteEntityUsesSourcePolicySuppression(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}); err != nil {
		t.Fatalf("source policy: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", ExternalID: "asset-1",
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "delete-1",
		Items: []IngestItem{{
			ExternalID:   "host:1",
			DeleteEntity: &graph.EntityDeleteRequest{ID: "host:1"},
		}},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v, want suppressed conflict without failure", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:1"); !ok {
		t.Fatal("suppressed delete removed manual entity")
	}
}

func TestIngestFullSyncMarksMissingEntitiesStale(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "seed",
		Items: []IngestItem{
			{ExternalID: "i-1", Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
			{ExternalID: "i-2", Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "app-02"}}},
		},
	}); err != nil {
		t.Fatalf("seed ingest: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "full-sync",
		FullSync:    true,
		Items: []IngestItem{
			{ExternalID: "i-1", Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
		},
	})
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}
	if result.Failed != 0 || result.Version == 0 {
		t.Fatalf("result = %#v", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entity, ok := g.GetEntity(graph.CanonicalEntityIDParts("host", "aws", "i-2"))
	if !ok || len(entity.Sources) != 1 || !entity.Sources[0].Stale {
		t.Fatalf("stale entity = %#v ok=%v", entity, ok)
	}
}

func TestIngestReplayUsesIdempotencyRecordWhenBatchRecordIsMissing(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(&failPutStore{ObjectStore: objects, contains: "/ingest/aws/batches/"}, "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save ingest batch") {
		t.Fatalf("err = %v, want batch metadata error", err)
	}
	if result.Version != 1 {
		t.Fatalf("result version = %d, want 1", result.Version)
	}

	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Version != 1 {
		t.Fatalf("replayed = %#v, want skipped version 1", replayed)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not create another commit", manifest.Version)
	}
}

func TestIngestReplayUsesBatchIDWithoutIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-only-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Skipped || result.Version != 1 {
		t.Fatalf("result = %#v, want first applied version 1", result)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Version != 1 || replayed.BatchID != request.BatchID {
		t.Fatalf("replayed = %#v, want skipped original batch", replayed)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not create another commit", manifest.Version)
	}
}

func TestIngestRejectsBatchIDReuseWithDifferentPayload(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	first := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-reused",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host"},
		}},
	}
	if _, err := store.Ingest(ctx, "tenant-a", first); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	second := first
	second.Items = []IngestItem{{
		ExternalID: "i-2",
		Entity:     &graph.Entity{ID: "host:b", Kind: "host"},
	}}
	if _, err := store.Ingest(ctx, "tenant-a", second); err == nil || !strings.Contains(err.Error(), "ingest record conflict") {
		t.Fatalf("second ingest err = %v, want record conflict", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, conflicting replay should not commit", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("conflicting replay committed host:b")
	}
}

func TestIngestRejectsIdempotencyKeyReuseWithDifferentPayload(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	first := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		IdempotencyKey: "idem-reused",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host", Fields: graph.Fields{"cpu": 1}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", first)
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	second := first
	second.Items = []IngestItem{{
		ExternalID: "i-2",
		Entity:     &graph.Entity{ID: "host:b", Kind: "host", Fields: graph.Fields{"cpu": 2}},
	}}
	if _, err := store.Ingest(ctx, "tenant-a", second); err == nil || !strings.Contains(err.Error(), "ingest record conflict") {
		t.Fatalf("second ingest err = %v, want record conflict", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != result.Version {
		t.Fatalf("manifest version = %d, want original version %d", manifest.Version, result.Version)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("conflicting idempotency replay committed host:b")
	}
}

func TestIngestDoesNotOverwriteRecordCreatedDuringPublish(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	conflictingRequest := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-race",
		Items: []IngestItem{{
			ExternalID: "i-other",
			Entity:     &graph.Entity{ID: "host:other", Kind: "host"},
		}},
	}
	store := NewTenantStore(&raceIngestRecordStore{
		ObjectStore: objects,
		contains:    "/ingest/aws/batches/collector-a/batch-race.parquet",
		record: IngestBatchRecord{
			TenantID: "tenant-a",
			Request:  conflictingRequest,
			Result:   IngestResult{BatchID: "batch-race", Version: 99, Applied: 1},
		},
	}, "test")
	request := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-race",
		Items: []IngestItem{{
			ExternalID: "i-new",
			Entity:     &graph.Entity{ID: "host:new", Kind: "host"},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save batch record") || !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want batch metadata conflict", err)
	}
	if result.Applied != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want committed data result", result)
	}
	stored, _, err := store.loadIngestRecordWithMeta(ctx, store.ingestBatchKey("tenant-a", "aws", "collector-a", "batch-race"))
	if err != nil {
		t.Fatalf("stored record: %v", err)
	}
	if stored.Request.Items[0].ExternalID != "i-other" {
		t.Fatalf("stored record was overwritten: %#v", stored.Request)
	}
}

func TestIngestDoesNotOverwriteIdempotencyRecordCreatedDuringPublish(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	conflictingRequest := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-existing",
		IdempotencyKey: "idem-race",
		Items: []IngestItem{{
			ExternalID: "i-other",
			Entity:     &graph.Entity{ID: "host:other", Kind: "host"},
		}},
	}
	store := NewTenantStore(&raceIngestRecordStore{
		ObjectStore: objects,
		contains:    "/ingest/aws/idempotency/collector-a/idem-race.parquet",
		record: IngestBatchRecord{
			TenantID: "tenant-a",
			Request:  conflictingRequest,
			Result:   IngestResult{BatchID: "batch-existing", Version: 99, Applied: 1},
		},
	}, "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-new",
		IdempotencyKey: "idem-race",
		Items: []IngestItem{{
			ExternalID: "i-new",
			Entity:     &graph.Entity{ID: "host:new", Kind: "host"},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save idempotency record") || !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want idempotency metadata conflict", err)
	}
	if result.Applied != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want committed data result", result)
	}
	stored, _, err := store.loadIngestRecordWithMeta(ctx, store.ingestIdempotencyKey("tenant-a", "aws", "collector-a", "idem-race"))
	if err != nil {
		t.Fatalf("stored idempotency record: %v", err)
	}
	if stored.Request.BatchID != "batch-existing" || stored.Request.Items[0].ExternalID != "i-other" {
		t.Fatalf("stored idempotency record was overwritten: %#v", stored.Request)
	}
}

func TestIngestBatchIDOnlyRecordsAreScopedByCollector(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	first := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "shared-batch",
		Items: []IngestItem{{
			ExternalID: "i-a",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host"},
		}},
	}
	second := first
	second.CollectorID = "collector-b"
	second.Items = []IngestItem{{
		ExternalID: "i-b",
		Entity:     &graph.Entity{ID: "host:b", Kind: "host"},
	}}
	firstResult, err := store.Ingest(ctx, "tenant-a", first)
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	secondResult, err := store.Ingest(ctx, "tenant-a", second)
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if firstResult.Skipped || secondResult.Skipped || firstResult.Version != 1 || secondResult.Version != 2 {
		t.Fatalf("results first=%#v second=%#v, want two distinct commits", firstResult, secondResult)
	}
	replayedFirst, err := store.Ingest(ctx, "tenant-a", first)
	if err != nil {
		t.Fatalf("replay first: %v", err)
	}
	replayedSecond, err := store.Ingest(ctx, "tenant-a", second)
	if err != nil {
		t.Fatalf("replay second: %v", err)
	}
	if !replayedFirst.Skipped || replayedFirst.Version != 1 || !replayedSecond.Skipped || replayedSecond.Version != 2 {
		t.Fatalf("replays first=%#v second=%#v, want collector-scoped previous results", replayedFirst, replayedSecond)
	}
}

func TestIngestReplayIgnoresBatchRecordWithMismatchedBatchID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	badRecord := IngestBatchRecord{
		Request: IngestRequest{
			Source:      request.Source,
			CollectorID: request.CollectorID,
			BatchID:     "batch-2",
		},
		Result: IngestResult{BatchID: "batch-2", Version: 99, Applied: 1},
	}
	if err := putIngestRecordFixture(ctx, store, store.ingestBatchKey("tenant-a", request.Source, request.CollectorID, request.BatchID), badRecord); err != nil {
		t.Fatalf("put mismatched batch record: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Skipped || result.BatchID != request.BatchID || result.Version != 1 {
		t.Fatalf("result = %#v, want fresh commit for requested batch", result)
	}
}

func TestIngestReplayIgnoresBatchRecordWithMismatchedTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	badRecord := IngestBatchRecord{
		TenantID: "tenant-b",
		Request: IngestRequest{
			Source:      request.Source,
			CollectorID: request.CollectorID,
			BatchID:     request.BatchID,
		},
		Result: IngestResult{BatchID: request.BatchID, Version: 99, Applied: 1},
	}
	if err := putIngestRecordFixture(ctx, store, store.ingestBatchKey("tenant-a", request.Source, request.CollectorID, request.BatchID), badRecord); err != nil {
		t.Fatalf("put cross-tenant batch record: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Skipped || result.BatchID != request.BatchID || result.Version != 1 {
		t.Fatalf("result = %#v, want fresh commit for requested tenant", result)
	}
}

func TestIngestReplayUsesIdempotencyRecordWhenBatchIDChanges(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	replay := request
	replay.BatchID = "batch-2"
	replayed, err := store.Ingest(ctx, "tenant-a", replay)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.BatchID != result.BatchID || replayed.Version != result.Version {
		t.Fatalf("replayed = %#v, want skipped original result %#v", replayed, result)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not create another commit", manifest.Version)
	}
}

func TestIngestRejectsIdempotencyRecordWithDifferentPayloadWhenBatchIDChanges(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	if _, err := store.Ingest(ctx, "tenant-a", request); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	replay := request
	replay.BatchID = "batch-2"
	replay.Items = []IngestItem{{
		ExternalID: "i-2",
		Entity:     &graph.Entity{ID: "host:i-2", Kind: "host"},
	}}
	if _, err := store.Ingest(ctx, "tenant-a", replay); err == nil || !strings.Contains(err.Error(), "ingest record conflict") {
		t.Fatalf("replay err = %v, want idempotency conflict", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, conflicting replay should not create another commit", manifest.Version)
	}
	if _, ok := g.GetEntity("host:i-2"); ok {
		t.Fatal("conflicting idempotency replay committed host:i-2")
	}
}

func TestIngestReplayIgnoresIdempotencyRecordWithMismatchedTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	badRecord := IngestBatchRecord{
		TenantID: "tenant-b",
		Request: IngestRequest{
			Source:         request.Source,
			CollectorID:    request.CollectorID,
			BatchID:        request.BatchID,
			IdempotencyKey: request.IdempotencyKey,
		},
		Result: IngestResult{BatchID: request.BatchID, Version: 99, Applied: 1},
	}
	if err := putIngestRecordFixture(ctx, store, store.ingestIdempotencyKey("tenant-a", request.Source, request.CollectorID, request.IdempotencyKey), badRecord); err != nil {
		t.Fatalf("put cross-tenant idempotency record: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Skipped || result.BatchID != request.BatchID || result.Version != 1 {
		t.Fatalf("result = %#v, want fresh commit for requested tenant", result)
	}
}

func TestIngestReplayAcceptsLegacyRecordWithoutTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	legacyRecord := IngestBatchRecord{
		Request: request,
		Result:  IngestResult{BatchID: request.BatchID, Version: 42, Applied: 1},
	}
	if err := putIngestRecordFixture(ctx, store, store.ingestIdempotencyKey("tenant-a", request.Source, request.CollectorID, request.IdempotencyKey), legacyRecord); err != nil {
		t.Fatalf("put legacy idempotency record: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if !result.Skipped || result.Version != 42 {
		t.Fatalf("result = %#v, want skipped legacy record", result)
	}
}

func TestIngestIdempotencyAndBatchRecordsAreScopedByCollector(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	first := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-a",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host"},
		}},
	}
	second := first
	second.CollectorID = "collector-b"
	second.Items = []IngestItem{{
		ExternalID: "i-b",
		Entity:     &graph.Entity{ID: "host:b", Kind: "host"},
	}}
	firstResult, err := store.Ingest(ctx, "tenant-a", first)
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	secondResult, err := store.Ingest(ctx, "tenant-a", second)
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if firstResult.Skipped || secondResult.Skipped || firstResult.Version != 1 || secondResult.Version != 2 {
		t.Fatalf("results first=%#v second=%#v, want two distinct commits", firstResult, secondResult)
	}
	replayedFirst, err := store.Ingest(ctx, "tenant-a", first)
	if err != nil {
		t.Fatalf("replay first: %v", err)
	}
	replayedSecond, err := store.Ingest(ctx, "tenant-a", second)
	if err != nil {
		t.Fatalf("replay second: %v", err)
	}
	if !replayedFirst.Skipped || replayedFirst.Version != 1 || !replayedSecond.Skipped || replayedSecond.Version != 2 {
		t.Fatalf("replays first=%#v second=%#v, want collector-scoped previous results", replayedFirst, replayedSecond)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatal("host:a missing")
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("host:b missing")
	}
}

func TestIngestDoesNotOverwriteCorruptCollectorStatus(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	statusKey := store.collectorStatusKey("tenant-a", "aws", "collector-a")
	if err := store.Objects.Put(ctx, statusKey, []byte(`{"tenant_id":`)); err != nil {
		t.Fatalf("put corrupt status: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "save collector status") {
		t.Fatalf("err = %v, want collector status error", err)
	}
	if result.Applied != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want committed result", result)
	}
	data, err := store.Objects.Get(ctx, statusKey)
	if err != nil {
		t.Fatalf("get corrupt status: %v", err)
	}
	if string(data) != `{"tenant_id":` {
		t.Fatalf("collector status was overwritten: %s", data)
	}
}

func TestIngestDoesNotOverwriteMismatchedCollectorStatus(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	statusKey := store.collectorStatusKey("tenant-a", "aws", "collector-a")
	if err := putCollectorStatusFixture(ctx, store, statusKey, CollectorStatus{
		TenantID:    "tenant-a",
		Source:      "aws",
		CollectorID: "other-collector",
	}); err != nil {
		t.Fatalf("put status: %v", err)
	}
	if _, err := store.GetCollectorStatus(ctx, "tenant-a", " aws ", " collector-a "); err == nil || !strings.Contains(err.Error(), "collector status identity mismatch") {
		t.Fatalf("status err = %v, want identity mismatch", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "save collector status") {
		t.Fatalf("err = %v, want collector status error", err)
	}
	if result.Applied != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want committed result", result)
	}
	status, _, err := store.loadCollectorStatusWithMeta(ctx, "tenant-a", "aws", "collector-a")
	if err != nil && !strings.Contains(err.Error(), "collector status identity mismatch") {
		t.Fatalf("get status: %v", err)
	}
	status, err = decodeParquetCollectorStatus(ctx, mustGetObject(t, ctx, store, statusKey))
	if err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.CollectorID != "other-collector" {
		t.Fatalf("collector status was overwritten: %#v", status)
	}
}

func TestIngestReplayRepairsCollectorStatusAfterMetadataFailure(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(&failPutOnceStore{ObjectStore: base, contains: "/ingest/aws/collectors/collector-a.parquet"}, "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save collector status") {
		t.Fatalf("err = %v, want collector status metadata error", err)
	}
	if result.Applied != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want committed result", result)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Version != 1 {
		t.Fatalf("replayed = %#v, want skipped original result", replayed)
	}
	status, err := store.GetCollectorStatus(ctx, "tenant-a", "aws", "collector-a")
	if err != nil {
		t.Fatalf("collector status: %v", err)
	}
	if status.LastBatchID != "batch-1" || status.LastVersion != 1 || status.AppliedTotal != 1 || status.FailedTotal != 0 {
		t.Fatalf("collector status = %#v, want repaired status for original batch", status)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not create another commit", manifest.Version)
	}
}

func TestIngestReplayRepairsDeadLetterAfterMetadataFailure(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(&failPutOnceStore{ObjectStore: base, contains: "/ingest/aws/deadletters/"}, "test")
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{
			{ExternalID: "bad-empty"},
		},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save dead letter") {
		t.Fatalf("err = %v, want dead letter metadata error", err)
	}
	if result.Applied != 0 || result.Failed != 1 || result.Version != 0 {
		t.Fatalf("result = %#v, want failed batch without commit", result)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Failed != 1 {
		t.Fatalf("replayed = %#v, want skipped failed result", replayed)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "aws")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].Status != "pending" || letters[0].BatchID != "batch-1" {
		t.Fatalf("deadletters = %#v, want repaired pending deadletter", letters)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 0 {
		t.Fatalf("manifest version = %d, failed replay should not create a commit", manifest.Version)
	}
}

func TestIngestCollectorStatusCASRetryPreservesConcurrentTotals(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(&raceCollectorStatusStore{
		ObjectStore: base,
		contains:    "/ingest/aws/collectors/collector-a.parquet",
		status: CollectorStatus{
			TenantID:     "tenant-a",
			Source:       "aws",
			CollectorID:  "collector-a",
			LastBatchID:  "competing-batch",
			AppliedTotal: 7,
			FailedTotal:  2,
		},
	}, "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Applied != 1 || result.Version != 1 {
		t.Fatalf("result = %#v, want committed ingest", result)
	}
	status, err := store.GetCollectorStatus(ctx, "tenant-a", "aws", "collector-a")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.AppliedTotal != 8 || status.FailedTotal != 2 || status.LastBatchID != "batch-1" {
		t.Fatalf("status = %#v, want concurrent totals preserved", status)
	}
}

func concurrentIngestRequest(batch int) IngestRequest {
	id := fmt.Sprintf("host:%03d", batch)
	return IngestRequest{
		Source:         "loadtest",
		CollectorID:    "collector-a",
		BatchID:        fmt.Sprintf("batch-%03d", batch),
		IdempotencyKey: fmt.Sprintf("batch-%03d", batch),
		Items: []IngestItem{{
			ExternalID: id,
			Entity:     &graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"hostname": id}},
		}},
	}
}

type failPutStore struct {
	ObjectStore
	contains string
}

func (s *failPutStore) Put(ctx context.Context, key string, data []byte) error {
	if strings.Contains(key, s.contains) {
		return errors.New("injected put failure")
	}
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *failPutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.Contains(key, s.contains) {
		return ObjectMeta{}, errors.New("injected put failure")
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

type failPutOnceStore struct {
	ObjectStore
	contains string
	mu       sync.Mutex
	failed   bool
}

func (s *failPutOnceStore) Put(ctx context.Context, key string, data []byte) error {
	if s.shouldFail(key) {
		return errors.New("injected put failure")
	}
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *failPutOnceStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldFail(key) {
		return ObjectMeta{}, errors.New("injected put failure")
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *failPutOnceStore) shouldFail(key string) bool {
	if !strings.Contains(key, s.contains) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return false
	}
	s.failed = true
	return true
}

func putIngestRecordFixture(ctx context.Context, store *TenantStore, key string, record IngestBatchRecord) error {
	data, err := marshalParquetIngestRecord(ctx, record)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, key, data)
}

func putCollectorStatusFixture(ctx context.Context, store *TenantStore, key string, status CollectorStatus) error {
	data, err := marshalParquetCollectorStatus(ctx, status)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, key, data)
}

func mustGetObject(t *testing.T, ctx context.Context, store *TenantStore, key string) []byte {
	t.Helper()
	data, err := store.Objects.Get(ctx, key)
	if err != nil {
		t.Fatalf("get object %s: %v", key, err)
	}
	return data
}

func assertParquetIngestRecord(t *testing.T, ctx context.Context, store *TenantStore, key string, request IngestRequest) {
	t.Helper()
	data := mustGetObject(t, ctx, store, key)
	if !strings.HasSuffix(key, ".parquet") || !isParquetBytes(data) {
		t.Fatalf("ingest record object key=%q parquet=%v", key, isParquetBytes(data))
	}
	record, err := decodeParquetIngestRecord(ctx, data)
	if err != nil {
		t.Fatalf("decode ingest record %s: %v", key, err)
	}
	if !ingestRecordMatchesRequest(record, "tenant-a", request) {
		t.Fatalf("ingest record = %#v, want request %#v", record, request)
	}
}

func assertParquetCollectorStatus(t *testing.T, ctx context.Context, store *TenantStore, key string, tenantID string, source string, collectorID string) {
	t.Helper()
	data := mustGetObject(t, ctx, store, key)
	if !strings.HasSuffix(key, ".parquet") || !isParquetBytes(data) {
		t.Fatalf("collector status object key=%q parquet=%v", key, isParquetBytes(data))
	}
	status, err := decodeParquetCollectorStatus(ctx, data)
	if err != nil {
		t.Fatalf("decode collector status %s: %v", key, err)
	}
	if status.TenantID != tenantID || status.Source != source || status.CollectorID != collectorID {
		t.Fatalf("collector status = %#v", status)
	}
}

func assertParquetDeadLetter(t *testing.T, ctx context.Context, store *TenantStore, key string, tenantID string, source string, id string) {
	t.Helper()
	data := mustGetObject(t, ctx, store, key)
	if !strings.HasSuffix(key, ".parquet") || !isParquetBytes(data) {
		t.Fatalf("deadletter object key=%q parquet=%v", key, isParquetBytes(data))
	}
	letter, err := decodeParquetDeadLetter(ctx, data)
	if err != nil {
		t.Fatalf("decode deadletter %s: %v", key, err)
	}
	if letter.TenantID != tenantID || letter.Source != source || letter.ID != id {
		t.Fatalf("deadletter = %#v", letter)
	}
}

type raceIngestRecordStore struct {
	ObjectStore
	contains string
	record   IngestBatchRecord
	once     sync.Once
}

func (s *raceIngestRecordStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if condition.IfNoneMatch && strings.Contains(key, s.contains) {
		var writeErr error
		s.once.Do(func() {
			payload, err := marshalParquetIngestRecord(ctx, s.record)
			if err != nil {
				writeErr = err
				return
			}
			writeErr = s.ObjectStore.Put(ctx, key, payload)
		})
		if writeErr != nil {
			return ObjectMeta{Key: key}, writeErr
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

type raceCollectorStatusStore struct {
	ObjectStore
	contains string
	status   CollectorStatus
	once     sync.Once
}

func (s *raceCollectorStatusStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.Contains(key, s.contains) {
		var writeErr error
		s.once.Do(func() {
			payload, err := marshalParquetCollectorStatus(ctx, s.status)
			if err != nil {
				writeErr = err
				return
			}
			writeErr = s.ObjectStore.Put(ctx, key, payload)
		})
		if writeErr != nil {
			return ObjectMeta{Key: key}, writeErr
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}
