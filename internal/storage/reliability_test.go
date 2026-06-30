package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"graphdb/internal/graph"
)

func TestWriterLeaseBlocksOtherOwnersUntilExpiry(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	first := NewTenantStore(objects, "test")
	first.LeaseTTL = time.Hour
	second := NewTenantStore(objects, "test")
	if _, err := first.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	_, err := second.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second commit error = %v, want ErrLeaseHeld", err)
	}

	expiring := NewTenantStore(NewMemoryStore(), "test")
	expiring.LeaseTTL = time.Millisecond
	if _, err := expiring.Commit(ctx, "tenant-b", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("expiring commit: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	takeover := NewTenantStore(expiring.Objects, "test")
	if _, err := takeover.Commit(ctx, "tenant-b", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("takeover commit after lease expiry: %v", err)
	}
}

func TestWriterLeaseRejectsCrossTenantObject(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	key := store.writerLeaseKey("tenant-a")
	if err := putWriterLeaseFixture(ctx, store, key, WriterLease{
		TenantID:  "tenant-b",
		OwnerID:   "other",
		ExpiresAt: time.Now().UTC().Add(time.Hour),
		UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("put lease: %v", err)
	}
	if _, err := store.GetWriterLease(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "writer lease tenant mismatch") {
		t.Fatalf("lease err = %v, want tenant mismatch", err)
	}
	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	if err == nil || !strings.Contains(err.Error(), "writer lease tenant mismatch") {
		t.Fatalf("commit err = %v, want tenant mismatch", err)
	}
	lease, err := getWriterLeaseFixture(ctx, store, key)
	if err != nil {
		t.Fatalf("get lease: %v", err)
	}
	if lease.TenantID != "tenant-b" || lease.OwnerID != "other" {
		t.Fatalf("cross tenant lease was overwritten: %#v", lease)
	}
}

func TestWriterLeaseAcceptsEmptyTenantObject(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.LeaseTTL = time.Hour
	key := store.writerLeaseKey("tenant-a")
	now := time.Now().UTC()
	if err := putWriterLeaseFixture(ctx, store, key, WriterLease{
		OwnerID:   "other",
		ExpiresAt: now.Add(time.Hour),
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("put legacy lease: %v", err)
	}
	lease, err := store.GetWriterLease(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get legacy lease: %v", err)
	}
	if lease.TenantID != "tenant-a" || lease.OwnerID != "other" {
		t.Fatalf("legacy lease = %#v", lease)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("commit err = %v, want ErrLeaseHeld", err)
	}
}

func TestWriterLeaseCanTakeOverExpiredEmptyTenantObject(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	key := store.writerLeaseKey("tenant-a")
	now := time.Now().UTC()
	if err := putWriterLeaseFixture(ctx, store, key, WriterLease{
		OwnerID:   "old",
		ExpiresAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("put expired legacy lease: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit with expired lease: %v", err)
	}
	lease, err := getWriterLeaseFixture(ctx, store, key)
	if err != nil {
		t.Fatalf("get rewritten lease: %v", err)
	}
	if lease.TenantID != "tenant-a" || lease.OwnerID != store.InstanceID {
		t.Fatalf("rewritten lease = %#v", lease)
	}
}

func putWriterLeaseFixture(ctx context.Context, store *TenantStore, key string, lease WriterLease) error {
	data, err := marshalParquetWriterLease(ctx, lease)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, key, data)
}

func getWriterLeaseFixture(ctx context.Context, store *TenantStore, key string) (WriterLease, error) {
	data, err := store.Objects.Get(ctx, key)
	if err != nil {
		return WriterLease{}, err
	}
	return decodeParquetWriterLease(ctx, data)
}

func TestCommitRetryReacquiresWriterLeaseAfterManifestConflict(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &takeoverOnManifestPutStore{ObjectStore: base, base: base, tenantID: "tenant-a"}
	store := NewTenantStore(objects, "test")
	store.LeaseTTL = time.Nanosecond
	store.MaxRetries = 2

	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("commit err = %v, want ErrLeaseHeld", err)
	}
	if !objects.triggered {
		t.Fatal("test store did not trigger takeover")
	}

	reader := NewTenantStore(base, "test")
	g, manifest, err := reader.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want takeover version 1", manifest.Version)
	}
	if _, ok := g.Entities["host:takeover"]; !ok {
		t.Fatal("takeover entity missing")
	}
	if _, ok := g.Entities["host:first"]; ok {
		t.Fatal("expired writer entity became visible after retry")
	}
}

func TestDeadLetterReplayResolvesFailedIngest(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "edge-before-node",
		Items: []IngestItem{{
			ExternalID: "edge",
			Edge:       &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"},
		}},
	})
	if err != nil {
		t.Fatalf("ingest failed edge: %v", err)
	}
	if result.Failed == 0 {
		t.Fatalf("result = %#v, want failure", result)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].Status != "pending" {
		t.Fatalf("deadletters = %#v", letters)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
			{ID: "host:b", Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit endpoints: %v", err)
	}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if report.Resolved != 1 || report.Failed != 0 {
		t.Fatalf("replay report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	edgeID := graph.CanonicalEdgeIDParts("connects_to", "host:a", "host:b")
	edge, ok := g.Edges[edgeID]
	if !ok {
		t.Fatal("replayed edge missing")
	}
	if !hasEdgeSourceAlias(edge.Sources, "edge:a-b") {
		t.Fatalf("edge sources = %#v", edge.Sources)
	}
}

func TestDeadLetterReplayClaimPreventsStaleDuplicateReplay(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "edge-before-node",
		Items: []IngestItem{{
			ExternalID: "edge",
			Edge:       &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"},
		}},
	})
	if err != nil || result.Failed == 0 {
		t.Fatalf("ingest result=%#v err=%v, want failed deadletter", result, err)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil || len(letters) != 1 {
		t.Fatalf("deadletters=%#v err=%v", letters, err)
	}
	stale := letters[0]
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
			{ID: "host:b", Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit endpoints: %v", err)
	}
	if replayed, claimed, err := store.replayDeadLetter(ctx, "tenant-a", "agent", stale); err != nil || !claimed || replayed.Failed != 0 {
		t.Fatalf("first replay result=%#v claimed=%v err=%v", replayed, claimed, err)
	}
	if replayed, claimed, err := store.replayDeadLetter(ctx, "tenant-a", "agent", stale); err != nil || claimed {
		t.Fatalf("second stale replay result=%#v claimed=%v err=%v, want skipped", replayed, claimed, err)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want one replay commit at version 2", manifest.Version)
	}
	letters, err = store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil || len(letters) != 1 || letters[0].Status != "resolved" || letters[0].Attempts != 1 {
		t.Fatalf("deadletters after replay=%#v err=%v", letters, err)
	}
}

func TestDeadLetterReplayResolvedAfterMetadataErrorDoesNotReplayAgain(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "edge-before-node",
		Items: []IngestItem{{
			ExternalID: "edge",
			Edge:       &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"},
		}},
	})
	if err != nil || result.Failed == 0 {
		t.Fatalf("ingest result=%#v err=%v, want failed deadletter", result, err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
			{ID: "host:b", Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit endpoints: %v", err)
	}
	store.Objects = &failPutStore{ObjectStore: base, contains: "/ingest/agent/collectors/collector-a.parquet"}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err == nil || !strings.Contains(err.Error(), "save collector status") {
		t.Fatalf("replay err = %v, want collector status metadata error", err)
	}
	if report.Replayed != 1 || report.Resolved != 1 || len(report.Results) != 1 || report.Results[0].Version != 2 {
		t.Fatalf("report = %#v, want committed replay result even when metadata save fails", report)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil || len(letters) != 1 || letters[0].Status != "resolved" || letters[0].Attempts != 1 {
		t.Fatalf("deadletters after metadata error=%#v err=%v", letters, err)
	}
	store.Objects = base
	report, err = store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if report.Scanned != 0 || report.Replayed != 0 {
		t.Fatalf("second replay report=%#v, want resolved letter skipped", report)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want one replay commit at version 2", manifest.Version)
	}
}

func TestDeadLetterReplayRejectsNegativeLimit(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	_, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", -1)
	if err == nil || err.Error() != "limit must be a non-negative integer" {
		t.Fatalf("err = %v, want negative limit validation", err)
	}
}

func TestDeadLetterScopeTrimsSourceBeforeValidation(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "trim-source",
		Items: []IngestItem{{
			ExternalID: "edge",
			Edge:       &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"},
		}},
	})
	if err != nil {
		t.Fatalf("ingest failed edge: %v", err)
	}
	if result.Failed == 0 {
		t.Fatalf("result = %#v, want failure", result)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", " agent ")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].Status != "pending" || letters[0].Source != "agent" {
		t.Fatalf("deadletters = %#v", letters)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
			{ID: "host:b", Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit endpoints: %v", err)
	}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", " agent ", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if report.Resolved != 1 || report.Failed != 0 {
		t.Fatalf("replay report = %#v", report)
	}
}

func TestDeadLetterStoresOnlyFailedPartialIngestItems(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "partial-batch",
		Items: []IngestItem{
			{ExternalID: "host-ok", Entity: &graph.Entity{ID: "host:ok", Kind: "host"}},
			{ExternalID: "bad-empty"},
		},
	})
	if err != nil {
		t.Fatalf("partial ingest: %v", err)
	}
	if result.Applied != 1 || result.Failed != 1 || result.Version != 1 {
		t.Fatalf("result = %#v", result)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 1 || len(letters[0].Request.Items) != 1 || letters[0].Request.Items[0].ExternalID != "bad-empty" {
		t.Fatalf("deadletters = %#v, want only failed item", letters)
	}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if report.Replayed != 1 || report.Resolved != 0 || report.Failed != 1 {
		t.Fatalf("replay report = %#v", report)
	}
	_, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, replay should not recommit successful item", manifest.Version)
	}
	letters, err = store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters after replay: %v", err)
	}
	if len(letters) != 1 || letters[0].Attempts != 1 || letters[0].Status != "pending" {
		t.Fatalf("deadletters after replay = %#v, want original pending record only", letters)
	}
}

func TestDeadLetterKeepsAppliedItemsForAtomicCommitFailure(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "atomic-failure",
		Items: []IngestItem{
			{ExternalID: "host-a", Entity: &graph.Entity{ID: "host:a", Kind: "host"}},
			{ExternalID: "edge-a-b", Edge: &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"}},
		},
	})
	if err != nil {
		t.Fatalf("atomic failure ingest: %v", err)
	}
	if result.Applied != 0 || result.Failed != 2 || result.Version != 0 {
		t.Fatalf("result = %#v, want atomic failure for both built items", result)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 1 || len(letters[0].Request.Items) != 2 {
		t.Fatalf("deadletters = %#v, want both failed applied items", letters)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit missing endpoint: %v", err)
	}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if report.Resolved != 1 || report.Failed != 0 {
		t.Fatalf("replay report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatal("replayed entity missing")
	}
	if _, ok := g.Edges[graph.CanonicalEdgeIDParts("connects_to", "host:a", "host:b")]; !ok {
		t.Fatal("replayed edge missing")
	}
}

func TestDeadLettersAreScopedByCollector(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	first := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "shared-bad-batch",
		Items: []IngestItem{{
			ExternalID: "edge-a",
			Edge:       &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"},
		}},
	}
	second := first
	second.CollectorID = "collector-b"
	second.Items = []IngestItem{{
		ExternalID: "edge-c",
		Edge:       &graph.Edge{ID: "edge:c-d", Type: "connects_to", From: "host:c", To: "host:d"},
	}}
	if result, err := store.Ingest(ctx, "tenant-a", first); err != nil || result.Failed != 1 {
		t.Fatalf("first result=%#v err=%v, want one failure", result, err)
	}
	if result, err := store.Ingest(ctx, "tenant-a", second); err != nil || result.Failed != 1 {
		t.Fatalf("second result=%#v err=%v, want one failure", result, err)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 2 {
		t.Fatalf("deadletters = %#v, want both collector records", letters)
	}
	got := map[string]string{}
	for _, letter := range letters {
		got[letter.ID] = letter.Request.CollectorID
	}
	if got["collector-a/shared-bad-batch"] != "collector-a" || got["collector-b/shared-bad-batch"] != "collector-b" {
		t.Fatalf("deadletter collector scope = %#v", got)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
			{ID: "host:b", Kind: "host"},
			{ID: "host:c", Kind: "host"},
			{ID: "host:d", Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit endpoints: %v", err)
	}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if report.Resolved != 2 || report.Failed != 0 {
		t.Fatalf("replay report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, edgeID := range []string{
		graph.CanonicalEdgeIDParts("connects_to", "host:a", "host:b"),
		graph.CanonicalEdgeIDParts("connects_to", "host:c", "host:d"),
	} {
		if _, ok := g.Edges[edgeID]; !ok {
			t.Fatalf("replayed edge %s missing", edgeID)
		}
	}
}

func TestDeadLetterListAndReplaySkipInvalidRecords(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "edge-before-node",
		Items: []IngestItem{{
			ExternalID: "edge",
			Edge:       &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"},
		}},
	})
	if err != nil {
		t.Fatalf("ingest failed edge: %v", err)
	}
	if result.Failed == 0 {
		t.Fatalf("result = %#v, want failure", result)
	}
	invalidKey := store.deadLetterKey("tenant-a", "agent", "broken/id")
	if err := store.Objects.Put(ctx, invalidKey, []byte(`{"id":`)); err != nil {
		t.Fatalf("put invalid deadletter: %v", err)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	var pending, invalid int
	for _, letter := range letters {
		switch letter.Status {
		case "pending":
			pending++
		case "invalid":
			invalid++
			if letter.ID != "broken/id" || letter.Error == "" {
				t.Fatalf("invalid deadletter = %#v", letter)
			}
		}
	}
	if pending != 1 || invalid != 1 {
		t.Fatalf("deadletters = %#v, want one pending and one invalid", letters)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
			{ID: "host:b", Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit endpoints: %v", err)
	}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if report.Scanned != 1 || report.Resolved != 1 || report.Failed != 0 {
		t.Fatalf("replay report = %#v", report)
	}
	if _, err := store.Objects.Get(ctx, invalidKey); err != nil {
		t.Fatalf("invalid deadletter should be retained for operator repair: %v", err)
	}
}

func TestDeadLetterListSkipsObjectsMissingAfterList(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &deleteDeadLetterAfterListStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	putPendingDeadLetterForTest(t, ctx, store, "tenant-a", "agent", "stale")

	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if !objects.deleted {
		t.Fatal("test store did not delete a listed deadletter")
	}
	if len(letters) != 0 {
		t.Fatalf("deadletters = %#v, want stale list entry skipped", letters)
	}
}

func TestDeadLetterReplaySkipsObjectsMissingAfterList(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &deleteDeadLetterAfterListStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	putPendingDeadLetterForTest(t, ctx, store, "tenant-a", "agent", "stale")

	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "agent", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if !objects.deleted {
		t.Fatal("test store did not delete a listed deadletter")
	}
	if report.Scanned != 0 || report.Replayed != 0 || report.Resolved != 0 || report.Failed != 0 {
		t.Fatalf("replay report = %#v, want stale list entry skipped", report)
	}
}

func TestDeadLetterListAndReplaySkipScopeMismatchedRecords(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	mismatched := DeadLetter{
		ID:       "scope-mismatch",
		TenantID: "tenant-b",
		Source:   "manual",
		Status:   "pending",
		Request: IngestRequest{
			Source:      "manual",
			CollectorID: "operator",
			BatchID:     "scope-mismatch",
			Items: []IngestItem{{
				ExternalID: "host:manual",
				Entity:     &graph.Entity{ID: "host:manual", Kind: "host"},
			}},
		},
	}
	key := store.deadLetterKey("tenant-a", "aws", mismatched.ID)
	if _, err := store.putDeadLetterWithMeta(ctx, key, mismatched, ObjectMeta{Key: key}); err != nil {
		t.Fatalf("put mismatched deadletter: %v", err)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "aws")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].Status != "invalid" || letters[0].ID != mismatched.ID {
		t.Fatalf("deadletters = %#v", letters)
	}
	report, err := store.ReplayDeadLetters(ctx, "tenant-a", "aws", 10)
	if err != nil {
		t.Fatalf("replay deadletters: %v", err)
	}
	if report.Scanned != 0 || report.Replayed != 0 {
		t.Fatalf("replay report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:manual"); ok {
		t.Fatal("scope mismatched deadletter was replayed")
	}
}

func hasEdgeSourceAlias(sources []graph.EdgeSource, edgeID string) bool {
	for _, source := range sources {
		if source.EdgeID == edgeID {
			return true
		}
	}
	return false
}

func TestRecoverTenantAndCleanupStaleCommits(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	orphan := graph.Commit{
		ID:        "orphan-v2",
		TenantID:  "tenant-a",
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	orphanKey := store.commitKey("tenant-a", orphan.Version, orphan.ID)
	if err := store.putCommitObjectIfAbsent(ctx, orphanKey, orphan); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Recovered != 1 || report.EndVersion != 2 {
		t.Fatalf("recovery report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("recovered entity missing")
	}
	stale := orphan
	stale.ID = "stale-v2"
	staleKey := store.commitKey("tenant-a", stale.Version, stale.ID)
	if err := store.putCommitObjectIfAbsent(ctx, staleKey, stale); err != nil {
		t.Fatalf("put stale: %v", err)
	}
	cleanup, err := store.CleanupCommits(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if cleanup.Deleted != 1 {
		t.Fatalf("cleanup report = %#v", cleanup)
	}
	if _, err := store.Objects.Get(ctx, staleKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale commit still exists: %v", err)
	}
}

func TestRecoverTenantSkipsVanishedListedOrphanCommit(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	orphan := graph.Commit{
		ID:        "vanished-v2",
		TenantID:  "tenant-a",
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	orphanKey := store.commitKey("tenant-a", orphan.Version, orphan.ID)
	if err := store.putCommitObjectIfAbsent(ctx, orphanKey, orphan); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	store.Objects = &deleteListedCommitStore{ObjectStore: base, key: orphanKey}
	store.deleteWriteCache("tenant-a")

	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Recovered != 0 || report.Blocked != 0 || report.EndVersion != 1 {
		t.Fatalf("recovery report = %#v", report)
	}
	if !store.Objects.(*deleteListedCommitStore).deleted {
		t.Fatal("test store did not delete the listed orphan commit")
	}
}

func TestCleanupCommitsSkipsVanishedListedOrphanCommit(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	stale := graph.Commit{
		ID:        "vanished-v1",
		TenantID:  "tenant-a",
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:stale", Kind: "host"}}},
	}
	staleKey := store.commitKey("tenant-a", stale.Version, stale.ID)
	if err := store.putCommitObjectIfAbsent(ctx, staleKey, stale); err != nil {
		t.Fatalf("put stale orphan: %v", err)
	}
	store.Objects = &deleteListedCommitStore{ObjectStore: base, key: staleKey}

	report, err := store.CleanupCommits(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if report.Deleted != 0 || len(report.InvalidKeys) != 0 {
		t.Fatalf("cleanup report = %#v", report)
	}
	if !store.Objects.(*deleteListedCommitStore).deleted {
		t.Fatal("test store did not delete the listed stale commit")
	}
}

func TestRecoverAndCleanupSkipInvalidOrphanCommitObjects(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	orphan := graph.Commit{
		ID:        "orphan-v2",
		TenantID:  "tenant-a",
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	orphanKey := store.commitKey("tenant-a", orphan.Version, orphan.ID)
	if err := store.putCommitObjectIfAbsent(ctx, orphanKey, orphan); err != nil {
		t.Fatalf("put valid orphan: %v", err)
	}
	invalidKey := store.commitKey("tenant-a", 2, "invalid-json")
	if err := store.Objects.Put(ctx, invalidKey, []byte(`{"id":`)); err != nil {
		t.Fatalf("put invalid orphan: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Recovered != 1 || report.EndVersion != 2 || report.Blocked != 1 || len(report.InvalidKeys) != 1 || report.InvalidKeys[0] != invalidKey {
		t.Fatalf("recovery report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("valid orphan was not recovered")
	}

	stale := orphan
	stale.ID = "stale-v2"
	staleKey := store.commitKey("tenant-a", stale.Version, stale.ID)
	if err := store.putCommitObjectIfAbsent(ctx, staleKey, stale); err != nil {
		t.Fatalf("put stale orphan: %v", err)
	}
	cleanup, err := store.CleanupCommits(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if cleanup.Deleted != 1 || len(cleanup.InvalidKeys) != 1 || cleanup.InvalidKeys[0] != invalidKey {
		t.Fatalf("cleanup report = %#v", cleanup)
	}
	if _, err := store.Objects.Get(ctx, staleKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale commit still exists: %v", err)
	}
	if _, err := store.Objects.Get(ctx, invalidKey); err != nil {
		t.Fatalf("invalid commit should be retained for operator repair: %v", err)
	}
}

func TestRecoverAndCleanupSkipMismatchedOrphanCommitObject(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	mismatched := graph.Commit{
		ID:        "object-v2",
		TenantID:  "tenant-a",
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	mismatchedKey := store.commitKey("tenant-a", mismatched.Version, "path-v2")
	if err := store.putCommitObjectIfAbsent(ctx, mismatchedKey, mismatched); err != nil {
		t.Fatalf("put mismatched orphan: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Recovered != 0 || report.EndVersion != 1 || report.Blocked != 1 || len(report.InvalidKeys) != 1 || report.InvalidKeys[0] != mismatchedKey {
		t.Fatalf("recovery report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("mismatched orphan was applied")
	}
	cleanup, err := store.CleanupCommits(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if cleanup.Deleted != 0 || len(cleanup.InvalidKeys) != 1 || cleanup.InvalidKeys[0] != mismatchedKey {
		t.Fatalf("cleanup report = %#v", cleanup)
	}
	if _, err := store.Objects.Get(ctx, mismatchedKey); err != nil {
		t.Fatalf("mismatched orphan should be retained for operator repair: %v", err)
	}
}

func TestRecoverSkipsCrossTenantOrphanCommit(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	crossTenant := graph.Commit{
		ID:        "cross-tenant-v2",
		TenantID:  "tenant-b",
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	crossTenantKey := store.commitKey("tenant-a", crossTenant.Version, crossTenant.ID)
	if err := store.putCommitObjectIfAbsent(ctx, crossTenantKey, crossTenant); err != nil {
		t.Fatalf("put cross tenant orphan: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Recovered != 0 || report.EndVersion != 1 || report.Blocked != 1 || len(report.InvalidKeys) != 1 || report.InvalidKeys[0] != crossTenantKey {
		t.Fatalf("recovery report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("cross tenant orphan was applied")
	}
	if _, err := store.Objects.Get(ctx, crossTenantKey); err != nil {
		t.Fatalf("cross tenant orphan should be retained for operator repair: %v", err)
	}
}

func TestCleanupKeepsCrossTenantOrphanCommit(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	crossTenant := graph.Commit{
		ID:        "cross-tenant-v1",
		TenantID:  "tenant-b",
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	crossTenantKey := store.commitKey("tenant-a", crossTenant.Version, crossTenant.ID)
	if err := store.putCommitObjectIfAbsent(ctx, crossTenantKey, crossTenant); err != nil {
		t.Fatalf("put cross tenant stale orphan: %v", err)
	}
	report, err := store.CleanupCommits(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if report.Deleted != 0 || len(report.InvalidKeys) != 1 || report.InvalidKeys[0] != crossTenantKey {
		t.Fatalf("cleanup report = %#v", report)
	}
	if _, err := store.Objects.Get(ctx, crossTenantKey); err != nil {
		t.Fatalf("cross tenant orphan should be retained for operator repair: %v", err)
	}
}

func TestRecoverSkipsMissingTenantOrphanCommit(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	missingTenant := graph.Commit{
		ID:        "missing-tenant-v2",
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	missingTenantKey := store.commitKey("tenant-a", missingTenant.Version, missingTenant.ID)
	if err := store.putCommitObjectIfAbsent(ctx, missingTenantKey, missingTenant); err != nil {
		t.Fatalf("put missing tenant orphan: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Recovered != 0 || report.EndVersion != 1 || report.Blocked != 1 || len(report.InvalidKeys) != 1 || report.InvalidKeys[0] != missingTenantKey {
		t.Fatalf("recovery report = %#v", report)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("missing tenant orphan was applied")
	}
	if _, err := store.Objects.Get(ctx, missingTenantKey); err != nil {
		t.Fatalf("missing tenant orphan should be retained for operator repair: %v", err)
	}
}

func TestCleanupKeepsMissingTenantOrphanCommit(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	missingTenant := graph.Commit{
		ID:        "missing-tenant-v1",
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	missingTenantKey := store.commitKey("tenant-a", missingTenant.Version, missingTenant.ID)
	if err := store.putCommitObjectIfAbsent(ctx, missingTenantKey, missingTenant); err != nil {
		t.Fatalf("put missing tenant stale orphan: %v", err)
	}
	report, err := store.CleanupCommits(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if report.Deleted != 0 || len(report.InvalidKeys) != 1 || report.InvalidKeys[0] != missingTenantKey {
		t.Fatalf("cleanup report = %#v", report)
	}
	if _, err := store.Objects.Get(ctx, missingTenantKey); err != nil {
		t.Fatalf("missing tenant orphan should be retained for operator repair: %v", err)
	}
}

func TestRecoverTenantUpdatesIncrementalIndexes(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
			},
		}},
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "a"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	orphan := graph.Commit{
		ID:        "orphan-v2-indexed",
		TenantID:  "tenant-a",
		Version:   2,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host", Fields: graph.Fields{"hostname": "b"}}},
		},
	}
	orphanKey := store.commitKey("tenant-a", orphan.Version, orphan.ID)
	if err := store.putCommitObjectIfAbsent(ctx, orphanKey, orphan); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.Recovered != 1 || report.EndVersion != 2 {
		t.Fatalf("recovery report = %#v", report)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if catalog.Version != 2 {
		t.Fatalf("catalog version = %d, want 2", catalog.Version)
	}
	lookup := PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: 2, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"b"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:b" {
		t.Fatalf("index lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
	entity, ok, err := lookup.GetEntity(ctx, "host:b", []string{"hostname"})
	if err != nil || !ok || entity.Fields["hostname"] != "b" {
		t.Fatalf("entity lookup=%#v ok=%v err=%v", entity, ok, err)
	}
}

type takeoverOnManifestPutStore struct {
	ObjectStore
	base      *MemoryStore
	tenantID  string
	triggered bool
}

func (s *takeoverOnManifestPutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.HasSuffix(key, "/manifest.parquet") && !s.triggered {
		s.triggered = true
		time.Sleep(time.Millisecond)
		takeover := NewTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.Commit(ctx, s.tenantID, graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:takeover", Kind: "host"}},
		}, CommitOptions{}); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func putPendingDeadLetterForTest(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, source string, id string) {
	t.Helper()
	now := time.Now().UTC()
	letter := DeadLetter{
		ID:       id,
		TenantID: tenantID,
		Source:   source,
		BatchID:  id,
		Request: IngestRequest{
			Source:  source,
			BatchID: id,
		},
		LastResult: IngestResult{
			BatchID: id,
			Failed:  1,
		},
		Status:    "pending",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if _, err := store.putDeadLetterWithMeta(ctx, store.deadLetterKey(tenantID, source, id), letter, ObjectMeta{Key: store.deadLetterKey(tenantID, source, id)}); err != nil {
		t.Fatalf("put deadletter: %v", err)
	}
}

type deleteDeadLetterAfterListStore struct {
	ObjectStore
	deleted bool
}

func (s *deleteDeadLetterAfterListStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items, err := s.ObjectStore.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if s.deleted || !strings.Contains(prefix, "/deadletters/") {
		return items, nil
	}
	for _, item := range items {
		if !strings.HasPrefix(item.Key, prefix) {
			continue
		}
		if err := s.ObjectStore.Delete(ctx, item.Key); err != nil {
			return nil, err
		}
		s.deleted = true
		break
	}
	return items, nil
}

type deleteListedCommitStore struct {
	ObjectStore
	key     string
	deleted bool
}

func (s *deleteListedCommitStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items, err := s.ObjectStore.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if s.deleted {
		return items, nil
	}
	for _, item := range items {
		if item.Key != s.key {
			continue
		}
		if err := s.ObjectStore.Delete(ctx, item.Key); err != nil {
			return nil, err
		}
		s.deleted = true
		break
	}
	return items, nil
}
