package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"graphdb/internal/graph"
)

func TestPutSourcePolicyRequiresWriterLease(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	owner := NewTenantStore(objects, "test")
	owner.LeaseTTL = time.Hour
	if _, err := owner.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	other := NewTenantStore(objects, "test")
	_, err := other.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		Sources: []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
	})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("put policy error = %v, want ErrLeaseHeld", err)
	}
	_, configured, err := owner.GetSourcePolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if configured {
		t.Fatal("policy was written despite held writer lease")
	}
}

func TestSourcePolicyRecordIncludesTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	policy, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 1,
		Sources:         []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
	})
	if err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if policy.DefaultPriority != 1 || len(policy.Sources) != 1 {
		t.Fatalf("policy = %#v", policy)
	}
	data, err := store.Objects.Get(ctx, store.sourcePolicyKey("tenant-a"))
	if err != nil {
		t.Fatalf("get raw policy: %v", err)
	}
	if !isParquetBytes(data) {
		t.Fatal("source policy object is not parquet")
	}
	record, err := decodeParquetSourcePolicy(ctx, data)
	if err != nil {
		t.Fatalf("decode raw policy: %v", err)
	}
	if record.TenantID != "tenant-a" || record.DefaultPriority != 1 || record.Sources[0].Name != "manual" {
		t.Fatalf("record = %#v", record)
	}
}

func TestPutSourcePolicyDoesNotOverwriteAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	objects := &takeoverDuringSourcePolicyPutStore{
		ObjectStore: base,
		base:        base,
		tenantID:    "tenant-a",
		triggerKey:  store.sourcePolicyKey("tenant-a"),
	}
	stale := NewTenantStore(objects, "test")
	stale.LeaseTTL = time.Nanosecond
	_, err := stale.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 1,
		Sources:         []graph.SourcePolicyItem{{Name: "agent", Priority: 100}},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale put err = %v, want ErrConflict", err)
	}
	if !objects.Triggered() {
		t.Fatal("test store did not trigger takeover")
	}

	policy, configured, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if !configured || policy.DefaultPriority != 9 || len(policy.Sources) != 1 || policy.Sources[0].Name != "manual" {
		t.Fatalf("policy after takeover = configured=%v %#v", configured, policy)
	}
}

func TestGetSourcePolicyRejectsMismatchedTenant(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putSourcePolicyFixture(ctx, store, "tenant-a", sourcePolicyRecord{
		TenantID: "tenant-b",
		SourcePolicy: graph.SourcePolicy{
			Sources: []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	_, _, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err == nil || !strings.Contains(err.Error(), "source policy tenant mismatch") {
		t.Fatalf("get policy err = %v, want tenant mismatch", err)
	}
}

func TestGetSourcePolicyAcceptsRecordWithoutTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putSourcePolicyFixture(ctx, store, "tenant-a", sourcePolicyRecord{
		SourcePolicy: graph.SourcePolicy{
			DefaultPriority: 1,
			Sources:         []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	policy, configured, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if !configured || policy.DefaultPriority != 1 || len(policy.Sources) != 1 || policy.Sources[0].Name != "manual" {
		t.Fatalf("policy configured=%v policy=%#v", configured, policy)
	}
}

func putSourcePolicyFixture(ctx context.Context, store *TenantStore, tenantID string, record sourcePolicyRecord) error {
	data, err := marshalParquetSourcePolicy(ctx, record)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, store.sourcePolicyKey(tenantID), data)
}

func TestIngestUsesSourcePolicyAndSuppressionIsNotFailure(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"owner": "platform"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("manual commit: %v", err)
	}
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "ec2"}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "aws")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(letters) != 0 {
		t.Fatalf("deadletters = %#v", letters)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed.Skipped || replayed.Suppressed != 1 {
		t.Fatalf("replayed = %#v", replayed)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "platform" {
		t.Fatalf("owner = %#v", entity.Fields["owner"])
	}
}

func TestIngestEntitySuppressionReportsItemLocation(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"owner": "platform"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("seed manual entity: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "entity-conflict-location",
		Items: []IngestItem{
			{ExternalID: "host-2", Entity: &graph.Entity{ID: "host:2", Kind: "host", Fields: graph.Fields{"owner": "ec2"}}},
			{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "ec2"}}},
		},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	conflict := result.Conflicts[0]
	if conflict.Index != 1 || conflict.ExternalID != "host-1" || conflict.IncomingID != "host-1" {
		t.Fatalf("conflict location = %#v", conflict)
	}
}

type takeoverDuringSourcePolicyPutStore struct {
	ObjectStore
	base       *MemoryStore
	tenantID   string
	triggerKey string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverDuringSourcePolicyPutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		time.Sleep(time.Millisecond)
		takeover := NewTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.PutSourcePolicy(ctx, s.tenantID, graph.SourcePolicy{
			DefaultPriority: 9,
			Sources:         []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		}); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *takeoverDuringSourcePolicyPutStore) shouldTrigger(key string) bool {
	if key != s.triggerKey {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *takeoverDuringSourcePolicyPutStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}
