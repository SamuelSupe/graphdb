package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestDirectCommitIdempotencyReplaysSameRequest(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}

	first, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{IdempotencyKey: "idem-1"})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if first.Version != 1 || first.IdempotentReplay || first.Skipped {
		t.Fatalf("first result = %#v", first)
	}
	recordData, err := objects.Get(ctx, store.commitIdempotencyKey("tenant-a", "idem-1"))
	if err != nil {
		t.Fatalf("get idempotency record: %v", err)
	}
	if !isParquetBytes(recordData) {
		t.Fatal("idempotency record is not parquet")
	}
	record, err := decodeParquetDirectCommitRecord(ctx, recordData)
	if err != nil {
		t.Fatalf("decode idempotency record: %v", err)
	}
	if record.TenantID != "tenant-a" || record.Request.IdempotencyKey != "idem-1" {
		t.Fatalf("idempotency record = %#v", record)
	}
	second, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{IdempotencyKey: " idem-1 "})
	if err != nil {
		t.Fatalf("replay commit: %v", err)
	}
	if second.Version != first.Version || !second.IdempotentReplay || !second.Skipped {
		t.Fatalf("replay result = %#v, first=%#v", second, first)
	}
	items, err := objects.List(ctx, store.commitPrefix("tenant-a"))
	if err != nil {
		t.Fatalf("list commits: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("commit prefix objects = %d, want only immutable commit object: %#v", len(items), items)
	}
}

func TestDirectCommitIdempotencyKeyCacheAvoidsHotMissRead(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "idem-1"}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	objects.Reset()
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "idem-2"}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if got := objects.CountContains("/idempotency/commits/"); got != 0 {
		t.Fatalf("commit idempotency GET count = %d, want 0", got)
	}
}

func TestDirectCommitIdempotencyRejectsDifferentPayload(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "idem-1"}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	_, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "idem-1"})
	if err == nil || !strings.Contains(err.Error(), "idempotency conflict") {
		t.Fatalf("conflict err = %v", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("version = %d, want original commit only", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("conflicting idempotent commit wrote host:b")
	}
}

func TestTenantConfigQuotaOverridesGlobalDefaults(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{})
	one := 1
	retry := int64(1500)
	compactObjects := 10
	compactBytes := int64(2048)
	if _, err := store.PutTenantConfig(ctx, "tenant-a", TenantConfig{
		Backpressure: TenantBackpressureConfig{RetryAfterMS: &retry},
		Quota:        TenantQuotaConfig{MaxEntitiesPerTenant: &one},
		Maintenance:  TenantMaintenanceConfig{CompactObjectCountThreshold: &compactObjects, CompactBytesThreshold: &compactBytes},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	configData, err := store.Objects.Get(ctx, store.tenantConfigKey("tenant-a"))
	if err != nil {
		t.Fatalf("get tenant config object: %v", err)
	}
	if !isParquetBytes(configData) {
		t.Fatal("tenant config object is not parquet")
	}
	configRecord, err := decodeParquetTenantConfig(ctx, configData)
	if err != nil {
		t.Fatalf("decode tenant config object: %v", err)
	}
	if configRecord.TenantID != "tenant-a" || configRecord.Config.Quota.MaxEntitiesPerTenant == nil || configRecord.Config.Maintenance.CompactObjectCountThreshold == nil || configRecord.Config.Maintenance.CompactBytesThreshold == nil {
		t.Fatalf("config record = %#v", configRecord)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant-a: %v", err)
	}
	_, err = store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{})
	var pressure *BackpressureError
	if !errors.As(err, &pressure) {
		t.Fatalf("err = %v, want BackpressureError", err)
	}
	if pressure.RetryAfterMS() != retry {
		t.Fatalf("retry_after_ms = %d, want %d", pressure.RetryAfterMS(), retry)
	}
	assertBackpressureReason(t, err, "tenant_entity_quota_exceeded")

	if _, err := store.Commit(ctx, "tenant-b", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("tenant-b should use global unlimited quota: %v", err)
	}
}

func TestTenantConfigRejectsNegativeNumbers(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	negative := -1
	negative64 := int64(-1)
	if _, err := store.PutTenantConfig(context.Background(), "tenant-a", TenantConfig{
		Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &negative},
	}); err == nil || !strings.Contains(err.Error(), "quota.max_entities_per_tenant") {
		t.Fatalf("err = %v, want negative quota rejection", err)
	}
	if _, err := store.PutTenantConfig(context.Background(), "tenant-a", TenantConfig{
		Maintenance: TenantMaintenanceConfig{DeadLetterMaxAgeSeconds: &negative64},
	}); err == nil || !strings.Contains(err.Error(), "maintenance.deadletter_max_age_seconds") {
		t.Fatalf("err = %v, want negative deadletter retention rejection", err)
	}
	if _, err := store.PutTenantConfig(context.Background(), "tenant-a", TenantConfig{
		Maintenance: TenantMaintenanceConfig{TaskMaxAgeSeconds: &negative64},
	}); err == nil || !strings.Contains(err.Error(), "maintenance.task_max_age_seconds") {
		t.Fatalf("err = %v, want negative task retention rejection", err)
	}
	if _, err := store.PutTenantConfig(context.Background(), "tenant-a", TenantConfig{
		Maintenance: TenantMaintenanceConfig{CompactObjectCountThreshold: &negative},
	}); err == nil || !strings.Contains(err.Error(), "maintenance.compact_object_count_threshold") {
		t.Fatalf("err = %v, want negative compact object threshold rejection", err)
	}
	if _, err := store.PutTenantConfig(context.Background(), "tenant-a", TenantConfig{
		Maintenance: TenantMaintenanceConfig{CompactBytesThreshold: &negative64},
	}); err == nil || !strings.Contains(err.Error(), "maintenance.compact_bytes_threshold") {
		t.Fatalf("err = %v, want negative compact bytes threshold rejection", err)
	}
}
