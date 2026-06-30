package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestTenantUsageReportsObjectCategoriesAndRetention(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	keepSnapshots := 3
	deadLetterAge := int64(3600)
	taskAge := int64(7200)
	if _, err := store.PutTenantConfig(ctx, "tenant-a", TenantConfig{Maintenance: TenantMaintenanceConfig{
		KeepSnapshots:           &keepSnapshots,
		DeadLetterMaxAgeSeconds: &deadLetterAge,
		TaskMaxAgeSeconds:       &taskAge,
	}}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", ReaderHeartbeat{ReaderID: "reader-a", Status: "fresh", VisibleVersion: 1, ManifestVersion: 1, Consistent: true}); err != nil {
		t.Fatalf("put reader heartbeat: %v", err)
	}
	heartbeatKey := store.readerHeartbeatKey("tenant-a", "reader-a")
	heartbeatData, err := store.Objects.Get(ctx, heartbeatKey)
	if err != nil {
		t.Fatalf("get reader heartbeat: %v", err)
	}
	if !isParquetBytes(heartbeatData) {
		t.Fatalf("reader heartbeat key=%q is not parquet", heartbeatKey)
	}
	heartbeat, err := decodeParquetReaderHeartbeat(ctx, heartbeatData)
	if err != nil {
		t.Fatalf("decode reader heartbeat: %v", err)
	}
	if heartbeat.ReaderID != "reader-a" || heartbeat.TenantID != "tenant-a" {
		t.Fatalf("reader heartbeat = %#v", heartbeat)
	}

	report, err := store.TenantUsage(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("usage: %v", err)
	}
	if report.ObjectCount == 0 || report.TotalBytes == 0 || report.ManifestVersion != 1 || report.CommitTailLength != 1 {
		t.Fatalf("usage report = %#v", report)
	}
	if report.Retention.KeepSnapshots != keepSnapshots || report.Retention.DeadLetterMaxAgeSeconds != deadLetterAge || report.Retention.TaskMaxAgeSeconds != taskAge {
		t.Fatalf("retention = %#v", report.Retention)
	}
	categories := usageCategoryMap(report.Categories)
	for _, name := range []string{"manifest", "commits", "config", "reader_heartbeats"} {
		if categories[name].ObjectCount == 0 {
			t.Fatalf("category %q missing from %#v", name, report.Categories)
		}
	}
}

func usageCategoryMap(items []TenantUsageCategory) map[string]TenantUsageCategory {
	result := make(map[string]TenantUsageCategory, len(items))
	for _, item := range items {
		result[item.Name] = item
	}
	return result
}
