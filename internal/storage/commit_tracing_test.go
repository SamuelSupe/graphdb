package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestCommitTracesPreflightAndCriticalSection(t *testing.T) {
	recorder := installStorageSpanRecorder(t)
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{})
	result, err := store.CommitWithReport(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "idem-tracing"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	spans := recorder.Ended()
	preflight := requireStorageSpan(t, spans, "graphdb.storage.commit.preflight")
	assertStorageSpanAttribute(t, preflight, "graphdb.commit.write_backpressure_reused", false)
	critical := requireStorageSpan(t, spans, "graphdb.storage.commit.critical_section")
	assertStorageSpanAttribute(t, critical, "graphdb.commit.version", result.Version)
	if held := storageSpanInt64(t, critical, "graphdb.commit.lock_held_ms"); held < 0 {
		t.Fatalf("lock held duration = %dms", held)
	}
	complete := requireStorageSpan(t, spans, "graphdb.storage.commit.complete_idempotency_record")
	assertStorageSpanAttribute(t, complete, "graphdb.commit.outside_tenant_lock", true)
}
