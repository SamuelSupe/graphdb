package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestVisitEntitiesTraceReportsPhysicalScanForMissingKind(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	entities := []graph.Entity{
		{ID: "host:a", Kind: "host"},
		{ID: "service:a", Kind: "service"},
		{ID: "database:a", Kind: "database"},
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}

	recorder := installStorageSpanRecorder(t)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	visited := 0
	available, err := lookup.VisitEntities(ctx, "missing-kind", nil, "", func(graph.Entity) (bool, error) {
		visited++
		return true, nil
	})
	if err != nil || !available {
		t.Fatalf("visit entities available=%v err=%v", available, err)
	}
	if visited != 0 {
		t.Fatalf("visited callbacks = %d, want 0", visited)
	}

	span := requireStorageSpan(t, recorder.Ended(), "graphdb.storage.index_lookup.visit_entities")
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.kind", "missing-kind")
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.available", true)
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.physical_entities_examined", int64(len(entities)))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.kind_matched", int64(0))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.candidates_delivered", int64(0))
	if got := storageSpanInt64(t, span, "graphdb.index_lookup.page_specs_visited"); got == 0 {
		t.Fatal("page_specs_visited = 0, want at least one persisted page")
	}
	if got := storageSpanInt64(t, span, "graphdb.index_lookup.parquet_decodes"); got == 0 {
		t.Fatal("parquet_decodes = 0, want at least one decoded page")
	}
}

func installStorageSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func requireStorageSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("span %q not found", name)
	return nil
}

func assertStorageSpanAttribute(t *testing.T, span sdktrace.ReadOnlySpan, key string, want any) {
	t.Helper()
	for _, item := range span.Attributes() {
		if string(item.Key) != key {
			continue
		}
		var got any
		switch item.Value.Type() {
		case attribute.STRING:
			got = item.Value.AsString()
		case attribute.INT64:
			got = item.Value.AsInt64()
		case attribute.BOOL:
			got = item.Value.AsBool()
		default:
			got = item.Value.Emit()
		}
		if got != want {
			t.Fatalf("span %q attribute %q = %#v, want %#v", span.Name(), key, got, want)
		}
		return
	}
	t.Fatalf("span %q missing attribute %q", span.Name(), key)
}

func storageSpanInt64(t *testing.T, span sdktrace.ReadOnlySpan, key string) int64 {
	t.Helper()
	for _, item := range span.Attributes() {
		if string(item.Key) == key {
			return item.Value.AsInt64()
		}
	}
	t.Fatalf("span %q missing attribute %q", span.Name(), key)
	return 0
}
