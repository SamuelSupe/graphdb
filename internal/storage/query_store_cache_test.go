package storage

import (
	"context"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

func TestPostgresSavedQueriesIgnoreStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	cacheConfig := WriterObjectCacheConfig{
		MaxBytes:    1 << 20,
		MaxKeys:     100,
		NegativeTTL: time.Hour,
	}
	first := NewTenantStore(
		NewWriterObjectCache(objects, cacheConfig),
		"test",
	)
	second := NewTenantStore(
		NewWriterObjectCache(objects, cacheConfig),
		"test",
	)
	coordinator := newTaskLeaseTestCoordinator()
	first.SetCoordinator(coordinator)
	second.SetCoordinator(coordinator)

	if _, err := first.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:        "hosts",
		Description: "old",
		Request:     query.Request{Op: "match", Kind: "host"},
	}); err != nil {
		t.Fatalf("save initial query: %v", err)
	}
	if _, err := second.GetSavedQuery(
		ctx,
		"tenant-a",
		"hosts",
	); err != nil {
		t.Fatalf("prime saved query cache: %v", err)
	}
	if _, err := second.ListSavedQueries(ctx, "tenant-a"); err != nil {
		t.Fatalf("prime saved query list cache: %v", err)
	}

	if _, err := first.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:        "hosts",
		Description: "new",
		Request:     query.Request{Op: "match", Kind: "host"},
	}); err != nil {
		t.Fatalf("update query: %v", err)
	}
	if _, err := first.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:    "services",
		Request: query.Request{Op: "match", Kind: "service"},
	}); err != nil {
		t.Fatalf("save second query: %v", err)
	}

	loaded, err := second.GetSavedQuery(ctx, "tenant-a", "hosts")
	if err != nil {
		t.Fatalf("get updated query: %v", err)
	}
	if loaded.Description != "new" {
		t.Fatalf("saved query description = %q, want new", loaded.Description)
	}
	listed, err := second.ListSavedQueries(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("list updated queries: %v", err)
	}
	if len(listed) != 2 ||
		listed[0].Name != "hosts" ||
		listed[1].Name != "services" {
		t.Fatalf("listed queries = %#v", listed)
	}
}
