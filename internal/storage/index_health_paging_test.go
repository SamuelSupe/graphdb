package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIndexHealthUsesBoundedObjectPages(t *testing.T) {
	ctx := context.Background()
	paged := &pagingOnlyStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(paged, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:app", Kind: "host",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if _, err := store.IndexHealth(ctx, "tenant-a"); err != nil {
		t.Fatalf("index health: %v", err)
	}
	if paged.listCalls != 0 {
		t.Fatalf("unbounded list calls=%d, want 0", paged.listCalls)
	}
	if paged.pageCalls == 0 {
		t.Fatal("index health did not use bounded object pages")
	}
}
