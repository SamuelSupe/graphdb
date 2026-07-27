package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestListEntitiesUsesPagesWithFiltersAndCursor(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedScanTenant(t, ctx, store)
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}

	first, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Kind: "host", Limit: 1})
	if err != nil {
		t.Fatalf("list first page: %v", err)
	}
	if !first.IndexedRead || len(first.Entities) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %#v, want one indexed result and cursor", first)
	}
	second, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Kind: "host", Limit: 1, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("list second page: %v", err)
	}
	if !second.IndexedRead || len(second.Entities) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %#v, want final indexed result", second)
	}
	seen := map[string]struct{}{first.Entities[0].ID: {}, second.Entities[0].ID: {}}
	if _, ok := seen["host:a"]; !ok {
		t.Fatalf("paged hosts = %#v, missing host:a", seen)
	}
	if _, ok := seen["host:b"]; !ok {
		t.Fatalf("paged hosts = %#v, missing host:b", seen)
	}

	manual, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Source: "manual"})
	if err != nil {
		t.Fatalf("list manual source: %v", err)
	}
	if len(manual.Entities) != 1 || manual.Entities[0].ID != "host:b" {
		t.Fatalf("manual source entities = %#v", manual.Entities)
	}
	shard, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Shard: entityShardID("host:a")})
	if err != nil {
		t.Fatalf("list shard: %v", err)
	}
	for _, entity := range shard.Entities {
		if entityShardID(entity.ID) != entityShardID("host:a") {
			t.Fatalf("entity %q outside requested shard", entity.ID)
		}
	}
}

func TestListEntitiesCursorUsesPinnedCatalogAfterManifestAdvance(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedScanTenant(t, ctx, store)
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	first, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Kind: "host", Limit: 1})
	if err != nil {
		t.Fatalf("list first page: %v", err)
	}
	if first.Version != catalog.Version || first.NextCursor == "" {
		t.Fatalf("first page = %#v, want cursor pinned to catalog version %d", first, catalog.Version)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host", Fields: graph.Fields{"hostname": "app-c"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}
	second, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Kind: "host", Limit: 1, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("list second page after manifest advance: %v", err)
	}
	if second.Version != catalog.Version || len(second.Entities) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %#v, want stable final page at version %d", second, catalog.Version)
	}
	if second.Entities[0].ID == "host:c" {
		t.Fatalf("second page included entity from later manifest: %#v", second.Entities)
	}
}

func TestListEntitiesCursorSurvivesSameVersionCatalogGC(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedScanTenant(t, ctx, store)
	previous, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	first, err := store.ListEntities(
		ctx,
		"tenant-a",
		EntityScanOptions{Kind: "host", Limit: 1},
	)
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page = %#v, err %v", first, err)
	}
	previousHash := scanCatalogContentHash(previous)
	previousKey := store.indexCatalogVersionHashKey(
		"tenant-a", previous.Version, previousHash,
	)

	current, meta, err := store.getIndexCatalogWithMeta(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get current catalog: %v", err)
	}
	current.Indexes = append(current.Indexes, IndexSpec{
		Name:   "host.hostname",
		Kind:   "host",
		Field:  "hostname",
		Type:   "string",
		Status: "ready",
	})
	if _, err := store.putIndexCatalogWithMeta(
		ctx, "tenant-a", current, meta,
	); err != nil {
		t.Fatalf("publish same-version catalog: %v", err)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{
		KeepSnapshots:       1,
		CleanupIndexOrphans: true,
	}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if _, err := store.Objects.Get(ctx, previousKey); err != nil {
		t.Fatalf("pinned catalog removed by GC: %v", err)
	}

	second, err := store.ListEntities(
		ctx,
		"tenant-a",
		EntityScanOptions{
			Kind: "host", Limit: 1, Cursor: first.NextCursor,
		},
	)
	if err != nil {
		t.Fatalf("list second page after same-version GC: %v", err)
	}
	if second.Version != previous.Version ||
		len(second.Entities) != 1 ||
		second.NextCursor != "" {
		t.Fatalf(
			"second page = %#v, want stable final page at version %d",
			second, previous.Version,
		)
	}
}

func TestListEdgesUsesShardsWithFilters(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedScanTenant(t, ctx, store)
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	result, err := store.ListEdges(ctx, "tenant-a", EdgeScanOptions{
		Type:      "runs_on",
		FromShard: edgeShardID("service:api"),
		Source:    "agent",
	})
	if err != nil {
		t.Fatalf("list edges: %v", err)
	}
	if !result.IndexedRead || len(result.Edges) != 1 || result.Edges[0].Type != "runs_on" || result.Edges[0].From != "service:api" {
		t.Fatalf("edge result = %#v", result)
	}
}

func TestListEdgesCursorUsesPinnedCatalogAfterManifestAdvance(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedScanTenant(t, ctx, store)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-2",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:b",
			Source:     "agent",
			ExternalID: "edge-2",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed second edge: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	first, err := store.ListEdges(ctx, "tenant-a", EdgeScanOptions{Type: "runs_on", Limit: 1})
	if err != nil {
		t.Fatalf("list first edge page: %v", err)
	}
	if first.Version != catalog.Version || len(first.Edges) != 1 || first.NextCursor == "" {
		t.Fatalf("first edge page = %#v, want cursor pinned to catalog version %d", first, catalog.Version)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host", Fields: graph.Fields{"hostname": "app-c"}}},
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-3",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:c",
			Source:     "agent",
			ExternalID: "edge-3",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}
	second, err := store.ListEdges(ctx, "tenant-a", EdgeScanOptions{Type: "runs_on", Limit: 1, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("list second edge page after manifest advance: %v", err)
	}
	if second.Version != catalog.Version || len(second.Edges) != 1 || second.NextCursor != "" {
		t.Fatalf("second edge page = %#v, want stable final page at version %d", second, catalog.Version)
	}
	if second.Edges[0].To == "host:c" {
		t.Fatalf("second edge page included edge from later manifest: %#v", second.Edges)
	}
}

func TestListScansUseReadableStaleCatalogInsteadOfLoadingLatestGraph(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedScanTenant(t, ctx, store)
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name:     "depends_on",
			FromKind: "service",
			ToKind:   "service",
			Directed: true,
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("stale commit: %v", err)
	}
	entities, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Kind: "host", Limit: 10})
	if err != nil {
		t.Fatalf("list entities: %v", err)
	}
	if !entities.IndexedRead || entities.Version != catalog.Version {
		t.Fatalf("entities scan = %#v, want stale indexed catalog version %d", entities, catalog.Version)
	}
	if len(entities.Entities) != 2 {
		t.Fatalf("entities = %#v, want catalog snapshot hosts", entities.Entities)
	}
	edges, err := store.ListEdges(ctx, "tenant-a", EdgeScanOptions{Type: "runs_on", Limit: 10})
	if err != nil {
		t.Fatalf("list edges: %v", err)
	}
	if !edges.IndexedRead || edges.Version != catalog.Version || len(edges.Edges) != 1 {
		t.Fatalf("edges scan = %#v, want only catalog snapshot edge", edges)
	}
}

func TestScanCursorRejectsDifferentFilter(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedScanTenant(t, ctx, store)
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	first, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Limit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatalf("first page has no cursor: %#v", first)
	}
	if _, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Kind: "host", Cursor: first.NextCursor}); err == nil {
		t.Fatal("expected cursor query mismatch")
	}
}

func seedScanTenant(t *testing.T, ctx context.Context, store *TenantStore) {
	t.Helper()
	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name:     "runs_on",
			FromKind: "service",
			ToKind:   "host",
			Directed: true,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host", Source: "aws", ExternalID: "i-a", Fields: graph.Fields{"hostname": "app-a"}},
			{ID: "host:b", Kind: "host", Source: "manual", ExternalID: "host-b", Fields: graph.Fields{"hostname": "app-b"}},
			{ID: "service:api", Kind: "service", Source: "agent", ExternalID: "svc-api", Fields: graph.Fields{"name": "api"}},
		},
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-1",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:a",
			Source:     "agent",
			ExternalID: "edge-1",
		}},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("seed commit: %v", err)
	}
}
