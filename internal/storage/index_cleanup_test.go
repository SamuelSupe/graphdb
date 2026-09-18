package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestRebuildIndexesDefersOrphanIndexObjectCleanupToGC(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	initialCatalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	initialKeys := indexObjectKeys(store, "tenant-a", initialCatalog)
	for key := range initialKeys {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("expected initial index object %s: %v", key, err)
		}
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string"}},
		}},
		DeleteEntities: []string{"host:app-01"},
	}, CommitOptions{}); err != nil {
		t.Fatalf("remove indexed entity: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("repair rebuild: %v", err)
	}
	for key := range initialKeys {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("orphan index object %s should remain until GC: %v", key, err)
		}
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	for key := range initialKeys {
		if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GC orphan index object %s err=%v, want ErrNotFound", key, err)
		}
	}
}

func TestRebuildIndexesDefersOrphanCleanupWhenCatalogMissing(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	initialCatalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	initialKeys := indexObjectKeys(store, "tenant-a", initialCatalog)
	for key := range initialKeys {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("expected initial index object %s: %v", key, err)
		}
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string"}},
		}},
		DeleteEntities: []string{"host:app-01"},
	}, CommitOptions{}); err != nil {
		t.Fatalf("remove indexed entity: %v", err)
	}
	if err := store.Objects.Delete(ctx, store.indexCatalogKey("tenant-a")); err != nil {
		t.Fatalf("delete catalog: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("repair rebuild: %v", err)
	}
	for key := range initialKeys {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("orphan index object %s should remain until GC: %v", key, err)
		}
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	for key := range initialKeys {
		if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GC orphan index object %s err=%v, want ErrNotFound", key, err)
		}
	}
}

func TestIncrementalIndexesDeferObsoleteEntityPagesAndEdgeShardsToGC(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	initialCatalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	edgeKey := requireIndexObjectKey(t, requireEdgeShardSpec(t, initialCatalog, "runs_on", edgeShardID("service:api")).Objects, "shard")
	pageKey := requireIndexObjectKey(t, requireEntityPageSpec(t, initialCatalog, entityShardID("host:app-01")).Objects, "page")
	for _, key := range []string{
		requireAnyIndexObjectKey(t, requireFieldIndexSpec(t, initialCatalog, "host", "hostname").Objects),
		edgeKey,
		pageKey,
	} {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("expected initial index object %s: %v", key, err)
		}
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		DeleteEntities: []string{"host:app-01"},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("delete entity: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	for _, key := range []string{edgeKey, pageKey} {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("obsolete index object %s should remain until GC: %v", key, err)
		}
	}
	currentCatalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current catalog: %v", err)
	}
	index, _ := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", currentCatalog, "host", "hostname")
	if len(index.Values) != 0 {
		t.Fatalf("field index values = %#v, want empty", index.Values)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	for _, key := range []string{edgeKey, pageKey} {
		if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GC obsolete index object %s err=%v, want ErrNotFound", key, err)
		}
	}
}

func TestIncrementalCleanupDefersObsoleteObjectsWhenConditionalDeleteUnsupported(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	initialCatalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	edgeKey := requireIndexObjectKey(t, requireEdgeShardSpec(t, initialCatalog, "runs_on", edgeShardID("service:api")).Objects, "shard")
	pageKey := requireIndexObjectKey(t, requireEntityPageSpec(t, initialCatalog, entityShardID("host:app-01")).Objects, "page")
	store.Objects = unsupportedConditionalDeleteStore{ObjectStore: base}

	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		DeleteEntities: []string{"host:app-01"},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("delete entity: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	for _, key := range []string{edgeKey, pageKey} {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("obsolete index object %s should remain until GC: %v", key, err)
		}
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "ready" {
		t.Fatalf("health = %#v, want ready", health)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	for _, key := range []string{edgeKey, pageKey} {
		if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GC obsolete index object %s err=%v, want ErrNotFound", key, err)
		}
	}
}

func TestCleanupRemovesVersionedParquetObjectsWhenConditionalDeleteUnsupported(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	seedIndexedGraph(t, ctx, store)
	previous, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("initial rebuild parquet: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-03", Kind: "host", Fields: graph.Fields{"hostname": "app-03", "region": "us"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("advance graph: %v", err)
	}
	current, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current rebuild parquet: %v", err)
	}
	oldKeys := indexObjectKeys(store, "tenant-a", previous)
	currentKeys := indexObjectKeys(store, "tenant-a", current)
	store.Objects = unsupportedConditionalDeleteStore{ObjectStore: base}
	if err := store.cleanupObsoleteIndexObjects(ctx, "tenant-a", IndexCatalog{}, current); err != nil {
		t.Fatalf("cleanup obsolete parquet objects: %v", err)
	}
	for key := range oldKeys {
		if _, ok := currentKeys[key]; ok {
			continue
		}
		if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("old versioned parquet object %s err=%v, want ErrNotFound", key, err)
		}
	}
	for key := range currentKeys {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("current parquet object %s should remain: %v", key, err)
		}
	}
}

func TestIncrementalCleanupSkipsChangedRemovedObject(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	shardID := edgeShardID("service:api")
	key := store.parquetEdgeShardVersionKey("tenant-a", 1, "runs_on", shardID)
	previousShard := EdgeShardData{
		TenantID:     "tenant-a",
		RelationType: "runs_on",
		Shard:        shardID,
		Version:      1,
		Edges: []graph.Edge{{
			ID:   graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:old"),
			Type: "runs_on",
			From: "service:api",
			To:   "host:old",
		}},
	}
	previousCatalog := IndexCatalog{Version: 1, EdgeShards: []EdgeShard{{
		RelationType: "runs_on",
		Shard:        shardID,
		EdgeCount:    1,
		ContentHash:  edgeShardContentHash(previousShard),
	}}}
	changedShard := EdgeShardData{
		TenantID:     "tenant-a",
		RelationType: "runs_on",
		Shard:        shardID,
		Version:      3,
		Edges: []graph.Edge{{
			ID:   graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:new"),
			Type: "runs_on",
			From: "service:api",
			To:   "host:new",
		}},
	}
	writeParquetEdgeShardForTest(t, ctx, store, key, changedShard)
	if err := store.cleanupCatalogObjectsRemovedFromCurrent(ctx, "tenant-a", previousCatalog, IndexCatalog{Version: 2}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	data, err := store.Objects.Get(ctx, key)
	if err != nil {
		t.Fatalf("changed shard should remain: %v", err)
	}
	got, err := decodeParquetEdgeShard(ctx, data, "tenant-a", "runs_on", shardID, 1)
	if err != nil {
		t.Fatalf("decode changed shard: %v", err)
	}
	if len(got.Edges) != 1 || got.Edges[0].To != "host:new" {
		t.Fatalf("changed shard = %#v, want preserved", got)
	}
}

func TestRebuildCleanupDeletesParquetListedOrphanObject(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	shardID := edgeShardID("service:api")
	key := store.parquetEdgeShardVersionKey("tenant-a", 1, "runs_on", shardID)
	orphanShard := EdgeShardData{
		TenantID:     "tenant-a",
		RelationType: "runs_on",
		Shard:        shardID,
		Version:      1,
		Edges: []graph.Edge{{
			ID:   graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:new"),
			Type: "runs_on",
			From: "service:api",
			To:   "host:new",
		}},
	}
	writeParquetEdgeShardForTest(t, ctx, store, key, orphanShard)
	if err := store.cleanupObsoleteIndexObjects(ctx, "tenant-a", IndexCatalog{}, IndexCatalog{Version: 2}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("parquet orphan shard err=%v, want deleted", err)
	}
}

func TestListedIndexCleanupPreservesUnsafeObjects(t *testing.T) {
	for _, test := range []struct {
		name    string
		tenant  string
		version int64
		corrupt bool
	}{
		{name: "current content", tenant: "tenant-a", version: 2},
		{name: "future content", tenant: "tenant-a", version: 3},
		{name: "other tenant", tenant: "tenant-b", version: 1},
		{name: "corrupt content", tenant: "tenant-a", version: 1, corrupt: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newParquetIndexTenantStore(NewMemoryStore(), "test")
			key := store.parquetEntityPageVersionKey("tenant-a", 1, "00")
			data, err := marshalParquetEntityPage(ctx, EntityPageData{
				TenantID: test.tenant, Shard: "00", Version: test.version,
				Entities: []graph.Entity{{ID: "host:old", Kind: "host"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.corrupt {
				data = data[:len(data)/2]
			}
			if err := store.Objects.Put(ctx, key, data); err != nil {
				t.Fatal(err)
			}
			if err := store.cleanupObsoleteIndexObjects(ctx, "tenant-a", IndexCatalog{}, IndexCatalog{Version: 2}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Objects.Get(ctx, key); err != nil {
				t.Fatalf("unsafe orphan should remain: %v", err)
			}
		})
	}
}

func BenchmarkCleanupListedEntityPage10K(b *testing.B) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "bench")
	key := store.parquetEntityPageVersionKey("tenant-a", 1, "00")
	page := EntityPageData{TenantID: "tenant-a", Shard: "00", Version: 1}
	for i := 0; i < 10_000; i++ {
		page.Entities = append(page.Entities, graph.Entity{
			ID: fmt.Sprintf("host:%05d", i), Kind: "host",
			Fields: graph.Fields{"hostname": fmt.Sprintf("host-%05d", i), "region": "ap-southeast-1", "state": "ready", "cpu": 8},
		})
	}
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := store.Objects.Put(ctx, key, data); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := store.deleteListedObsoleteIndexObjectIfSafe(ctx, "tenant-a", key, 2); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			b.Fatalf("obsolete entity page was not deleted: %v", err)
		}
		b.StartTimer()
	}
}

func TestIncrementalCleanupSkipsTenantMismatchedRemovedObject(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	shardID := edgeShardID("service:api")
	key := store.parquetEdgeShardVersionKey("tenant-a", 1, "runs_on", shardID)
	shard := EdgeShardData{
		TenantID:     "tenant-b",
		RelationType: "runs_on",
		Shard:        shardID,
		Version:      1,
		Edges: []graph.Edge{{
			ID:   graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:old"),
			Type: "runs_on",
			From: "service:api",
			To:   "host:old",
		}},
	}
	previousCatalog := IndexCatalog{Version: 1, EdgeShards: []EdgeShard{{
		RelationType: "runs_on",
		Shard:        shardID,
		EdgeCount:    1,
		ContentHash:  edgeShardContentHash(shard),
	}}}
	writeParquetEdgeShardForTest(t, ctx, store, key, shard)
	if err := store.cleanupCatalogObjectsRemovedFromCurrent(ctx, "tenant-a", previousCatalog, IndexCatalog{Version: 2}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	data, err := store.Objects.Get(ctx, key)
	if err != nil {
		t.Fatalf("tenant-mismatched shard should remain: %v", err)
	}
	got, err := decodeParquetEdgeShard(ctx, data, "tenant-b", "runs_on", shardID, 1)
	if err != nil {
		t.Fatalf("decode tenant-mismatched shard: %v", err)
	}
	if got.TenantID != "tenant-b" {
		t.Fatalf("tenant-mismatched shard = %#v, want preserved", got)
	}
}

type noListObjectStore struct {
	ObjectStore
}

func (s noListObjectStore) List(context.Context, string) ([]ObjectInfo, error) {
	return nil, errors.New("list should not be used")
}

type unsupportedConditionalDeleteStore struct {
	ObjectStore
}

func (s unsupportedConditionalDeleteStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	if condition.IfMatch != "" {
		return fmt.Errorf("%w: %w", ErrConflict, ErrConditionalDeleteUnsupported)
	}
	return s.ObjectStore.DeleteConditional(ctx, key, condition)
}
