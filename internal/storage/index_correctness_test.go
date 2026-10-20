package storage

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func TestPersistedFieldIndexMixedValueParity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		op    string
		bound any
	}{
		{"lowercase_gt_uppercase", "apple", "gt", "Z"},
		{"empty_lt_b", "", "lt", "b"},
		{"negative_zero_eq_zero", math.Copysign(0, -1), "eq", 0},
		{"numeric_range_control", float64(10), "gt", 5},
		{"array_range", []any{"a"}, "gt", "A"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newParquetIndexTenantStore(NewMemoryStore(), "test")
			filler := "0"
			if tc.name == "empty_lt_b" {
				filler = "z"
			}
			fieldType := "string"
			if tc.name == "array_range" || tc.name == "negative_zero_eq_zero" || tc.name == "numeric_range_control" {
				fieldType = "any"
			}
			_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
				UpsertCITypes:  []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"value": {Type: fieldType, Indexed: true}}}},
				UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"value": tc.value}}, {ID: "host:other", Kind: "host", Fields: graph.Fields{"value": filler}}},
			}, CommitOptions{})
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := store.RebuildIndexes(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			g, _, err := store.Load(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
			req := query.Request{Op: "match", Where: []query.Filter{{Field: "value", Op: tc.op, Value: tc.bound}}, Sort: []query.SortSpec{{Field: "id"}}, Limit: 100, Profile: true}
			scan, err := query.ExecuteContext(ctx, g, req)
			if err != nil {
				t.Fatal(err)
			}
			req.Kind = "host"
			lazy := graph.New()
			lazy.Version = catalog.Version
			indexed, err := query.ExecuteContextWithOptions(ctx, lazy, req, query.ExecuteOptions{IndexLookup: lookup, EntityLookup: lookup, PlannerStats: catalog.PlannerStats()})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("full scan=%d persisted index=%d plan=%+v", len(scan.Results), len(indexed.Results), indexed.Plan)
			if !reflect.DeepEqual(queryEntityIDs(scan.Results), queryEntityIDs(indexed.Results)) {
				t.Errorf("query results differ")
			}
		})
	}
}

func TestLegacyDefaultPageCursorAdvances(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	page := EntityPageData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: "tenant-a", Version: 1, Shard: "default",
		Entities: []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}, {ID: "host:c", Kind: "host"}}}
	meta, err := store.putParquetEntityPage(ctx, "tenant-a", page)
	if err != nil {
		t.Fatal(err)
	}
	spec := EntityPageSpec{Shard: "default", EntityCount: 3, Format: IndexFormatParquet, ContentHash: entityPageContentHash(page),
		Objects: []IndexObject{{Role: "page", Key: meta.Key, ContentHash: entityPageContentHash(page)}}}
	catalog := IndexCatalog{TenantID: "tenant-a", Version: 1, EntityPages: []EntityPageSpec{spec}}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: 1, Catalog: catalog}
	lazy := graph.New()
	lazy.Version = 1
	options := query.ExecuteOptions{PlannerStats: catalog.PlannerStats(), IndexLookup: lookup, EntityLookup: lookup}
	request := query.Request{Op: "match", Kind: "host", Limit: 1}
	first, err := query.ExecuteContextWithOptions(ctx, lazy, request, options)
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = first.NextCursor
	second, err := query.ExecuteContextWithOptions(ctx, lazy, request, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("first=%v second=%v firstCursor=%q secondCursor=%q", queryEntityIDs(first.Results), queryEntityIDs(second.Results), first.NextCursor, second.NextCursor)
	if len(first.Results) > 0 && len(second.Results) > 0 && first.Results[0].Entity.ID == second.Results[0].Entity.ID {
		t.Error("legacy default catalog repeats first page")
	}
	scanOptions := EntityScanOptions{Kind: "host", Limit: 1}
	scanFirst, ok, err := store.listEntitiesFromPages(ctx, "tenant-a", 1, catalog, scanOptions, scanCursor{}, nil)
	if err != nil || !ok {
		t.Fatalf("first scan: available=%v error=%v", ok, err)
	}
	cursor, err := parseScanCursor(scanFirst.NextCursor, 1, entityScanQueryHash(scanOptions))
	if err != nil {
		t.Fatal(err)
	}
	scanSecond, ok, err := store.listEntitiesFromPages(ctx, "tenant-a", 1, catalog, scanOptions, cursor, nil)
	if err != nil || !ok {
		t.Fatalf("second scan: available=%v error=%v", ok, err)
	}
	t.Logf("entity list: first=%s second=%s", scanFirst.Entities[0].ID, scanSecond.Entities[0].ID)
	if scanFirst.Entities[0].ID == scanSecond.Entities[0].ID {
		t.Error("legacy default entity list repeats first page")
	}
}

func TestOrderedRebuildPreservesNewerCatalog(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := newParquetIndexTenantStore(files, "test")
	store.WriteEntityRecords = false
	if _, err = store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	oldGraph, oldManifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host", Fields: graph.Fields{"hostname": "new"}}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	before, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = store.acquireAndBindWriterFence(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx = context.WithValue(ctx, orderedIndexUpdateKey{}, true)
	_, err = store.rebuildIndexesFromGraph(ctx, "tenant-a", oldGraph, oldManifest)
	after, ae := store.GetIndexCatalog(ctx, "tenant-a")
	t.Logf("catalog before=%d after=%d rebuildErr=%v catalogErr=%v", before.Version, after.Version, err, ae)
	if err == nil && after.Version < before.Version {
		t.Error("ordered rebuild overwrote a newer published catalog")
	}
}

func TestSplitFieldEqualityIncludesLongerPrefix(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	entities := []graph.Entity{{ID: "host:target", Kind: "host", Fields: graph.Fields{"name": "server10"}}}
	for i := 0; i < 2050; i++ {
		entities = append(entities, graph.Entity{ID: fmt.Sprintf("host:%d", i), Kind: "host", Fields: graph.Fields{"name": "server1"}})
	}
	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes:  []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Indexed: true}}}},
		UpsertEntities: entities,
	}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "name", []any{"server10"})
	t.Logf("ids=%v available=%v err=%v objects=%+v", ids, ok, err, catalog.Indexes[0].Objects)
	if err != nil || !ok || len(ids) != 1 {
		t.Errorf("server10 lookup lost existing entity")
	}
}
