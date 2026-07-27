package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

func TestPersistedMatchCursorKeepsOrderDuringMaterializedFallback(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	entities := make([]graph.Entity, 128)
	for i := range entities {
		entities[i] = graph.Entity{
			ID:     fmt.Sprintf("host:%03d", i),
			Kind:   "host",
			Fields: graph.Fields{"state": "ready"},
		}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes:  []graph.CIType{{Name: "host"}},
		UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	lookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a",
		Version: catalog.Version, Catalog: catalog,
	}
	options := query.ExecuteOptions{
		PlannerStats: catalog.PlannerStats(),
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
	lazyGraph := graph.New()
	lazyGraph.Version = catalog.Version
	request := query.Request{Op: "match", Kind: "host", Limit: 7}
	first, err := query.ExecuteContextWithOptions(ctx, lazyGraph, request, options)
	if err != nil {
		t.Fatalf("first lazy page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("first lazy page did not return a cursor")
	}

	request.Cursor = first.NextCursor
	want, err := query.ExecuteContextWithOptions(ctx, lazyGraph, request, options)
	if err != nil {
		t.Fatalf("second lazy page: %v", err)
	}
	fullGraph, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load materialized graph: %v", err)
	}
	got, err := query.ExecuteContextWithOptions(
		ctx, fullGraph, request, query.ExecuteOptions{},
	)
	if err != nil {
		t.Fatalf("materialized fallback page: %v", err)
	}
	if wantIDs, gotIDs := queryEntityIDs(want.Results), queryEntityIDs(got.Results); !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("fallback page ids = %v, want persisted continuation %v", gotIDs, wantIDs)
	}

	request.Cursor = ""
	materializedFirst, err := query.ExecuteContextWithOptions(
		ctx, fullGraph, request, query.ExecuteOptions{},
	)
	if err != nil {
		t.Fatalf("first materialized page: %v", err)
	}
	request.Cursor = materializedFirst.NextCursor
	if _, err := query.ExecuteContextWithOptions(
		ctx, lazyGraph, request, options,
	); !errors.Is(err, query.ErrIndexUnavailable) {
		t.Fatalf("identity cursor on shard scan error = %v, want index fallback", err)
	}
}

func queryEntityIDs(results []query.Result) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		if result.Entity != nil {
			ids = append(ids, result.Entity.ID)
		}
	}
	return ids
}
