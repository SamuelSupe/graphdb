package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func TestPersistedFieldVisitorSeeksAndPreservesVersion(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	mutations := graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"state": {Type: "string", Indexed: true}, "score": {Type: "number", Indexed: true}}}}}
	for i := range 320 {
		state := "ready"
		if i%2 == 1 {
			state = "down"
		}
		mutations.UpsertEntities = append(mutations.UpsertEntities, graph.Entity{ID: fmt.Sprintf("host:%05d", i), Kind: "host", Fields: graph.Fields{"state": state, "score": i % 3}})
	}
	if _, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	check := func(l *PersistedIndexLookup, want []string) {
		t.Helper()
		var ids []string
		ok, err := l.VisitFieldIndex(ctx, "host", "state", []any{"ready", "ready"}, "host:00200", func(id string) (bool, error) { ids = append(ids, id); return len(ids) < 4, nil })
		if err != nil || !ok || !reflect.DeepEqual(ids, want) {
			t.Fatalf("page=%v ok=%v err=%v", ids, ok, err)
		}
	}
	check(lookup, []string{"host:00200", "host:00202", "host:00204", "host:00206"})
	var ranged []string
	ok, err := lookup.VisitFilteredFieldIndex(ctx, "host", "score", []query.Filter{{Field: "score", Op: "gte", Value: 1}}, "host:00200", func(id string) (bool, error) { ranged = append(ranged, id); return len(ranged) < 4, nil })
	if err != nil || !ok || !reflect.DeepEqual(ranged, []string{"host:00200", "host:00202", "host:00203", "host:00205"}) {
		t.Fatalf("range=%v ok=%v err=%v", ranged, ok, err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:00200", Kind: "host", Fields: graph.Fields{"state": "down"}}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	next, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	check(&PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: next.Version, Catalog: next}, []string{"host:00202", "host:00204", "host:00206", "host:00208"})
	check(lookup, []string{"host:00200", "host:00202", "host:00204", "host:00206"})
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := lookup.VisitFieldIndex(canceled, "host", "state", []any{"ready"}, "", func(string) (bool, error) { t.Fatal("canceled visitor delivered an ID"); return false, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}
