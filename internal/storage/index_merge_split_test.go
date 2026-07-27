package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIncrementalIndexesTrackMergeAndSplit(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
			},
		}},
		UpsertRelationTypes: []graph.RelationType{{
			Name:        "link",
			FromKind:    "host",
			ToKind:      "host",
			Cardinality: graph.ManyToMany,
		}},
		UpsertEntities: []graph.Entity{
			{
				ID: "host:target", Kind: "host",
				Fields: graph.Fields{"hostname": "target"},
			},
			{
				ID: "host:source", Kind: "host",
				Fields: graph.Fields{"hostname": "source"},
			},
			{
				ID: "host:sink", Kind: "host",
				Fields: graph.Fields{"hostname": "sink"},
			},
		},
		UpsertEdges: []graph.Edge{{
			Type: "link", From: "host:source", To: "host:sink",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}

	if result, err := store.CommitWithReport(
		ctx,
		"tenant-a",
		graph.Mutations{
			MergeEntities: []graph.MergeRequest{{
				TargetID:  "host:target",
				SourceIDs: []string{"host:source"},
			}},
		},
		CommitOptions{},
	); err != nil {
		t.Fatal(err)
	} else if len(result.IndexWarnings) != 0 {
		t.Fatalf("merge index warnings = %#v", result.IndexWarnings)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Version != 2 {
		t.Fatalf("catalog version after merge = %d, want 2", catalog.Version)
	}
	lookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a",
		Version: catalog.Version, Catalog: catalog,
	}
	if _, found, err := lookup.GetEntity(
		ctx, "host:source", nil,
	); err != nil || found {
		t.Fatalf("merged source found=%v err=%v", found, err)
	}
	if _, found, err := lookup.GetEntity(
		ctx, "host:target", nil,
	); err != nil || !found {
		t.Fatalf("merge target found=%v err=%v", found, err)
	}
	if ids, available, err := lookup.MatchFieldIndex(
		ctx, "host", "hostname", []any{"source"},
	); err != nil || !available || len(ids) != 1 ||
		ids[0] != "host:target" {
		t.Fatalf(
			"merged source index ids=%#v available=%v err=%v",
			ids,
			available,
			err,
		)
	}
	edges, available, err := lookup.OutEdges(
		ctx, "host:target", map[string]struct{}{"link": {}},
	)
	if err != nil || !available || len(edges) != 1 ||
		edges[0].To != "host:sink" {
		t.Fatalf(
			"redirected edges=%#v available=%v err=%v",
			edges,
			available,
			err,
		)
	}

	if result, err := store.CommitWithReport(
		ctx,
		"tenant-a",
		graph.Mutations{
			SplitEntities: []graph.SplitRequest{{
				SourceID: "host:target",
				Entities: []graph.Entity{
					{
						ID: "host:left", Kind: "host",
						Fields: graph.Fields{"hostname": "left"},
					},
					{
						ID: "host:right", Kind: "host",
						Fields: graph.Fields{"hostname": "right"},
					},
				},
			}},
		},
		CommitOptions{},
	); err != nil {
		t.Fatal(err)
	} else if len(result.IndexWarnings) != 0 {
		t.Fatalf("split index warnings = %#v", result.IndexWarnings)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Version != 3 {
		t.Fatalf("catalog version after split = %d, want 3", catalog.Version)
	}
	lookup = &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a",
		Version: catalog.Version, Catalog: catalog,
	}
	if _, found, err := lookup.GetEntity(
		ctx, "host:target", nil,
	); err != nil || found {
		t.Fatalf("split source found=%v err=%v", found, err)
	}
	for _, id := range []string{"host:left", "host:right"} {
		if _, found, err := lookup.GetEntity(
			ctx, id, nil,
		); err != nil || !found {
			t.Fatalf("split replacement %q found=%v err=%v", id, found, err)
		}
	}
	for _, hostname := range []string{"left", "right"} {
		ids, available, err := lookup.MatchFieldIndex(
			ctx, "host", "hostname", []any{hostname},
		)
		if err != nil || !available || len(ids) != 1 {
			t.Fatalf(
				"split %q index ids=%#v available=%v err=%v",
				hostname,
				ids,
				available,
				err,
			)
		}
	}
	edges, available, err = lookup.OutEdges(
		ctx, "host:target", map[string]struct{}{"link": {}},
	)
	if err != nil || !available || len(edges) != 0 {
		t.Fatalf(
			"split source edges=%#v available=%v err=%v",
			edges,
			available,
			err,
		)
	}
}
