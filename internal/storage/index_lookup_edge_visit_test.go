package storage

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPersistedIndexLookupVisitsSortedOutEdgeRange(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	entities := []graph.Entity{{
		ID: "service:range", Kind: "service",
	}}
	edges := make([]graph.Edge, 0, 20)
	for index := 0; index < 20; index++ {
		hostID := fmt.Sprintf("host:%02d", index)
		entities = append(entities, graph.Entity{ID: hostID, Kind: "host"})
		relationType := "calls"
		if index%2 == 0 {
			relationType = "runs_on"
		}
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:%02d", index),
			Type: relationType,
			From: "service:range",
			To:   hostID,
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "calls", FromKind: "service", ToKind: "host"},
			{Name: "runs_on", FromKind: "service", ToKind: "host"},
		},
		UpsertEntities: entities,
		UpsertEdges:    edges,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph: %v", err)
	}
	edgeIDs := make([]string, 0, len(g.Edges))
	for _, edge := range g.Edges {
		if edge.From == "service:range" {
			edgeIDs = append(edgeIDs, edge.ID)
		}
	}
	sort.Strings(edgeIDs)
	if len(edgeIDs) != len(edges) {
		t.Fatalf("loaded edge IDs=%d, want %d", len(edgeIDs), len(edges))
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	lookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a",
		Version: catalog.Version, Catalog: catalog,
	}
	var visited []string
	ok, err := lookup.VisitOutEdges(
		ctx,
		"service:range",
		nil,
		edgeIDs[10],
		func(edge graph.Edge) (bool, error) {
			visited = append(visited, edge.ID)
			return len(visited) < 3, nil
		},
	)
	if err != nil || !ok {
		t.Fatalf("visit ok=%v err=%v", ok, err)
	}
	want := edgeIDs[10:13]
	if fmt.Sprint(visited) != fmt.Sprint(want) {
		t.Fatalf("visited=%v, want %v", visited, want)
	}
}

func TestPersistedIndexLookupVisitsSortedInEdgeRange(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	entities := []graph.Entity{{
		ID: "host:range", Kind: "host",
	}}
	edges := make([]graph.Edge, 0, 20)
	for index := 0; index < 20; index++ {
		serviceID := fmt.Sprintf("service:%02d", index)
		entities = append(entities, graph.Entity{
			ID: serviceID, Kind: "service",
		})
		relationType := "calls"
		if index%2 == 0 {
			relationType = "runs_on"
		}
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:%02d", index),
			Type: relationType,
			From: serviceID,
			To:   "host:range",
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "calls", FromKind: "service", ToKind: "host"},
			{Name: "runs_on", FromKind: "service", ToKind: "host"},
		},
		UpsertEntities: entities,
		UpsertEdges:    edges,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph: %v", err)
	}
	edgeIDs := make([]string, 0, len(g.Edges))
	for _, edge := range g.Edges {
		if edge.To == "host:range" {
			edgeIDs = append(edgeIDs, edge.ID)
		}
	}
	sort.Strings(edgeIDs)
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	reverse, err := store.GetReverseIndexCatalog(
		ctx, "tenant-a", catalog.Version,
	)
	if err != nil {
		t.Fatalf("load reverse catalog: %v", err)
	}
	lookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a",
		Version: catalog.Version, Catalog: catalog,
		ReverseCatalog: &reverse,
	}
	var visited []string
	ok, err := lookup.VisitInEdges(
		ctx,
		"host:range",
		nil,
		edgeIDs[10],
		func(edge graph.Edge) (bool, error) {
			visited = append(visited, edge.ID)
			return len(visited) < 3, nil
		},
	)
	if err != nil || !ok {
		t.Fatalf("visit ok=%v err=%v", ok, err)
	}
	want := edgeIDs[10:13]
	if fmt.Sprint(visited) != fmt.Sprint(want) {
		t.Fatalf("visited=%v, want %v", visited, want)
	}
}

func TestPersistedIndexLookupMergesBothEdgeDirections(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "links", FromKind: "node", ToKind: "node",
		}},
		UpsertEntities: []graph.Entity{
			{ID: "node:start", Kind: "node"},
			{ID: "node:in", Kind: "node"},
			{ID: "node:out", Kind: "node"},
		},
		UpsertEdges: []graph.Edge{
			{
				ID: "edge:01", Type: "links",
				From: "node:in", To: "node:start",
			},
			{
				ID: "edge:02", Type: "links",
				From: "node:start", To: "node:out",
			},
			{
				ID: "edge:03", Type: "links",
				From: "node:start", To: "node:start",
			},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph: %v", err)
	}
	neighbors, err := g.FilteredNeighbors(
		"node:start", "both", nil, nil, false, nil,
	)
	if err != nil {
		t.Fatalf("load expected neighbors: %v", err)
	}
	want := make([]string, 0, len(neighbors))
	for _, neighbor := range neighbors {
		want = append(
			want, neighbor.Direction+":"+neighbor.Edge.ID,
		)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	reverse, err := store.GetReverseIndexCatalog(
		ctx, "tenant-a", catalog.Version,
	)
	if err != nil {
		t.Fatalf("load reverse catalog: %v", err)
	}
	lookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a",
		Version: catalog.Version, Catalog: catalog,
		ReverseCatalog: &reverse,
	}
	var visited []string
	ok, err := lookup.VisitBothEdges(
		ctx,
		"node:start",
		nil,
		"",
		func(edge graph.Edge, direction string) (bool, error) {
			visited = append(
				visited, direction+":"+edge.ID,
			)
			return true, nil
		},
	)
	if err != nil || !ok {
		t.Fatalf("visit ok=%v err=%v", ok, err)
	}
	if fmt.Sprint(visited) != fmt.Sprint(want) {
		t.Fatalf("visited=%v, want %v", visited, want)
	}
}
