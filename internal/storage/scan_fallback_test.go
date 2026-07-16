package storage

import (
	"fmt"
	"sort"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

var benchmarkFallbackEntities []graph.Entity

func TestFallbackEntityPageMatchesFullSort(t *testing.T) {
	entities := make(map[string]graph.Entity, 500)
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("host:%04d", i)
		entities[id] = graph.Entity{ID: id, Kind: []string{"host", "service"}[i%2], Source: []string{"agent", "manual"}[i%2]}
	}
	options := EntityScanOptions{Kind: "host", Source: "agent", Limit: 17}
	first, firstCursor := referenceEntityPage(entities, 7, options, scanCursor{})
	actual, actualCursor := pageEntityMap(entities, 7, options, scanCursor{})
	assertEntityPageEqual(t, actual, actualCursor, first, firstCursor)

	after, err := parseScanCursor(firstCursor, 7, entityScanQueryHash(options))
	if err != nil {
		t.Fatalf("parse cursor: %v", err)
	}
	want, wantCursor := referenceEntityPage(entities, 7, options, after)
	actual, actualCursor = pageEntityMap(entities, 7, options, after)
	assertEntityPageEqual(t, actual, actualCursor, want, wantCursor)
}

func TestFallbackEdgePageMatchesFullSort(t *testing.T) {
	edges := make(map[string]graph.Edge, 500)
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("edge:%04d", i)
		edges[id] = graph.Edge{
			ID: id, Type: []string{"depends_on", "runs_on"}[i%2],
			From: fmt.Sprintf("service:%03d", i%73), To: fmt.Sprintf("host:%03d", i),
			Source: []string{"agent", "manual"}[i%2],
		}
	}
	options := EdgeScanOptions{Type: "depends_on", Source: "agent", Limit: 19}
	want, wantCursor := referenceEdgePage(edges, 11, options, scanCursor{})
	actual, actualCursor := pageEdgeMap(edges, 11, options, scanCursor{})
	if actualCursor != wantCursor || len(actual) != len(want) {
		t.Fatalf("edge page len/cursor = %d/%q, want %d/%q", len(actual), actualCursor, len(want), wantCursor)
	}
	for i := range want {
		if actual[i].ID != want[i].ID {
			t.Fatalf("edge[%d] = %q, want %q", i, actual[i].ID, want[i].ID)
		}
	}
}

func BenchmarkFallbackEntityPage10K(b *testing.B) {
	entities := make(map[string]graph.Entity, 10_000)
	for i := 0; i < 10_000; i++ {
		id := fmt.Sprintf("host:%05d", i)
		entities[id] = graph.Entity{ID: id, Kind: "host", Source: "agent"}
	}
	options := EntityScanOptions{Kind: "host", Limit: 100}

	b.Run("full-sort", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkFallbackEntities, _ = referenceEntityPage(entities, 1, options, scanCursor{})
		}
	})
	b.Run("bounded-heap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkFallbackEntities, _ = pageEntityMap(entities, 1, options, scanCursor{})
		}
	})
}

func referenceEntityPage(entities map[string]graph.Entity, version int64, options EntityScanOptions, cursor scanCursor) ([]graph.Entity, string) {
	items := make([]graph.Entity, 0, len(entities))
	for _, entity := range entities {
		items = append(items, entity)
	}
	sort.Slice(items, func(i, j int) bool {
		return scanKey(entityShardID(items[i].ID), items[i].ID) < scanKey(entityShardID(items[j].ID), items[j].ID)
	})
	return pageEntities(items, version, options, cursor)
}

func referenceEdgePage(edges map[string]graph.Edge, version int64, options EdgeScanOptions, cursor scanCursor) ([]graph.Edge, string) {
	items := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		items = append(items, edge)
	}
	sort.Slice(items, func(i, j int) bool {
		left := scanKey(items[i].Type+"\x00"+edgeShardID(items[i].From), items[i].ID)
		right := scanKey(items[j].Type+"\x00"+edgeShardID(items[j].From), items[j].ID)
		return left < right
	})
	return pageEdges(items, version, options, cursor)
}

func assertEntityPageEqual(t *testing.T, actual []graph.Entity, actualCursor string, want []graph.Entity, wantCursor string) {
	t.Helper()
	if actualCursor != wantCursor || len(actual) != len(want) {
		t.Fatalf("entity page len/cursor = %d/%q, want %d/%q", len(actual), actualCursor, len(want), wantCursor)
	}
	for i := range want {
		if actual[i].ID != want[i].ID {
			t.Fatalf("entity[%d] = %q, want %q", i, actual[i].ID, want[i].ID)
		}
	}
}
