package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

var benchmarkFallbackEntities []graph.Entity

func TestReadViewEntityPagesPreserveOrderingAndOwnership(t *testing.T) {
	ctx := context.Background()
	g := graph.New()
	g.Version = 7
	for i := range 700 {
		id := fmt.Sprintf("host:%04d", i)
		g.Entities[id] = graph.Entity{ID: id, Kind: []string{"host", "service"}[i%2],
			Fields: graph.Fields{"name": id}, Source: "agent",
			Sources: []graph.EntitySource{{Source: "manual"}}}
	}
	manifest := Manifest{TenantID: "tenant-a", Version: 7}
	cache := NewReaderCache(NewTenantStore(NewMemoryStore(), "test"), time.Minute)
	if err := cache.storeEntryLocked("tenant-a", cacheEntry{graph: g, manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, filter := range []EntityScanOptions{{Limit: 17}, {Kind: "service", Source: "manual", Limit: 13}, {Shard: entityShardID("host:0001"), Limit: 3}, {Limit: -1}} {
				for {
					got, err := cache.ListEntitiesFromReadView(ctx, "tenant-a", g, manifest, filter)
					if err != nil {
						t.Error(err)
						return
					}
					want, err := ListEntitiesFromGraph(ctx, "tenant-a", g, manifest, filter)
					if err != nil {
						t.Error(err)
						return
					}
					assertEntityPageEqual(t, got.Entities, got.NextCursor, want.Entities, want.NextCursor)
					if len(got.Entities) > 0 {
						got.Entities[0].Fields["name"] = "changed by caller"
						if g.Entities[got.Entities[0].ID].Fields["name"] == "changed by caller" {
							t.Error("result mutated read view")
						}
					}
					if got.NextCursor == "" {
						break
					}
					filter.Cursor = got.NextCursor
				}
			}
		}()
	}
	wg.Wait()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := cache.ListEntitiesFromReadView(canceled, "tenant-a", g, manifest, EntityScanOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan: %v", err)
	}
	// A restored view can have the same version and different entity IDs.
	restored := graph.New()
	restored.Version = 7
	restored.Entities["host:restored"] = graph.Entity{ID: "host:restored", Kind: "host"}
	cache.Invalidate("tenant-a")
	if err := cache.storeEntryLocked("tenant-a", cacheEntry{graph: restored, manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	got, err := cache.ListEntitiesFromReadView(ctx, "tenant-a", restored, manifest, EntityScanOptions{})
	if err != nil || len(got.Entities) != 1 || got.Entities[0].ID != "host:restored" {
		t.Fatalf("restored page: %+v, %v", got, err)
	}
	cache.Invalidate("tenant-a")
	if cache.bytes != 0 {
		t.Fatalf("eviction retained %d bytes", cache.bytes)
	}
	// With no space for ordering, pagination still uses the bounded fallback.
	cache.ConfigureCapacity(1, 1)
	if err := cache.storeEntryLocked("tenant-a", cacheEntry{graph: g, manifest: manifest, bytes: 1}); err != nil {
		t.Fatal(err)
	}
	got, err = cache.ListEntitiesFromReadView(ctx, "tenant-a", g, manifest, EntityScanOptions{Limit: 17})
	if err != nil {
		t.Fatal(err)
	}
	want, err := ListEntitiesFromGraph(ctx, "tenant-a", g, manifest, EntityScanOptions{Limit: 17})
	if err != nil {
		t.Fatal(err)
	}
	assertEntityPageEqual(t, got.Entities, got.NextCursor, want.Entities, want.NextCursor)
	if cache.bytes > cache.MaxBytes {
		t.Fatal("scan exceeded cache budget")
	}
}

func TestFallbackEntityPageMatchesFullSort(t *testing.T) {
	entities := make(map[string]graph.Entity, 500)
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("host:%04d", i)
		entities[id] = graph.Entity{ID: id, Kind: []string{"host", "service"}[i%2], Source: []string{"agent", "manual"}[i%2]}
	}
	options := EntityScanOptions{Kind: "host", Source: "agent", Limit: 17}
	first, firstCursor := referenceEntityPage(entities, 7, options, scanCursor{})
	actual, actualCursor, err := pageEntityMap(context.Background(), entities, 7, options, scanCursor{})
	if err != nil {
		t.Fatalf("pageEntityMap: %v", err)
	}
	assertEntityPageEqual(t, actual, actualCursor, first, firstCursor)

	after, err := parseScanCursor(firstCursor, 7, entityScanQueryHash(options))
	if err != nil {
		t.Fatalf("parse cursor: %v", err)
	}
	want, wantCursor := referenceEntityPage(entities, 7, options, after)
	actual, actualCursor, err = pageEntityMap(context.Background(), entities, 7, options, after)
	if err != nil {
		t.Fatalf("pageEntityMap after: %v", err)
	}
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
	actual, actualCursor, err := pageEdgeMap(context.Background(), edges, 11, options, scanCursor{})
	if err != nil {
		t.Fatalf("pageEdgeMap: %v", err)
	}
	if actualCursor != wantCursor || len(actual) != len(want) {
		t.Fatalf("edge page len/cursor = %d/%q, want %d/%q", len(actual), actualCursor, len(want), wantCursor)
	}
	for i := range want {
		if actual[i].ID != want[i].ID {
			t.Fatalf("edge[%d] = %q, want %q", i, actual[i].ID, want[i].ID)
		}
	}
}

func TestFallbackScanStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := pageEntityMap(ctx, map[string]graph.Entity{
		"host:a": {ID: "host:a", Kind: "host"},
	}, 1, EntityScanOptions{}, scanCursor{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pageEntityMap err = %v, want context.Canceled", err)
	}
}

func BenchmarkFallbackEntityPage10K(b *testing.B) {
	entities := make(map[string]graph.Entity, 10_000)
	for i := 0; i < 10_000; i++ {
		id := fmt.Sprintf("host:%05d", i)
		entities[id] = graph.Entity{ID: id, Kind: "host", Source: "agent"}
	}
	options := EntityScanOptions{Kind: "host", Limit: 100}

	b.Run("cached-read-view", func(b *testing.B) {
		g := graph.New()
		g.Version = 1
		g.Entities = entities
		manifest := Manifest{TenantID: "tenant-a", Version: 1}
		cache := NewReaderCache(NewTenantStore(NewMemoryStore(), "test"), time.Minute)
		if err := cache.storeEntryLocked("tenant-a", cacheEntry{graph: g, manifest: manifest}); err != nil {
			b.Fatal(err)
		}
		if _, err := cache.ListEntitiesFromReadView(context.Background(), "tenant-a", g, manifest, options); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			page, err := cache.ListEntitiesFromReadView(context.Background(), "tenant-a", g, manifest, options)
			if err != nil || len(page.Entities) != 100 {
				b.Fatalf("page: %d entities, %v", len(page.Entities), err)
			}
			benchmarkFallbackEntities = page.Entities
		}
	})

	b.Run("full-sort", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkFallbackEntities, _ = referenceEntityPage(entities, 1, options, scanCursor{})
		}
	})
	b.Run("bounded-heap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkFallbackEntities, _, _ = pageEntityMap(context.Background(), entities, 1, options, scanCursor{})
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
