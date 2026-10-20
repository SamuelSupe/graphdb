package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestIncrementalEntityPageRewriteIsBounded(t *testing.T) {
	ctx := context.Background()
	var entities []graph.Entity
	for i := 0; len(entities) < 4096; i++ {
		id := fmt.Sprintf("host:bounded-%d", i)
		if entityShardID(id) == "00" {
			entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"value": i}})
		}
	}
	before, err := graph.FromSnapshot(graph.Snapshot{Version: 1, Entities: entities})
	if err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteIndexRangePages = true
	artifacts, err := buildTenantIndexArtifacts(ctx, "tenant-a", before, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.decorateIndexCatalog(&artifacts.Catalog, "tenant-a")
	if err := store.writeParquetEntityPages(ctx, "tenant-a", artifacts.EntityPages, 1); err != nil {
		t.Fatal(err)
	}
	id := entities[0].ID
	after, _, err := before.ApplyCommitStorageCopyWithOptions(graph.Commit{ID: "one-change", Version: 2, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: id, Kind: "host", Fields: graph.Fields{"value": -1}}}}}, graph.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pages, _, err := store.buildIncrementalEntityPages(ctx, "tenant-a", 1, artifacts.Catalog.EntityPages, before, after, []string{id}, 2, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, page := range pages {
		rows += len(page.Entities)
	}
	if rows > 1024 {
		t.Fatalf("one changed entity rewrites %d entities; want at most 1024 after a fresh index build", rows)
	}
}

func TestIncrementalEdgeShardRewriteIsBounded(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%v", reverse), func(t *testing.T) {
			ctx := context.Background()
			entities := []graph.Entity{{ID: "host:hub", Kind: "host"}}
			var edges []graph.Edge
			for i := 0; i < 4096; i++ {
				id := fmt.Sprintf("host:leaf-%04d", i)
				entities = append(entities, graph.Entity{ID: id, Kind: "host"})
				edge := graph.Edge{ID: fmt.Sprintf("edge:%04d", i), Type: "links", From: "host:hub", To: id}
				if reverse {
					edge.From, edge.To = edge.To, edge.From
				}
				edges = append(edges, edge)
			}
			before, err := graph.FromSnapshot(graph.Snapshot{Version: 1, Entities: entities, Edges: edges, RelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}})
			if err != nil {
				t.Fatal(err)
			}
			store := NewTenantStore(NewMemoryStore(), "test")
			store.WriteIndexRangePages = true
			artifacts, err := buildTenantIndexArtifacts(ctx, "tenant-a", before, 1, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			store.decorateIndexCatalog(&artifacts.Catalog, "tenant-a")
			previous := artifacts.Catalog.EdgeShards
			if reverse {
				catalog, _, err := store.prepareReverseIndex(ctx, "tenant-a", before, 1)
				if err != nil {
					t.Fatal(err)
				}
				previous = catalog.EdgeShards
			} else if err := store.writeParquetEdgeShards(ctx, "tenant-a", artifacts.EdgeShards); err != nil {
				t.Fatal(err)
			}
			var edge graph.Edge
			for _, item := range before.Edges.All() {
				edge = item
				break
			}
			edge.Fields = graph.Fields{"value": -1}
			after, _, err := before.ApplyCommitStorageCopyWithOptions(graph.Commit{ID: "one-edge", Version: 2, Mutations: graph.Mutations{UpsertEdges: []graph.Edge{edge}}}, graph.ApplyOptions{})
			if err != nil {
				t.Fatal(err)
			}
			shards, _, err := store.buildIncrementalEdgeShardsFor(ctx, "tenant-a", 1, previous, before, after, []string{edge.ID}, 2, time.Unix(1, 0), reverse,
				func(e graph.Edge) string {
					if reverse {
						return edgeShardID(e.To)
					}
					return edgeShardID(e.From)
				},
				func(c *IndexCatalog) {
					if reverse {
						store.decorateReverseEdgeCatalog(c, "tenant-a")
					} else {
						store.decorateIndexCatalog(c, "tenant-a")
					}
				})
			if err != nil {
				t.Fatal(err)
			}
			rows := 0
			for _, shard := range shards {
				rows += len(shard.Edges)
			}
			if rows > 1024 {
				t.Fatalf("one changed edge rewrites %d edges; want at most 1024 after a fresh index build", rows)
			}
		})
	}
}

func TestBoundedIndexPagesRoundTrip(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%v", local), func(t *testing.T) {
			ctx := context.Background()
			var objects ObjectStore = NewMemoryStore()
			if local {
				files, err := OpenFileStore(filepath.Join(t.TempDir(), "data"))
				if err != nil {
					t.Fatal(err)
				}
				defer files.Close()
				objects = files
			}
			store := NewTenantStore(objects, "test")
			store.WriteIndexRangePages = true
			defer store.ShutdownTasks(ctx)
			store.WriteEntityRecords = !local
			store.UseEntityRecordsForRead = !local
			var entities []graph.Entity
			for i := 0; len(entities) < 2050; i++ {
				id := fmt.Sprintf("host:range-%07d", i)
				if entityShardID(id) == "00" {
					entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"value": i}})
				}
			}
			hub := entities[0].ID
			var edges []graph.Edge
			for _, entity := range entities[1:] {
				edges = append(edges, graph.Edge{Type: "links", From: hub, To: entity.ID}, graph.Edge{Type: "links", From: entity.ID, To: hub})
			}
			if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities, UpsertEdges: edges, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			original, err := store.RebuildIndexes(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			first := requireEntityPageSpec(t, original, "00")
			if !hasIndexRanges(first.Objects) || len(first.Objects) < 2 {
				t.Fatalf("expected bounded pages: %#v", first)
			}
			health, err := store.IndexHealth(ctx, "tenant-a")
			if err != nil || health.Status != "ready" {
				t.Fatalf("initial health=%s issues=%v err=%v", health.Status, health.Issues[:min(5, len(health.Issues))], err)
			}
			g, _, err := store.Load(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			var changedEdge graph.Edge
			for _, edge := range g.Edges.All() {
				if edge.From == hub {
					changedEdge = edge
					break
				}
			}
			changedEdge.Fields = graph.Fields{"state": "updated"}
			changedEntity := entities[0]
			changedEntity.Fields = graph.Fields{"value": -1}
			result, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{changedEntity}, UpsertEdges: []graph.Edge{changedEdge}}, CommitOptions{})
			if err != nil {
				t.Fatal(err)
			}
			current, err := store.GetIndexCatalogAtVersion(ctx, "tenant-a", result.Version)
			if err != nil {
				t.Fatal(err)
			}
			next := requireEntityPageSpec(t, current, "00")
			oldKeys := map[string]bool{}
			for _, object := range first.Objects {
				oldKeys[object.Key] = true
			}
			rewritten := 0
			for _, object := range next.Objects {
				if !oldKeys[object.Key] {
					rewritten += object.RowCount
				}
			}
			if rewritten > 1024 || rewritten == 0 {
				t.Fatalf("rewritten entity rows=%d", rewritten)
			}
			reverse, err := store.GetReverseIndexCatalog(ctx, "tenant-a", current.Version)
			if err != nil {
				t.Fatal(err)
			}
			lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: current.Version, Catalog: current, ReverseCatalog: &reverse}
			for _, index := range []int{0, 1024, len(entities) - 1} {
				entity, ok, err := lookup.GetEntity(ctx, entities[index].ID, nil)
				if err != nil || !ok || entity.ID != entities[index].ID {
					t.Fatalf("point %d: %#v %v %v", index, entity, ok, err)
				}
			}
			entity, ok, err := lookup.GetEntity(ctx, hub, nil)
			if err != nil || !ok || entity.Fields["value"] != float64(-1) {
				t.Fatalf("changed entity=%#v ok=%v err=%v", entity, ok, err)
			}
			oldLookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: original.Version, Catalog: original}
			entity, ok, err = oldLookup.GetEntity(ctx, hub, nil)
			if err != nil || !ok || entity.Fields["value"] == float64(-1) {
				t.Fatalf("old catalog changed: %#v %v %v", entity, ok, err)
			}
			seen := map[string]bool{}
			cursor := ""
			for {
				page, err := store.ListEntitiesFromCatalog(ctx, "tenant-a", current, EntityScanOptions{Limit: 127, Cursor: cursor})
				if err != nil {
					t.Fatal(err)
				}
				for _, entity := range page.Entities {
					if seen[entity.ID] {
						t.Fatalf("duplicate scan entity %s", entity.ID)
					}
					seen[entity.ID] = true
				}
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			if len(seen) != len(entities) {
				t.Fatalf("scan=%d want=%d", len(seen), len(entities))
			}
			// Each fresh lookup has cold data caches; a deep page must not load
			// the earlier physical pages in the same logical bucket.
			for _, mode := range []string{"point", "batch", "entity_visit", "entity_scan", "out", "in", "both"} {
				t.Run(mode, func(t *testing.T) {
					counted := &countingMetaReadStore{ObjectStore: objects}
					cold := NewTenantStore(counted, "test")
					defer cold.ShutdownTasks(ctx)
					q := &PersistedIndexLookup{Store: cold, TenantID: "tenant-a", Version: current.Version, Catalog: current, ReverseCatalog: &reverse}
					count := 0
					limit := 1
					switch mode {
					case "point":
						_, ok, err := q.GetEntity(ctx, entities[1800].ID, nil)
						if err != nil || !ok {
							t.Fatalf("point: %v %v", ok, err)
						}
					case "batch":
						values, ok, err := q.GetEntities(ctx, []string{entities[0].ID, entities[1800].ID, entities[1801].ID}, nil)
						if err != nil || !ok || len(values) != 3 {
							t.Fatalf("batch: %d %v %v", len(values), ok, err)
						}
						limit = 2
					case "entity_visit":
						ok, err := q.VisitEntities(ctx, "host", nil, entities[1800].ID, func(e graph.Entity) (bool, error) { count++; return count < 7, nil })
						if err != nil || !ok || count != 7 {
							t.Fatalf("visit: %d %v %v", count, ok, err)
						}
					case "entity_scan":
						opts := EntityScanOptions{Limit: 7}
						hash, _ := indexCatalogContentHash(current)
						opts.Cursor = encodeScanCursor(scanCursor{Version: current.Version, CatalogHash: hash, After: scanKey("00", entities[1800].ID), Query: entityScanQueryHash(opts)})
						page, err := cold.ListEntitiesFromCatalog(ctx, "tenant-a", current, opts)
						if err != nil || len(page.Entities) != 7 {
							t.Fatalf("scan: %d %v", len(page.Entities), err)
						}
					default:
						var all []graph.Edge
						for _, edge := range g.Edges.All() {
							all = append(all, edge)
						}
						sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
						start := all[len(all)-30].ID
						callback := func(edge graph.Edge) (bool, error) { count++; return count < 7, nil }
						var ok bool
						var err error
						if mode == "out" {
							ok, err = q.VisitOutEdges(ctx, hub, nil, start, callback)
						} else if mode == "in" {
							ok, err = q.VisitInEdges(ctx, hub, nil, start, callback)
						} else {
							limit = 2
							ok, err = q.VisitBothEdges(ctx, hub, nil, start, func(e graph.Edge, _ string) (bool, error) { return callback(e) })
						}
						if err != nil || !ok || count != 7 {
							t.Fatalf("edges: %d %v %v", count, ok, err)
						}
					}
					loaded := 0
					for _, spec := range current.EntityPages {
						for _, object := range spec.Objects {
							loaded += counted.GetWithMetaCount(object.Key)
						}
					}
					for _, spec := range current.EdgeShards {
						for _, object := range spec.Objects {
							loaded += counted.GetWithMetaCount(object.Key)
						}
					}
					for _, spec := range reverse.EdgeShards {
						for _, object := range spec.Objects {
							loaded += counted.GetWithMetaCount(object.Key)
						}
					}
					if loaded != limit {
						t.Fatalf("deep %s loaded %d physical pages, want %d", mode, loaded, limit)
					}
				})
			}
			out, ok, err := lookup.OutEdges(ctx, hub, nil)
			if err != nil || !ok || len(out) != len(entities)-1 {
				t.Fatalf("out=%d ok=%v err=%v", len(out), ok, err)
			}
			in, ok, err := lookup.InEdges(ctx, hub, nil)
			if err != nil || !ok || len(in) != len(entities)-1 {
				t.Fatalf("in=%d ok=%v err=%v", len(in), ok, err)
			}
			sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
			visited := 0
			ok, err = lookup.VisitOutEdges(ctx, hub, nil, out[1000].ID, func(edge graph.Edge) (bool, error) {
				if edge.ID < out[1000].ID {
					t.Fatal("edge cursor moved backwards")
				}
				visited++
				return true, nil
			})
			if err != nil || !ok || visited != len(out)-1000 {
				t.Fatalf("edge seek=%d ok=%v err=%v", visited, ok, err)
			}
			health, err = store.IndexHealth(ctx, "tenant-a")
			if err != nil || health.Status != "ready" {
				t.Fatalf("updated health=%s issues=%v err=%v", health.Status, health.Issues[:min(5, len(health.Issues))], err)
			}
			if _, err := store.Compact(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
			audit, err := store.AuditIntegrity(ctx, "tenant-a", IntegrityAuditOptions{Deep: true})
			if err != nil || audit.Status != "ok" {
				t.Fatalf("audit status=%s issues=%v err=%v", audit.Status, audit.Issues[:min(5, len(audit.Issues))], err)
			}
			if local {
				backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
				if err != nil {
					t.Fatal(err)
				}
				backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
				restore, err := store.StartTask(ctx, "tenant-restored", TaskTypeTenantRestore, map[string]any{"backup_key": backup.ResultKey, "required_source_tenant": "tenant-a"})
				if err != nil {
					t.Fatal(err)
				}
				waitForLocalRestoreTask(t, store, "tenant-restored", restore.ID)
				restored, err := store.GetIndexCatalog(ctx, "tenant-restored")
				if err != nil {
					t.Fatal(err)
				}
				if len(restored.EntityPages) != 1 || !hasIndexRanges(restored.EntityPages[0].Objects) || restored.EntityPages[0].EntityCount != len(entities) {
					t.Fatal("restore lost the selected range layout or entities")
				}
				r, err := store.GetReverseIndexCatalog(ctx, "tenant-restored", restored.Version)
				if err != nil {
					t.Fatal(err)
				}
				q := &PersistedIndexLookup{Store: store, TenantID: "tenant-restored", Version: restored.Version, Catalog: restored, ReverseCatalog: &r}
				incoming, ok, err := q.InEdges(ctx, hub, nil)
				if err != nil || !ok || len(incoming) != len(entities)-1 {
					t.Fatalf("restored reverse pages: %d %v %v", len(incoming), ok, err)
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, _, err := lookup.GetEntity(canceled, hub, nil); err == nil {
				t.Fatal("canceled range read succeeded")
			}
		})
	}
}
