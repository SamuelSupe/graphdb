package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func TestRangeEdgesMoveBetweenBucketsAndPreserveSelfLoops(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteIndexRangePages = true
	store.WriteEntityRecords = false
	defer store.ShutdownTasks(ctx)
	entities := entitiesForDistinctShards("host", "range-endpoints", 3, entityShardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities:      entities,
		UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}, {Name: "observes", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}},
		UpsertEdges:         []graph.Edge{{Type: "links", From: entities[0].ID, To: entities[0].ID}, {Type: "links", From: entities[0].ID, To: entities[1].ID}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := store.GetReverseIndexCatalog(ctx, "tenant-a", catalog.Version)
	if err != nil {
		t.Fatal(err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog, ReverseCatalog: &reverse}
	if edges, ok, err := lookup.InEdges(ctx, entities[1].ID, nil); err != nil || !ok || len(edges) != 1 {
		t.Fatalf("destination bucket could not read its physical page: %d %v %v", len(edges), ok, err)
	}
	count := 0
	if ok, err := lookup.VisitBothEdges(ctx, entities[0].ID, nil, "", func(graph.Edge, string) (bool, error) { count++; return true, nil }); err != nil || !ok || count != 3 {
		t.Fatalf("self-loop directions: count=%d ok=%v err=%v", count, ok, err)
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{entities[0].ID}, UpsertEdges: []graph.Edge{{Type: "observes", From: entities[1].ID, To: entities[2].ID}}}, CommitOptions{})
	if err != nil || len(result.IndexWarnings) != 0 {
		t.Fatalf("edge move: %+v %v", result, err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	reverse, err = store.GetReverseIndexCatalog(ctx, "tenant-a", catalog.Version)
	if err != nil {
		t.Fatal(err)
	}
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog, ReverseCatalog: &reverse}
	for _, test := range []struct {
		endpoint string
		in       bool
		want     int
	}{{entities[0].ID, false, 0}, {entities[0].ID, true, 0}, {entities[1].ID, true, 0}, {entities[1].ID, false, 1}, {entities[2].ID, true, 1}} {
		var edges []graph.Edge
		var ok bool
		var err error
		if test.in {
			edges, ok, err = lookup.InEdges(ctx, test.endpoint, nil)
		} else {
			edges, ok, err = lookup.OutEdges(ctx, test.endpoint, nil)
		}
		if err != nil || !ok || len(edges) != test.want {
			t.Fatalf("moved endpoint %s/in=%v: %d %v %v", test.endpoint, test.in, len(edges), ok, err)
		}
		if len(edges) > 0 && edges[0].Type != "observes" {
			t.Fatal("old relation survived the move")
		}
	}
}

func TestRangeMigrationDoesNotDuplicateLegacyBucketCandidates(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	defer store.ShutdownTasks(ctx)
	var first, second string
	for i := 0; first == ""; i++ {
		id := fmt.Sprintf("host:legacy-a-%d", i)
		if legacyIndexShardID(id) != entityShardID(id) {
			first = id
		}
	}
	for i := 0; second == ""; i++ {
		id := fmt.Sprintf("host:legacy-b-%d", i)
		if legacyIndexShardID(id) == entityShardID(first) {
			second = id
		}
	}
	entities := []graph.Entity{{ID: first, Kind: "host"}, {ID: second, Kind: "host"}}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	pages := []EntityPageData{{Shard: legacyIndexShardID(first), Version: 1, Entities: entities[:1]}, {Shard: legacyIndexShardID(second), Version: 1, Entities: entities[1:]}}
	catalog := IndexCatalog{Version: 1, TenantID: "tenant-a", EntityPages: entityPageSpecs(pages)}
	store.decorateIndexCatalog(&catalog, "tenant-a")
	for i := range catalog.EntityPages {
		catalog.EntityPages[i].Codec = parquetEntityPageCodec
	}
	if err := store.writeParquetEntityPages(ctx, "tenant-a", pages, 1); err != nil {
		t.Fatal(err)
	}
	// A reader must still try the legacy bucket when the current candidate
	// exists as a range for other IDs during a mixed-layout transition.
	ranged, err := prepareBoundedEntityPages(ctx, "tenant-a", []EntityPageData{{Shard: entityShardID(second), Version: 1, Entities: entities[1:]}})
	if err != nil {
		t.Fatal(err)
	}
	mini := IndexCatalog{Version: 1, EntityPages: entityPageSpecs(ranged)}
	store.decorateIndexCatalog(&mini, "tenant-a")
	if err := store.writeParquetEntityPages(ctx, "tenant-a", ranged, 1); err != nil {
		t.Fatal(err)
	}
	for i := range catalog.EntityPages {
		if catalog.EntityPages[i].Shard == entityShardID(second) {
			catalog.EntityPages[i] = mini.EntityPages[0]
		}
	}
	writeParquetIndexCatalogForTest(t, ctx, store, "tenant-a", catalog)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: 1, Catalog: catalog}
	if _, ok, err := lookup.GetEntity(ctx, first, nil); err != nil || !ok {
		t.Fatal("range candidate hid a legacy entity", err)
	}
	store.WriteIndexRangePages = true
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: first, Kind: "host", Fields: graph.Fields{"updated": true}}}}, CommitOptions{})
	if err != nil || len(result.IndexWarnings) != 0 {
		t.Fatalf("controlled legacy migration failed: %+v %v", result, err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.EntityPages) != 1 || catalog.EntityPages[0].EntityCount != 2 || !hasIndexRanges(catalog.EntityPages[0].Objects) {
		t.Fatal("legacy candidate buckets were both updated instead of rebuilt")
	}
	page, err := store.ListEntitiesFromCatalog(ctx, "tenant-a", catalog, EntityScanOptions{Limit: 10})
	if err != nil || len(page.Entities) != 2 || page.Entities[0].ID == page.Entities[1].ID {
		t.Fatal("legacy migration duplicated or lost an entity", err)
	}
}

func TestRangeCanceledEncoderWaitReleasesMaintenance(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	store.WriteIndexRangePages = true
	store.WriteEntityRecords = false
	defer store.ShutdownTasks(ctx)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	previous, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	store.maintenance.encodes = make(chan struct{}, 1)
	store.maintenance.encodes <- struct{}{}
	canceled, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = store.RebuildIndexes(canceled, "tenant-a")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("range probe ignored cancellation while waiting for an encoder", err)
	}
	if store.maintenance.bytes != 0 || len(store.maintenance.builds) != 0 {
		t.Fatal("canceled probe retained maintenance resources")
	}
	<-store.maintenance.encodes
	current, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	beforeHash, _ := indexCatalogContentHash(previous)
	afterHash, _ := indexCatalogContentHash(current)
	if beforeHash != afterHash {
		t.Fatal("canceled rebuild replaced the published catalog")
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal("canceled encoder wait prevented a later rebuild", err)
	}
	if len(store.maintenance.encodes) != 0 {
		t.Fatal("successful rebuild retained an encoder slot")
	}
}

func TestRangeSparseCandidatesConsumeQueryBudget(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteIndexRangePages = true
	store.WriteEntityRecords = false
	defer store.ShutdownTasks(ctx)
	var entities []graph.Entity
	for i := 0; len(entities) < 1100; i++ {
		id := fmt.Sprintf("host:budget-%07d", i)
		if entityShardID(id) == "00" {
			entities = append(entities, graph.Entity{ID: id, Kind: "host"})
		}
	}
	var edges []graph.Edge
	for _, entity := range entities[2:] {
		edges = append(edges, graph.Edge{Type: "links", From: entities[0].ID, To: entity.ID})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities, UpsertEdges: edges, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := store.GetReverseIndexCatalog(ctx, "tenant-a", catalog.Version)
	if err != nil {
		t.Fatal(err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog, ReverseCatalog: &reverse}
	g := graph.New()
	g.Version = catalog.Version
	requests := []query.Request{{Op: "match", Kind: "missing", Limit: 1, CostLimit: 8}}
	for _, direction := range []string{"out", "in", "both"} {
		requests = append(requests, query.Request{Op: "neighbors", ID: entities[1].ID, Direction: direction, Limit: 1, CostLimit: 8})
	}
	for _, request := range requests {
		_, err := query.ExecuteContextWithOptions(ctx, g, request, query.ExecuteOptions{IndexLookup: lookup, EntityLookup: lookup})
		if !errors.Is(err, query.ErrLimitExceeded) {
			t.Fatalf("sparse %s/%s bypassed cost limit: %v", request.Op, request.Direction, err)
		}
	}
}

func TestRangePublicationFailuresRetainPreviousCatalog(t *testing.T) {
	for _, mode := range []string{"page_write", "catalog_cas", "uncertain_publication"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base := NewMemoryStore()
			store := NewTenantStore(base, "test")
			store.WriteEntityRecords = false
			store.WriteIndexRangePages = true
			defer store.ShutdownTasks(ctx)
			if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"value": "old"}}}}, CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			previous, err := store.RebuildIndexes(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			rule := faultMatrixRule{name: mode, op: "write", contains: "/entities/pages/", err: ErrObjectStoreUnavailable, remaining: -1}
			if mode != "page_write" {
				rule.contains = "/indexes/catalog.parquet"
				rule.op = "put_conditional"
				rule.err = ErrConflict
			}
			if mode == "uncertain_publication" {
				rule.op = "write"
				rule.err = ErrObjectStoreUnavailable
			}
			faults := newFaultMatrixStore(base, rule)
			store.Objects = faults
			fired := func() bool { return faults.fired(mode) > 0 }
			if mode == "uncertain_publication" {
				after := &uncertainRangeCatalogStore{ObjectStore: base}
				store.Objects = after
				fired = func() bool { return after.failed.Load() }
			}
			result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"value": "new"}}}}, CommitOptions{})
			if err != nil || result.Version != 2 || len(result.IndexWarnings) == 0 {
				t.Fatalf("accepted graph commit changed outcome: %+v %v", result, err)
			}
			if !fired() {
				t.Fatal("publication fault was not exercised")
			}
			reader := NewTenantStore(base, "test")
			defer reader.ShutdownTasks(ctx)
			old := &PersistedIndexLookup{Store: reader, TenantID: "tenant-a", Version: previous.Version, Catalog: previous}
			entity, ok, err := old.GetEntity(ctx, "host:a", nil)
			if err != nil || !ok || entity.Fields["value"] != "old" {
				t.Fatal("failed index publication damaged the previous directory", err)
			}
			current, _, err := reader.Load(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if entity, ok := current.GetEntity("host:a"); !ok || entity.Fields["value"] != "new" {
				t.Fatal("index fault lost committed graph data")
			}
			if mode == "uncertain_publication" {
				published, err := reader.GetIndexCatalog(ctx, "tenant-a")
				if err != nil || published.Version != 2 {
					t.Fatal("ambiguous publication was removed", err)
				}
				q := &PersistedIndexLookup{Store: reader, TenantID: "tenant-a", Version: 2, Catalog: published}
				if entity, ok, err := q.GetEntity(ctx, "host:a", nil); err != nil || !ok || entity.Fields["value"] != "new" {
					t.Fatal("published range data was removed", err)
				}
			}
			store.Objects = base
			if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRangeSplitDeleteReinsertAndPinnedGC(t *testing.T) {
	ctx := context.Background()
	objects, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	store := NewTenantStore(objects, "test")
	store.WriteIndexRangePages = true
	store.WriteEntityRecords = false
	defer store.ShutdownTasks(ctx)
	var entities []graph.Entity
	for i := 0; len(entities) < 1025; i++ {
		id := fmt.Sprintf("host:lifecycle-%07d", i)
		if entityShardID(id) == "00" {
			entities = append(entities, graph.Entity{ID: id, Kind: "host"})
		}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities[:1024]}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	original, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.PinReadView(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	old := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: original.Version, Catalog: original}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities[1024:]}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	spec := requireEntityPageSpec(t, catalog, "00")
	if len(spec.Objects) != 2 || spec.Objects[0].RowCount < 500 || spec.Objects[1].RowCount < 500 {
		t.Fatal("append did not split a full page evenly")
	}
	untouched := spec.Objects[1].Key
	page, ok, err := store.borrowEntityRange(ctx, "tenant-a", catalog.Version, spec, 0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	var ids []string
	for _, entity := range page.Entities {
		ids = append(ids, entity.ID)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteEntities: ids}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	spec = requireEntityPageSpec(t, catalog, "00")
	if len(spec.Objects) != 1 || spec.Objects[0].Key != untouched {
		t.Fatal("deleting an empty page rewrote its neighbor")
	}
	routing, err := decodeIndexRange(spec.Objects[0].Role)
	if err != nil || routing.Lower != "" || routing.Upper != "" {
		t.Fatal("neighbor did not take over the vacated range")
	}
	page, ok, err = store.borrowEntityRange(ctx, "tenant-a", catalog.Version, spec, 0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	ids = nil
	for _, entity := range page.Entities {
		ids = append(ids, entity.ID)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteEntities: ids}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil || len(catalog.EntityPages) != 0 {
		t.Fatal("empty bucket retained a data reference", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities[:1]}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := old.GetEntity(ctx, entities[100].ID, nil); err != nil || !ok {
		t.Fatal("GC lost a pinned range page", err)
	}
	release()
	// Removing an obsolete catalog invalidates the local retirement proof.
	// A subsequent complete sweep must reclaim the now-unpinned data page.
	for pass := 0; pass < 8; pass++ {
		reclaimed, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true})
		if err != nil {
			t.Fatal(err)
		}
		_, err = objects.Get(ctx, original.EntityPages[0].Objects[0].Key)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil || pass == 7 || reclaimed.Checkpoint.DeferredFiles == 0 {
			t.Fatalf("unreferenced range page was not reclaimed after unpinning: %v report=%+v", err, reclaimed)
		}
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewTenantStore(objects, "test")
	defer fresh.ShutdownTasks(ctx)
	lookup := &PersistedIndexLookup{Store: fresh, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if _, ok, err := lookup.GetEntity(ctx, entities[0].ID, nil); err != nil || !ok {
		t.Fatal("reinsert failed", err)
	}
	key := catalog.EntityPages[0].Objects[0].Key
	if err := objects.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	cold := NewTenantStore(objects, "test")
	defer cold.ShutdownTasks(ctx)
	lookup = &PersistedIndexLookup{Store: cold, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if ok, err := lookup.VisitEntities(ctx, "", nil, "", func(graph.Entity) (bool, error) { t.Fatal("missing page delivered a row"); return false, nil }); err == nil && ok {
		t.Fatal("missing page was treated as an empty bucket")
	}
	result, err := cold.ListEntities(ctx, "tenant-a", EntityScanOptions{Limit: 10})
	if err != nil || len(result.Entities) != 1 {
		t.Fatal("missing index page did not fall back to the graph", err)
	}
}

func TestRangeWriteRequiresExplicitEnableAndSupportsDowngrade(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	defer store.ShutdownTasks(ctx)
	if store.WriteIndexRangePages {
		t.Fatal("new format was enabled before the reader rollout")
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{false, true, false} {
		store.WriteIndexRangePages = enabled
		catalog, err := store.RebuildIndexes(ctx, "tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		if hasIndexRanges(catalog.EntityPages[0].Objects) != enabled {
			t.Fatal("rebuild ignored the explicit write format")
		}
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"enabled": enabled}}}}, CommitOptions{}); err != nil {
			t.Fatal(err)
		}
		catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		if hasIndexRanges(catalog.EntityPages[0].Objects) != enabled {
			t.Fatal("incremental update changed the selected write format")
		}
	}
}

type uncertainRangeCatalogStore struct {
	ObjectStore
	failed atomic.Bool
}

func (s *uncertainRangeCatalogStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, condition)
	if err == nil && strings.HasSuffix(key, "/indexes/catalog.parquet") && s.failed.CompareAndSwap(false, true) {
		return meta, ErrObjectStoreUnavailable
	}
	return meta, err
}
