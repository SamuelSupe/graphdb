package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

var benchmarkIndexBuildArtifacts indexBuildArtifacts

func TestStreamedIndexBuildMatchesRetainedArtifacts(t *testing.T) {
	g, err := graph.FromSnapshot(graph.Snapshot{Version: 1, CITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Unique: true}, "state": {Type: "string", Indexed: true}}}}, Entities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a", "state": "ready"}}, {ID: "host:b", Kind: "host", Fields: graph.Fields{"name": "b", "state": "ready"}}}})
	if err != nil {
		t.Fatal(err)
	}
	definitions := []IndexDefinition{{Kind: "host", Field: "name"}, {Kind: "host", Field: "state"}}
	at := time.Unix(100, 0).UTC()
	retained, err := buildIndexArtifactsWithDefinitions(g, 1, definitions, at)
	if err != nil {
		t.Fatal(err)
	}
	written := map[string]SecondaryIndex{}
	streamed, err := buildIndexArtifacts(context.Background(), g, 1, definitions, func(index SecondaryIndex) error {
		data, err := marshalParquetSecondaryIndex(context.Background(), index)
		if err != nil {
			return err
		}
		decoded, err := decodeParquetSecondaryIndex(context.Background(), data, "", index.Kind, index.Field, 1, index.Unique)
		written[index.Kind+"."+index.Field] = decoded
		return err
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(retained.Catalog)
	right, _ := json.Marshal(streamed.Catalog)
	if !bytes.Equal(left, right) {
		t.Fatal("streaming changed the published index catalog")
	}
	for _, index := range retained.Indexes {
		decoded, ok := written[index.Kind+"."+index.Field]
		if !ok || secondaryIndexContentHash(index) != secondaryIndexContentHash(decoded) {
			t.Fatal("streaming changed persisted postings")
		}
	}
}

func TestPageAndEdgeWritesUseSharedEncodeBudget(t *testing.T) {
	g, err := graph.FromSnapshot(graph.Snapshot{
		Version:       2,
		CITypes:       []graph.CIType{{Name: "host"}},
		RelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}},
		Entities:      []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}},
		Edges:         []graph.Edge{{ID: "link:a:b", Type: "links", From: "host:a", To: "host:b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"pages", "incremental_pages", "incremental_edges", "reverse_edges"} {
		t.Run(mode, func(t *testing.T) {
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { files.Close() })
			blocked := &blockOncePutStore{ObjectStore: files, substring: "/", paused: make(chan struct{}), resume: make(chan struct{})}
			store := NewTenantStore(blocked, "test")
			store.WriteEntityRecords = false
			store.maintenance.encodes = make(chan struct{}, 1)
			artifacts, err := buildIndexArtifactsWithDefinitions(g, 2, nil)
			if err != nil {
				t.Fatal(err)
			}
			store.decorateIndexCatalog(&artifacts.Catalog, "tenant-a")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			t.Cleanup(cancel)
			done := make(chan error, 1)
			go func() {
				switch mode {
				case "pages":
					done <- store.writeParquetEntityPages(ctx, "tenant-a", artifacts.EntityPages, 2)
				case "incremental_pages":
					done <- store.writeChangedParquetEntityPagesFast(ctx, "tenant-a", artifacts.EntityPages, artifacts.Catalog, nil, g, nil, 2)
				case "incremental_edges":
					done <- store.writeChangedParquetEdgeShardsFast(ctx, "tenant-a", artifacts.EdgeShards, artifacts.Catalog, 2)
				case "reverse_edges":
					_, _, err := store.prepareReverseIndex(ctx, "tenant-a", g, 2)
					done <- err
				}
			}()
			t.Cleanup(func() {
				close(blocked.resume)
				if err := <-done; err != nil {
					t.Errorf("index write did not finish: %v", err)
				}
			})
			select {
			case <-blocked.paused:
			case <-ctx.Done():
				t.Fatal("index write did not start")
			}
			if got := len(store.maintenance.encodes); got != 1 {
				t.Fatalf("active index encoding used %d budget slots, want 1", got)
			}
		})
	}
}

func TestLocalMaintenanceMemoryBudgetCancellationAndProgress(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "test")
	store.MaxMaintenanceBytes = 1 // Both tenants exceed the budget and must run alone.
	target := store.restoreDrillTargetStore("drill", store.Objects)
	for tenant, writer := range map[string]*TenantStore{"tenant-a": store, "tenant-b": target} {
		if _, err := writer.Commit(ctx, tenant, indexMutations(), CommitOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	blocked := &blockOncePutStore{ObjectStore: files, substring: "/snapshots/sharded/", paused: make(chan struct{}), resume: make(chan struct{})}
	resume := sync.OnceFunc(func() { close(blocked.resume) })
	t.Cleanup(resume)
	store.Objects = blocked
	done := make(chan error, 1)
	go func() { _, err := store.Compact(ctx, "tenant-a"); done <- err }()
	select {
	case <-blocked.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("compact did not start")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err = target.RebuildIndexes(waitCtx, "tenant-b")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rebuild exceeded active memory budget: %v", err)
	}
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := target.RebuildIndexes(ctx, "tenant-b"); err != nil {
		t.Fatalf("canceled waiter leaked budget: %v", err)
	}
}

func TestIncrementalIndexAcceptsRepeatedEntityChanges(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	before, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"app-01-extra", "changed"} {
		t.Run(value, func(t *testing.T) {
			after, report, err := before.ApplyCommitCopyWithOptions(graph.Commit{
				Version: 2,
				Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
					ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": value},
				}}},
			}, graph.ApplyOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// A WAL group can update the same entity in multiple logical commits.
			report.AffectedEntityIDs = append(report.AffectedEntityIDs, report.AffectedEntityIDs...)
			artifacts, err := store.buildIncrementalIndexArtifacts(ctx, "tenant-a", catalog, before, after, report, 2)
			if err != nil {
				t.Fatalf("repeated changes forced a full index rebuild: %v", err)
			}
			if len(artifacts.IncrementalIndexWrites) == 0 {
				t.Fatal("missing incremental index changes")
			}
			for _, write := range artifacts.IncrementalIndexWrites {
				if write.Index.Field != "hostname" {
					continue
				}
				ids := write.Index.Values["s:"+value]
				if len(ids) != 1 || ids[0] != "host:app-01" || len(write.Index.Values["s:app-01"]) != 0 {
					t.Fatalf("repeated change corrupted index postings: %#v", write.Index.Values)
				}
			}
		})
	}
}

func TestIncrementalEntityPagesPreserveNormalizedHashes(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	for _, number := range []any{float64(2), int(2), json.Number("2")} {
		before, after := graph.New(), graph.New()
		before.Version, after.Version = 1, 2
		after.Entities.Set("host:a", graph.Entity{ID: "host:a", Kind: "host", Fields: graph.Fields{"nested": map[string]any{"count": number}}})
		pages, _, err := store.buildIncrementalEntityPages(context.Background(), "tenant-a", 1, nil, before, after, []string{"host:a"}, 2, time.Now())
		if err != nil || len(pages) != 1 {
			t.Fatalf("incremental pages: %v, %v", pages, err)
		}
		legacy := pages[0]
		legacy.logicalContentHash, legacy.hashCanonical = "", false
		if entityPageContentHash(pages[0]) != entityPageContentHash(legacy) {
			t.Fatalf("normalized hash changed for %T", number)
		}
		if _, err := marshalParquetEntityPage(context.Background(), pages[0]); err != nil {
			t.Fatal(err)
		}
		if after.Entities.At("host:a").Fields["nested"].(map[string]any)["count"] != number {
			t.Fatal("index preparation mutated graph fields")
		}
	}
}

func TestIncrementalEdgeShardsPreserveNormalizedHashes(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	for _, number := range []any{float64(2), int(2), json.Number("2")} {
		before := graph.New()
		before.Version = 1
		after, err := graph.FromSnapshot(graph.Snapshot{
			Version: 2,
			CITypes: []graph.CIType{{Name: "host"}},
			RelationTypes: []graph.RelationType{
				{Name: "links", FromKind: "host", ToKind: "host", Directed: true},
				{Name: "observes", FromKind: "host", ToKind: "host", Directed: true},
			},
			Entities: []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}},
			Edges: []graph.Edge{
				{Type: "links", From: "host:a", To: "host:b"},
				{Type: "observes", From: "host:a", To: "host:b"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		var edgeIDs []string
		for id, edge := range after.Edges.All() {
			edgeIDs = append(edgeIDs, id)
			edge.Fields = graph.Fields{"nested": map[string]any{"count": number}}
			after.Edges.Set(id, edge)
		}
		for _, reverse := range []bool{false, true} {
			shards, _, err := store.buildIncrementalEdgeShardsFor(context.Background(), "tenant-a", 1, nil, before, after, edgeIDs, 2, time.Now(), reverse, func(edge graph.Edge) string {
				if reverse {
					return edgeShardID(edge.To)
				}
				return edgeShardID(edge.From)
			}, func(*IndexCatalog) {})
			if err != nil || len(shards) != 2 {
				t.Fatalf("incremental edges: %v, %v", shards, err)
			}
			for _, shard := range shards {
				if len(shard.Edges) != 1 || shard.Edges[0].Type != shard.RelationType {
					t.Fatal("incremental shard lost or mixed relation types")
				}
				legacy := shard
				legacy.logicalContentHash, legacy.hashCanonical = "", false
				if edgeShardContentHash(shard) != edgeShardContentHash(legacy) {
					t.Fatalf("normalized hash changed for %T (reverse=%v)", number, reverse)
				}
				data, err := marshalParquetEdgeShard(context.Background(), shard)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := decodeParquetEdgeShard(context.Background(), data, "tenant-a", shard.RelationType, shard.Shard, 2)
				if err != nil {
					t.Fatal(err)
				}
				if edgeShardContentHash(decoded) != edgeShardContentHash(shard) {
					t.Fatal("persisted edge shard hash changed")
				}
				if after.Edges.At(shard.Edges[0].ID).Fields["nested"].(map[string]any)["count"] != number {
					t.Fatal("index preparation mutated graph fields")
				}
			}
		}
	}
}

func TestPreparedIndexArtifactsPreserveLogicalHashes(t *testing.T) {
	g, err := graph.FromSnapshot(graph.Snapshot{
		Version: 7,
		CITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"state": {Type: "string", Indexed: true},
				"meta":  {Type: "object"},
			},
		}},
		RelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}},
		Entities: []graph.Entity{
			{ID: "host:a", Kind: "host", Fields: graph.Fields{"state": "ready", "meta": map[string]any{"attempts": 1, "tags": []any{"blue", 2}}}},
			{ID: "host:b", Kind: "host", Fields: graph.Fields{"state": "down", "meta": map[string]any{"attempts": 3}}},
		},
		Edges: []graph.Edge{{ID: "link:a:b", Type: "links", From: "host:a", To: "host:b", Fields: graph.Fields{"weight": 4}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := buildIndexArtifactsWithDefinitions(g, 8, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, index := range artifacts.Indexes {
		legacy := index
		legacy.logicalContentHash = ""
		legacy.hashCanonical = false
		legacy.cachedObjectGroups = nil
		if got, want := secondaryIndexContentHash(index), secondaryIndexContentHash(legacy); got != want {
			t.Fatalf("secondary index hash changed: got %s want %s", got, want)
		}
		for _, group := range secondaryIndexObjectGroups(index) {
			legacyGroup := group.Index
			legacyGroup.logicalContentHash = ""
			legacyGroup.hashCanonical = false
			if got, want := secondaryIndexContentHash(group.Index), secondaryIndexContentHash(legacyGroup); got != want {
				t.Fatalf("secondary index group hash changed: got %s want %s", got, want)
			}
		}
	}
	for _, shard := range artifacts.EdgeShards {
		legacy := shard
		legacy.logicalContentHash = ""
		legacy.hashCanonical = false
		if got, want := edgeShardContentHash(shard), edgeShardContentHash(legacy); got != want {
			t.Fatalf("edge shard hash changed: got %s want %s", got, want)
		}
	}
	for _, page := range artifacts.EntityPages {
		legacy := page
		legacy.logicalContentHash = ""
		legacy.hashCanonical = false
		if got, want := entityPageContentHash(page), entityPageContentHash(legacy); got != want {
			t.Fatalf("entity page hash changed: got %s want %s", got, want)
		}
		preparedJSON, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		legacyJSON, err := json.Marshal(legacy)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(preparedJSON, legacyJSON) {
			t.Fatal("in-memory hash metadata changed serialized entity page")
		}
	}
}

func TestNonCanonicalEntityPageKeepsNormalizedHash(t *testing.T) {
	pages := buildEntityPagesFromEntities([]graph.Entity{{
		ID:     "host:a",
		Kind:   "host",
		Fields: graph.Fields{"attempts": 1},
	}}, 1)
	if len(pages) != 1 {
		t.Fatalf("pages = %d, want 1", len(pages))
	}
	if pages[0].hashCanonical {
		t.Fatal("page containing a non-JSON-normalized int was marked canonical")
	}
	want := indexContentHash(struct {
		Shard    string         `json:"shard"`
		Entities []graph.Entity `json:"entities"`
	}{
		Shard:    pages[0].Shard,
		Entities: normalizeGraphEntities(pages[0].Entities),
	})
	if got := entityPageContentHash(pages[0]); got != want {
		t.Fatalf("non-canonical page hash = %s, want %s", got, want)
	}
}

func BenchmarkBuildIndexArtifacts10K(b *testing.B) {
	entities := make([]graph.Entity, 10_000)
	for i := range entities {
		entities[i] = graph.Entity{
			ID:     fmt.Sprintf("host:%05d", i),
			Kind:   "host",
			Fields: graph.Fields{"state": "ready"},
		}
	}
	g, err := graph.FromSnapshot(graph.Snapshot{
		Version: 1,
		CITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"state": {Type: "string", Indexed: true}},
		}},
		Entities: entities,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		artifacts, err := buildIndexArtifactsWithDefinitions(g, 2, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkIndexBuildArtifacts = artifacts
	}
}

func TestQueuedIndexReservationAllowsProgressWithWaitingMaintenance(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "test")
	store.MaxMaintenanceBytes = 100
	queued := store.reserveIndexMemory(100)
	if queued == nil {
		t.Fatal("cannot reserve empty budget")
	}
	defer queued.release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, release, err := store.admitMaintenance(ctx, 100)
		if err == nil {
			release()
		}
		done <- err
	}()
	// A queued job must keep its memory while it acquires an execution slot,
	// even when another build is already waiting for that memory.
	if _, err := queued.activate(ctx); err != nil {
		t.Fatal(err)
	}
	if extra := store.reserveIndexMemory(1); extra != nil {
		extra.release()
		t.Fatal("active and queued work exceeded their shared memory budget")
	}
	queued.release()
	if err := <-done; err != nil {
		t.Fatalf("waiting maintenance did not resume: %v", err)
	}
}

func TestIndexQueueDoesNotReserveBehindSynchronousPredecessor(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "test")
	ctx, err := store.acquireAndBindWriterFence(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	predecessor := &commitIndexUpdate{}
	store.enqueueCommitIndexUpdate(ctx, "tenant-a", predecessor)
	next := &commitIndexUpdate{before: graph.New(), after: graph.New()}
	accepted := store.enqueueLocalIngestIndexUpdate(ctx, "tenant-a", next)
	// Always drain the predecessor, including on failure, so no background worker
	// outlives the test's data directory.
	if err := store.runCommitIndexUpdate(ctx, "tenant-a", predecessor); err != nil {
		t.Fatal(err)
	}
	if accepted {
		<-next.done
		t.Fatal("later index work reserved memory needed by its synchronous predecessor")
	}
}

func TestIndexUpdateCancellationPreservesPublicationOrder(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, middle, last := &commitIndexUpdate{}, &commitIndexUpdate{onlineRebuild: true}, &commitIndexUpdate{}
	for _, work := range []*commitIndexUpdate{first, middle, last} {
		store.enqueueCommitIndexUpdate(ctx, "tenant-a", work)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	for _, work := range []*commitIndexUpdate{{joined: first}, middle} {
		returned := make(chan error, 1)
		go func() { returned <- store.runCommitIndexUpdate(canceled, "tenant-a", work) }()
		select {
		case err := <-returned:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled waiter returned %v", err)
			}
		case <-ctx.Done():
			close(first.done)
			t.Fatal("index waiter ignored cancellation")
		}
	}
	lastDone := make(chan error, 1)
	go func() { lastDone <- store.runCommitIndexUpdate(ctx, "tenant-a", last) }()
	select {
	case err := <-lastDone:
		t.Fatalf("successor overtook unfinished predecessor: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := store.runCommitIndexUpdate(ctx, "tenant-a", first); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-lastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("cancellation stranded the publication chain")
	}
	store.indexUpdateMu.Lock()
	defer store.indexUpdateMu.Unlock()
	if len(store.indexUpdateTails) != 0 || len(store.indexUnreservedTails) != 0 || len(store.localIndexMaintenance) != 0 {
		t.Fatal("finished/canceled updates retained queue or maintenance state")
	}
}

func TestBackgroundIndexFailureReleasesResourcesAndAllowsRepair(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		for _, transient := range []bool{false, true} {
			t.Run(fmt.Sprintf("deferred=%v/transient=%v", deferred, transient), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				files, err := OpenFileStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer files.Close()
				fault := errors.New("persistent corrupt catalog")
				if transient {
					fault = ErrObjectStoreUnavailable
				}
				objects := &localIngestPublicationFault{ObjectStore: files, err: fault}
				store := NewTenantStore(objects, "test")
				store.WriteEntityRecords = false
				defer store.ShutdownTasks(context.Background())
				if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
					t.Fatal(err)
				}
				objects.key = store.indexCatalogKey("tenant-a")
				objects.failing.Store(true)
				endMaintenance := func() {}
				if deferred {
					endMaintenance = store.beginLocalIndexMaintenance(ctx, "tenant-a")
				}
				result, err := store.Ingest(ctx, "tenant-a", ingestEntityRequest("fault", "host:new"))
				endMaintenance()
				if err != nil {
					t.Fatal(err)
				}
				for {
					store.indexUpdateMu.Lock()
					active := store.activeIngestIndexUpdates
					store.indexUpdateMu.Unlock()
					if active == 0 {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("persistent index error stranded a background worker")
					case <-time.After(time.Millisecond):
					}
				}
				wantAttempts := int32(1)
				if transient {
					wantAttempts = 3
				}
				if got := objects.attempts.Load(); got != wantAttempts {
					t.Fatalf("publication attempts=%d want=%d", got, wantAttempts)
				}
				store.maintenance.mu.Lock()
				activeBytes, queuedBytes := store.maintenance.bytes, store.maintenance.queued
				store.maintenance.mu.Unlock()
				if activeBytes != 0 || queuedBytes != 0 || len(store.maintenance.builds) != 0 {
					t.Fatalf("leaked resources: active=%d queued=%d slots=%d", activeBytes, queuedBytes, len(store.maintenance.builds))
				}
				health, err := store.IndexHealthWithOptions(ctx, "tenant-a", IndexHealthOptions{})
				if err != nil || health.Status != "stale" || health.Updating || health.ManifestVersion != result.Version {
					t.Fatalf("failed work hidden from repair: health=%+v err=%v", health, err)
				}
				// Purge/recover/restore use this exclusive view. An index failure
				// must not retain a reader that prevents those repairs.
				release, err := store.lockReadViews(ctx, "tenant-a", true)
				if err != nil {
					t.Fatal(err)
				}
				release()
				if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:later", Kind: "host"}}}, CommitOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, err := store.RebuildIndexes(ctx, "tenant-a"); !errors.Is(err, fault) {
					t.Fatalf("rebuild should return its failure: %v", err)
				}
				objects.failing.Store(false)
				if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
					t.Fatal(err)
				}
				health, err = store.IndexHealth(ctx, "tenant-a")
				if err != nil || health.Status != "ready" || health.ManifestVersion <= result.Version {
					t.Fatalf("repair did not reach latest head: %+v err=%v", health, err)
				}
			})
		}
	}
}

type indexRetryPause struct {
	once           sync.Once
	paused, resume chan struct{}
	phase          string
}

func (*indexRetryPause) RecordManifestCASConflict(string)      {}
func (*indexRetryPause) RecordCommitTail(string, int)          {}
func (*indexRetryPause) RecordMaintenanceMemory(string, int64) {}
func (p *indexRetryPause) RecordMaintenancePhase(phase string, _ time.Duration) {
	if phase == p.phase || (p.phase == "" && phase == "index_retry") {
		p.once.Do(func() { close(p.paused); <-p.resume })
	}
}

type versionedCatalogPause struct {
	ObjectStore
	version        int64
	once           sync.Once
	paused, resume chan struct{}
}

func (s *versionedCatalogPause) UnwrapObjectStore() ObjectStore { return s.ObjectStore }

func (s *versionedCatalogPause) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.HasSuffix(key, "/indexes/catalog.parquet") {
		catalog, err := decodeParquetIndexCatalog(ctx, data)
		if err != nil {
			return ObjectMeta{}, err
		}
		if catalog.Version == s.version {
			s.once.Do(func() { close(s.paused) })
			select {
			case <-s.resume:
			case <-ctx.Done():
				return ObjectMeta{}, ctx.Err()
			}
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func TestDeferredIndexBudgetCoversConcurrentGrowth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	objects := &versionedCatalogPause{ObjectStore: files, version: 3, paused: make(chan struct{}), resume: make(chan struct{})}
	store := NewTenantStore(objects, "test")
	store.WriteEntityRecords = false
	store.MaxMaintenanceBytes = 1 << 20
	defer store.ShutdownTasks(context.Background())
	resumeCatalog := sync.OnceFunc(func() { close(objects.resume) })
	defer resumeCatalog()
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	pause := &indexRetryPause{phase: "memory_wait", paused: make(chan struct{}), resume: make(chan struct{})}
	resumeAdmission := sync.OnceFunc(func() { close(pause.resume) })
	defer resumeAdmission()
	store.backpressureObserver = pause
	endMaintenance := store.beginLocalIndexMaintenance(ctx, "tenant-a")
	_, err = store.Ingest(ctx, "tenant-a", ingestEntityRequest("small", "host:small"))
	endMaintenance()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-pause.paused:
	case <-ctx.Done():
		t.Fatal("deferred catch-up did not reach memory admission")
	}
	request := ingestEntityRequest("large", "host:large")
	request.Items[0].Entity.Fields["payload"] = strings.Repeat("x", 2<<20)
	if _, err := store.Ingest(ctx, "tenant-a", request); err != nil {
		t.Fatal(err)
	}
	resumeAdmission()
	select {
	case <-objects.paused:
	case <-ctx.Done():
		t.Fatal("catch-up did not build the larger head")
	}
	// A graph larger than the shared budget must run alone, including when
	// it was ingested while an earlier catch-up was entering the budget.
	waitCtx, cancelWait := context.WithTimeout(ctx, 50*time.Millisecond)
	_, release, err := store.admitMaintenance(waitCtx, 1)
	cancelWait()
	if err == nil {
		release()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("oversized catch-up allowed concurrent maintenance: %v", err)
	}
	resumeCatalog()
	for {
		store.indexUpdateMu.Lock()
		active := store.activeIngestIndexUpdates
		store.indexUpdateMu.Unlock()
		if active == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("catch-up did not finish")
		case <-time.After(time.Millisecond):
		}
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" || health.ManifestVersion != 3 {
		t.Fatalf("catch-up lost concurrent growth: %+v err=%v", health, err)
	}
	if err := store.ShutdownTasks(ctx); err != nil {
		t.Fatal(err)
	}
	_, release, err = store.admitMaintenance(ctx, store.MaxMaintenanceBytes)
	if err != nil {
		t.Fatalf("catch-up leaked maintenance budget: %v", err)
	}
	release()
}

func TestBackgroundIndexRetryReleasesViewAndCapturesFreshHead(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(fmt.Sprintf("purge=%v", purge), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			objects := &localIngestPublicationFault{ObjectStore: files, err: ErrObjectStoreUnavailable}
			store := NewTenantStore(objects, "test")
			store.WriteEntityRecords = false
			defer store.ShutdownTasks(context.Background())
			if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
			pause := &indexRetryPause{paused: make(chan struct{}), resume: make(chan struct{})}
			resume := sync.OnceFunc(func() { close(pause.resume) })
			defer resume()
			store.backpressureObserver = pause
			objects.key = store.indexCatalogKey("tenant-a")
			objects.failing.Store(true)
			if _, err := store.Ingest(ctx, "tenant-a", ingestEntityRequest("retry", "host:new")); err != nil {
				t.Fatal(err)
			}
			select {
			case <-pause.paused:
			case <-ctx.Done():
				t.Fatal("transient retry was not reached")
			}
			release, err := store.lockReadViews(ctx, "tenant-a", true)
			if err != nil {
				t.Fatal("retry retained read view: ", err)
			}
			release()
			_, releaseMemory, err := store.admitMaintenance(ctx, store.backgroundIndexByteLimit())
			if err != nil {
				t.Fatal("retry retained maintenance resources: ", err)
			}
			releaseMemory()
			objects.failing.Store(false)
			if purge {
				if _, err := store.PurgeTenant(ctx, "tenant-a", true); err != nil {
					t.Fatal("repair could not purge during backoff: ", err)
				}
				resume()
				for {
					store.indexUpdateMu.Lock()
					active := store.activeIngestIndexUpdates
					store.indexUpdateMu.Unlock()
					if active == 0 {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("retry did not stop after tenant purge")
					case <-time.After(time.Millisecond):
					}
				}
				if _, err := store.GetIndexCatalog(ctx, "tenant-a"); !errors.Is(err, ErrNotFound) {
					t.Fatalf("retry resurrected a purged catalog: %v", err)
				}
				return
			}
			later, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:later", Kind: "host"}}}, CommitOptions{})
			if err != nil {
				t.Fatal(err)
			}
			resume()
			for {
				catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
				if err == nil && catalog.Version == later.Version {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("retry did not capture latest head: catalog=%d latest=%d err=%v", catalog.Version, later.Version, err)
				case <-time.After(time.Millisecond):
				}
			}
			health, err := store.IndexHealth(ctx, "tenant-a")
			if err != nil || health.Status != "ready" {
				t.Fatalf("retry lost intervening data: %+v err=%v", health, err)
			}
		})
	}
}
