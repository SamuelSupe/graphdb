package storage

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestEntityPagePackingMergesSmallPages(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	store.WriteEntityRecords = false
	entities := entitiesForDistinctShards("host", "host", 8, entityShardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if len(catalog.EntityPages) < 2 {
		t.Fatalf("test expected multiple logical pages, catalog=%#v", catalog.EntityPages)
	}
	keys := entityPageObjectKeys(catalog.EntityPages)
	if len(keys) != 1 || !strings.Contains(keys[0], "/entities/pages/packs/") {
		t.Fatalf("entity pages should share one pack object, keys=%#v pages=%#v", keys, catalog.EntityPages)
	}
	objects, err := store.Objects.List(ctx, "test/tenants/tenant-a/indexes/parquet/versions/v1/entities/pages/")
	if err != nil {
		t.Fatalf("list entity pages: %v", err)
	}
	if parquetObjects := countParquetObjects(objects); parquetObjects != 1 {
		t.Fatalf("entity page parquet object count = %d, want 1; objects=%#v", parquetObjects, objects)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	for _, entity := range entities {
		got, ok, err := lookup.GetEntity(ctx, entity.ID, nil)
		if err != nil || !ok || got.ID != entity.ID {
			t.Fatalf("entity lookup %s got=%#v ok=%v err=%v", entity.ID, got, ok, err)
		}
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func TestEntityScanReadsAndFiltersPackedPageOncePerRequest(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	store.WriteEntityRecords = false
	entities := entitiesForDistinctShards("host", "packed-scan", 8, entityShardID)
	entities[0].Kind = "system"
	entities[1].Kind = "system"
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	keys := entityPageObjectKeys(catalog.EntityPages)
	if len(catalog.EntityPages) != len(entities) || len(keys) != 1 {
		t.Fatalf("test requires one packed object across logical pages: pages=%d keys=%#v", len(catalog.EntityPages), keys)
	}

	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects
	recorder := installStorageSpanRecorder(t)
	result, err := store.ListEntitiesFromCatalog(ctx, "tenant-a", catalog, EntityScanOptions{Kind: "system", Limit: 500})
	if err != nil || len(result.Entities) != 2 {
		t.Fatalf("list entities result=%#v err=%v", result, err)
	}
	if got := objects.GetWithMetaCount(keys[0]); got != 1 {
		t.Fatalf("packed entity object reads = %d, want one request-local physical read", got)
	}

	span := requireStorageSpan(t, recorder.Ended(), "graphdb.storage.scan.entities.pages")
	assertStorageSpanAttribute(t, span, "graphdb.scan.unique_objects", int64(1))
	assertStorageSpanAttribute(t, span, "graphdb.scan.object_loads", int64(1))
	assertStorageSpanAttribute(t, span, "graphdb.scan.candidate_object_scans", int64(1))
	assertStorageSpanAttribute(t, span, "graphdb.scan.candidate_filter_requests", int64(len(catalog.EntityPages)))
	assertStorageSpanAttribute(t, span, "graphdb.scan.candidate_scan_reuses", int64(len(catalog.EntityPages)-1))
	assertStorageSpanAttribute(t, span, "graphdb.scan.parquet_decodes", int64(2))
	requireStorageSpan(t, recorder.Ended(), "graphdb.storage.scan.entities.candidate_filter")
	requireStorageSpan(t, recorder.Ended(), "graphdb.storage.scan.entities.decode_page")
	for _, entity := range entities[:2] {
		spec := requireEntityPageSpec(t, catalog, entityShardID(entity.ID))
		if _, _, ok := store.cachedEntityPage("tenant-a", catalog.Version, keys[0], spec.ContentHash, spec.SchemaHash); !ok {
			t.Fatalf("decoded entity page for %q was not cached", entity.ID)
		}
	}
}

func TestEdgeShardPackingMergesSmallShards(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	services := entitiesForDistinctShards("service", "service", 8, edgeShardID)
	hosts := make([]graph.Entity, 0, len(services))
	edges := make([]graph.Edge, 0, len(services))
	for i, service := range services {
		host := graph.Entity{ID: fmt.Sprintf("host:%02d", i), Kind: "host"}
		hosts = append(hosts, host)
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:%02d", i),
			Type: "runs_on",
			From: service.ID,
			To:   host.ID,
			Fields: graph.Fields{
				"ordinal": float64(i),
			},
			FieldSources: map[string]graph.FieldSource{
				"ordinal": {Source: "agent", Priority: 100, Version: 1},
			},
			Source:     "agent",
			ExternalID: fmt.Sprintf("rel-%02d", i),
			Sources: []graph.EdgeSource{{
				Source: "agent", ExternalID: fmt.Sprintf("rel-%02d", i),
				EdgeID: fmt.Sprintf("collector-%02d", i), Priority: 100,
			}},
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
		}},
		UpsertEntities: append(services, hosts...),
		UpsertEdges:    edges,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load committed graph: %v", err)
	}
	committedEdges := make([]graph.Edge, 0, len(edges))
	for _, edge := range loaded.Edges {
		if edge.Type == "runs_on" {
			committedEdges = append(committedEdges, edge)
		}
	}
	if len(committedEdges) != len(edges) {
		t.Fatalf("committed edges=%d, want %d", len(committedEdges), len(edges))
	}
	shards := relationEdgeShards(catalog, "runs_on")
	if len(shards) < 2 {
		t.Fatalf("test expected multiple logical edge shards, catalog=%#v", catalog.EdgeShards)
	}
	keys := edgeShardObjectKeys(shards)
	if len(keys) != 1 || !strings.Contains(keys[0], "/edges/runs_on/packs/") {
		t.Fatalf("edge shards should share one pack object, keys=%#v shards=%#v", keys, shards)
	}
	objects, err := store.Objects.List(ctx, "test/tenants/tenant-a/indexes/parquet/versions/v1/edges/runs_on/")
	if err != nil {
		t.Fatalf("list edge shards: %v", err)
	}
	if parquetObjects := countParquetObjects(objects); parquetObjects != 1 {
		t.Fatalf("edge shard parquet object count = %d, want 1; objects=%#v", parquetObjects, objects)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	for _, edge := range committedEdges {
		got, ok, err := lookup.OutEdges(ctx, edge.From, map[string]struct{}{"runs_on": {}})
		if err != nil || !ok || len(got) != 1 || got[0].From != edge.From ||
			!reflect.DeepEqual(got[0].Fields, edge.Fields) ||
			!reflect.DeepEqual(got[0].FieldSources, edge.FieldSources) ||
			got[0].Source != edge.Source || got[0].ExternalID != edge.ExternalID ||
			!reflect.DeepEqual(got[0].Sources, edge.Sources) {
			t.Fatalf("edge lookup from=%s got=%#v ok=%v err=%v", edge.From, got, ok, err)
		}
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func TestReverseEdgeShardPackingPreservesLogicalShardsAndMetadata(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	services := entitiesForDistinctShards("service", "reverse-service", 8, edgeShardID)
	targets := entitiesForDistinctShards("host", "reverse-target", len(services), edgeShardID)
	edges := make([]graph.Edge, 0, len(services))
	entities := make([]graph.Entity, 0, len(services)*2)
	entities = append(entities, services...)
	entities = append(entities, targets...)
	for i := range services {
		edges = append(edges, graph.Edge{
			ID: fmt.Sprintf("edge:reverse-pack:%02d", i), Type: "runs_on",
			From: services[i].ID, To: targets[i].ID,
			Fields: graph.Fields{"ordinal": float64(i)},
			FieldSources: map[string]graph.FieldSource{
				"ordinal": {Source: "agent", Priority: 100, Version: 7},
			},
			Source:     "agent",
			ExternalID: fmt.Sprintf("reverse-rel-%02d", i),
			Sources: []graph.EdgeSource{{
				Source: "agent", ExternalID: fmt.Sprintf("reverse-rel-%02d", i),
				EdgeID: fmt.Sprintf("reverse-collector-%02d", i), Priority: 100,
			}},
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
		}},
		UpsertEntities: entities,
		UpsertEdges:    edges,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load committed graph: %v", err)
	}
	committedEdges := make([]graph.Edge, 0, len(edges))
	for _, edge := range loaded.Edges {
		if edge.Type == "runs_on" {
			committedEdges = append(committedEdges, edge)
		}
	}
	if len(committedEdges) != len(edges) {
		t.Fatalf("committed edges=%d, want %d", len(committedEdges), len(edges))
	}
	reverse, err := store.GetReverseIndexCatalog(ctx, "tenant-a", catalog.Version)
	if err != nil {
		t.Fatalf("load reverse catalog: %v", err)
	}
	reverseShards := make([]EdgeShard, 0, len(reverse.EdgeShards))
	for _, spec := range reverse.EdgeShards {
		if spec.RelationType == "runs_on" {
			reverseShards = append(reverseShards, spec)
		}
	}
	if len(reverseShards) < 2 {
		t.Fatalf("test expected multiple reverse logical shards, catalog=%#v", reverse.EdgeShards)
	}
	reverseKeys := edgeShardObjectKeys(reverseShards)
	if len(reverseKeys) != 1 || !strings.Contains(reverseKeys[0], "pack_") {
		t.Fatalf("reverse shards should share one physical pack, keys=%#v shards=%#v", reverseKeys, reverseShards)
	}

	lookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a", Version: catalog.Version,
		Catalog: catalog, ReverseCatalog: &reverse,
	}
	for _, edge := range committedEdges {
		incoming, ok, err := lookup.InEdges(ctx, edge.To, map[string]struct{}{"runs_on": {}})
		if err != nil || !ok || len(incoming) != 1 || incoming[0].ID != edge.ID ||
			incoming[0].From != edge.From || incoming[0].To != edge.To ||
			!reflect.DeepEqual(incoming[0].Fields, edge.Fields) ||
			!reflect.DeepEqual(incoming[0].FieldSources, edge.FieldSources) ||
			incoming[0].Source != edge.Source || incoming[0].ExternalID != edge.ExternalID ||
			!reflect.DeepEqual(incoming[0].Sources, edge.Sources) {
			t.Fatalf("reverse edge lookup to=%s got=%#v ok=%v err=%v want=%#v", edge.To, incoming, ok, err, edge)
		}
		outgoing, ok, err := lookup.OutEdges(ctx, edge.From, map[string]struct{}{"runs_on": {}})
		if err != nil || !ok || len(outgoing) != 1 || outgoing[0].ID != edge.ID || outgoing[0].To != edge.To {
			t.Fatalf("forward edge lookup from=%s got=%#v ok=%v err=%v", edge.From, outgoing, ok, err)
		}
	}
}

func TestReverseEdgeShardPackingSplitsColdReadsAcrossPhysicalPacks(t *testing.T) {
	ctx := context.Background()
	const (
		reversePackEdgeLimit = 128
		largeShardEdges      = reversePackEdgeLimit + 1
		smallShardEdges      = 16
	)
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	targets := entitiesForDistinctShards("host", "reverse-multipack-target", 9, edgeShardID)
	entities := append([]graph.Entity(nil), targets...)
	edges := make([]graph.Edge, 0, largeShardEdges+(len(targets)-1)*smallShardEdges)
	for targetIndex, target := range targets {
		edgeCount := smallShardEdges
		if targetIndex == 0 {
			edgeCount = largeShardEdges
		}
		for edgeIndex := 0; edgeIndex < edgeCount; edgeIndex++ {
			source := graph.Entity{
				ID:   fmt.Sprintf("reverse-multipack-source:%02d:%03d", targetIndex, edgeIndex),
				Kind: "service",
			}
			entities = append(entities, source)
			edges = append(edges, graph.Edge{
				ID:   fmt.Sprintf("edge:reverse-multipack:%02d:%03d", targetIndex, edgeIndex),
				Type: "runs_on", From: source.ID, To: target.ID,
				Fields: graph.Fields{
					"ordinal":      float64(edgeIndex),
					"target_index": float64(targetIndex),
				},
				FieldSources: map[string]graph.FieldSource{
					"ordinal":      {Source: "agent", Priority: 100, Version: 11},
					"target_index": {Source: "agent", Priority: 100, Version: 11},
				},
				Source:     "agent",
				ExternalID: fmt.Sprintf("reverse-multipack-rel:%02d:%03d", targetIndex, edgeIndex),
				Sources: []graph.EdgeSource{{
					Source: "agent", ExternalID: fmt.Sprintf("reverse-multipack-rel:%02d:%03d", targetIndex, edgeIndex),
					EdgeID: fmt.Sprintf("reverse-multipack-collector:%02d:%03d", targetIndex, edgeIndex), Priority: 100,
				}},
				ExistenceSource: &graph.FieldSource{Source: "agent", Priority: 100, Version: 11},
			})
		}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
		}},
		UpsertEntities: entities,
		UpsertEdges:    edges,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load committed graph: %v", err)
	}
	expectedByTarget := make(map[string]map[string]graph.Edge, len(targets))
	for _, target := range targets {
		expectedByTarget[target.ID] = make(map[string]graph.Edge)
	}
	for _, edge := range loaded.Edges {
		if edge.Type == "runs_on" {
			expectedByTarget[edge.To][edge.ID] = edge
		}
	}
	if got := len(expectedByTarget); got != len(targets) {
		t.Fatalf("expected target buckets=%d, got %d", len(targets), got)
	}
	if got := len(expectedByTarget[targets[0].ID]); got != largeShardEdges {
		t.Fatalf("large target edges=%d, want %d", got, largeShardEdges)
	}
	for _, target := range targets[1:] {
		if got := len(expectedByTarget[target.ID]); got != smallShardEdges {
			t.Fatalf("target %s edges=%d, want %d", target.ID, got, smallShardEdges)
		}
	}

	reverse, err := store.GetReverseIndexCatalog(ctx, "tenant-a", catalog.Version)
	if err != nil {
		t.Fatalf("load reverse catalog: %v", err)
	}
	physicalSpecs := map[string][]EdgeShard{}
	for _, spec := range reverse.EdgeShards {
		if spec.RelationType != "runs_on" {
			continue
		}
		if want := len(expectedByTarget[findTargetForShard(t, targets, spec.Shard)]); want != spec.EdgeCount {
			t.Fatalf("reverse shard %s edge count=%d, want %d", spec.Shard, spec.EdgeCount, want)
		}
		key := requireAnyIndexObjectKey(t, spec.Objects)
		physicalSpecs[key] = append(physicalSpecs[key], spec)
	}
	if len(physicalSpecs) < 2 {
		t.Fatalf("expected reverse index to span multiple physical packs, keys=%#v", physicalSpecs)
	}

	for key, specs := range physicalSpecs {
		logicalRows := 0
		for _, spec := range specs {
			logicalRows += spec.EdgeCount
		}
		if len(specs) > 1 && logicalRows > reversePackEdgeLimit {
			t.Fatalf("physical reverse pack %s merged %d logical shards/%d edges, want at most %d edges", key, len(specs), logicalRows, reversePackEdgeLimit)
		}
		data, err := store.Objects.Get(ctx, key)
		if err != nil {
			t.Fatalf("read reverse physical object %s: %v", key, err)
		}
		physicalID := strings.TrimSuffix(path.Base(key), ".parquet")
		decoded, err := decodeParquetEdgeShard(ctx, data, "tenant-a", "runs_on", physicalID, catalog.Version)
		if err != nil {
			t.Fatalf("decode reverse physical object %s: %v", key, err)
		}
		if len(decoded.Edges) != logicalRows {
			t.Fatalf("physical reverse object %s edges=%d, want %d", key, len(decoded.Edges), logicalRows)
		}
	}

	lookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a", Version: catalog.Version,
		Catalog: catalog, ReverseCatalog: &reverse,
	}
	seen := make(map[string]struct{}, len(edges))
	for _, target := range targets {
		incoming, ok, err := lookup.InEdges(ctx, target.ID, map[string]struct{}{"runs_on": {}})
		if err != nil || !ok {
			t.Fatalf("reverse lookup to=%s got=%#v ok=%v err=%v", target.ID, incoming, ok, err)
		}
		expected := expectedByTarget[target.ID]
		if len(incoming) != len(expected) {
			t.Fatalf("reverse lookup to=%s edges=%d, want %d", target.ID, len(incoming), len(expected))
		}
		for _, edge := range incoming {
			if edge.To != target.ID {
				t.Fatalf("reverse lookup to=%s returned cross-shard edge %#v", target.ID, edge)
			}
			want, exists := expected[edge.ID]
			if !exists {
				t.Fatalf("reverse lookup to=%s returned unexpected edge %#v", target.ID, edge)
			}
			if _, duplicate := seen[edge.ID]; duplicate {
				t.Fatalf("reverse lookup returned duplicate edge %s", edge.ID)
			}
			seen[edge.ID] = struct{}{}
			assertPackedEdgeMetadata(t, edge, want)
		}
	}
	if len(seen) != len(edges) {
		t.Fatalf("reverse lookups returned %d unique edges, want %d", len(seen), len(edges))
	}
}

func findTargetForShard(t *testing.T, targets []graph.Entity, shard string) string {
	t.Helper()
	for _, target := range targets {
		if edgeShardID(target.ID) == shard {
			return target.ID
		}
	}
	t.Fatalf("missing target for reverse shard %s", shard)
	return ""
}

func TestPackedReverseEdgeShardDecoderStreamsRowsAndReusesHash(t *testing.T) {
	ctx := context.Background()
	pack, expectedByID, logicalShards := packedReverseEdgeShardFixture(t)
	data, err := marshalParquetEdgeShard(ctx, pack)
	if err != nil {
		t.Fatalf("marshal packed reverse edge shard: %v", err)
	}
	data = rewriteEdgeShardWithRowGroups(t, ctx, data, 64)
	full, err := decodeParquetEdgeShard(ctx, data, pack.TenantID, pack.RelationType, pack.Shard, pack.Version)
	if err != nil {
		t.Fatalf("decode physical pack: %v", err)
	}
	if len(full.Edges) != len(expectedByID) {
		t.Fatalf("physical pack edges=%d, want %d", len(full.Edges), len(expectedByID))
	}
	for _, edge := range full.Edges {
		want, ok := expectedByID[edge.ID]
		if !ok {
			t.Fatalf("physical pack returned unexpected edge %#v", edge)
		}
		assertPackedEdgeMetadata(t, edge, want)
	}
	for _, shardID := range logicalShards {
		decoded, err := decodeParquetEdgeShard(ctx, data, pack.TenantID, pack.RelationType, shardID, pack.Version)
		if err != nil {
			t.Fatalf("decode logical shard %s: %v", shardID, err)
		}
		wantCount := 0
		for _, edge := range expectedByID {
			if edgeShardID(edge.To) == shardID {
				wantCount++
			}
		}
		if len(decoded.Edges) != wantCount {
			t.Fatalf("logical shard %s edges=%d, want %d", shardID, len(decoded.Edges), wantCount)
		}
		for _, edge := range decoded.Edges {
			want := expectedByID[edge.ID]
			if edgeShardID(edge.To) != shardID {
				t.Fatalf("logical shard %s returned edge for %s: %#v", shardID, edge.To, edge)
			}
			assertPackedEdgeMetadata(t, edge, want)
		}
	}

	objects := newCountingIndexStore(NewMemoryStore())
	objects.TrackIndexes = true
	store := NewTenantStore(objects, "test")
	key := store.parquetEdgeShardPackVersionKey("tenant-a", pack.Version, pack.RelationType, pack.Shard)
	if err := store.putParquetEdgeShardObject(ctx, key, pack.TenantID, pack, false); err != nil {
		t.Fatalf("write packed edge object: %v", err)
	}
	objects.ResetPutCounts()
	if err := store.putParquetEdgeShardObject(ctx, key, pack.TenantID, pack, false); err != nil {
		t.Fatalf("reuse packed edge object: %v", err)
	}
	if writes := objects.PutCount("/indexes/parquet/"); writes != 1 {
		t.Fatalf("hash-equivalent pack conflict writes=%d, want one conditional attempt and no replacement", writes)
	}
}

func packedReverseEdgeShardFixture(t *testing.T) (EdgeShardData, map[string]graph.Edge, []string) {
	t.Helper()
	targets := entitiesForDistinctShards("host", "decode-target", 8, edgeShardID)
	shards := make([]EdgeShardData, 0, len(targets))
	expectedByID := make(map[string]graph.Edge, len(targets)*24)
	for targetIndex, target := range targets {
		shard := EdgeShardData{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      "tenant-a",
			RelationType:  "runs_on",
			Shard:         edgeShardID(target.ID),
			Version:       9,
			Edges:         make([]graph.Edge, 0, 24),
			hashCanonical: true,
		}
		for edgeIndex := 0; edgeIndex < 24; edgeIndex++ {
			edge := graph.Edge{
				ID:   fmt.Sprintf("edge:decode:%02d:%03d", targetIndex, edgeIndex),
				Type: "runs_on", From: fmt.Sprintf("service:%02d:%03d", targetIndex, edgeIndex), To: target.ID,
				Fields: graph.Fields{
					"ordinal": float64(edgeIndex),
					"escaped": fmt.Sprintf("<value-%d>&", edgeIndex),
				},
				FieldSources: map[string]graph.FieldSource{
					"ordinal": {Source: "agent", Priority: 100, Version: 9},
				},
				Source:     "agent",
				ExternalID: fmt.Sprintf("external:%02d:%03d", targetIndex, edgeIndex),
				Sources: []graph.EdgeSource{{
					Source: "agent", ExternalID: fmt.Sprintf("external:%02d:%03d", targetIndex, edgeIndex),
					EdgeID: fmt.Sprintf("collector:%02d:%03d", targetIndex, edgeIndex), Priority: 100,
				}},
				ExistenceSource: &graph.FieldSource{Source: "agent", Priority: 100, Version: 9},
				Version:         9,
			}
			shard.Edges = append(shard.Edges, edge)
			expectedByID[edge.ID] = edge
		}
		shards = append(shards, shard)
	}
	groups := edgeShardDataPackGroups(shards)
	if len(groups) != 1 || len(groups[0].Shards) != len(shards) {
		t.Fatalf("fixture did not form one multi-shard pack: %#v", groups)
	}
	pack := mergeEdgeShardPack(groups[0])
	pack.reverse = true
	return pack, expectedByID, func() []string {
		out := make([]string, 0, len(shards))
		for _, shard := range shards {
			out = append(out, shard.Shard)
		}
		return out
	}()
}

func rewriteEdgeShardWithRowGroups(t *testing.T, ctx context.Context, data []byte, rowGroupSize int64) []byte {
	t.Helper()
	table, release, err := readParquetTable(ctx, data)
	if err != nil {
		t.Fatalf("read edge table for row groups: %v", err)
	}
	defer func() {
		table.Release()
		release()
	}()
	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, rowGroupSize, writerProps, arrowProps); err != nil {
		t.Fatalf("rewrite edge table with row groups: %v", err)
	}
	return buf.Bytes()
}

func assertPackedEdgeMetadata(t *testing.T, got graph.Edge, want graph.Edge) {
	t.Helper()
	if got.ID != want.ID || got.Type != want.Type || got.From != want.From || got.To != want.To {
		t.Fatalf("edge endpoints got=%#v want=%#v", got, want)
	}
	if !reflect.DeepEqual(got.Fields, want.Fields) ||
		!reflect.DeepEqual(got.FieldSources, want.FieldSources) ||
		got.Source != want.Source || got.ExternalID != want.ExternalID ||
		!reflect.DeepEqual(got.Sources, want.Sources) ||
		!reflect.DeepEqual(got.ExistenceSource, want.ExistenceSource) {
		t.Fatalf("edge metadata got=%#v want=%#v", got, want)
	}
}

func entitiesForDistinctShards(kind string, prefix string, count int, shardFn func(string) string) []graph.Entity {
	seen := map[string]struct{}{}
	out := make([]graph.Entity, 0, count)
	for i := 0; len(out) < count; i++ {
		id := fmt.Sprintf("%s:%04d", prefix, i)
		shard := shardFn(id)
		if _, ok := seen[shard]; ok {
			continue
		}
		seen[shard] = struct{}{}
		out = append(out, graph.Entity{ID: id, Kind: kind})
	}
	return out
}

func entityPageObjectKeys(pages []EntityPageSpec) []string {
	objects := make([]IndexObject, 0, len(pages))
	for _, page := range pages {
		objects = append(objects, page.Objects...)
	}
	return uniqueObjectKeys(objects)
}

func relationEdgeShards(catalog IndexCatalog, relationType string) []EdgeShard {
	out := make([]EdgeShard, 0)
	for _, shard := range catalog.EdgeShards {
		if shard.RelationType == relationType {
			out = append(out, shard)
		}
	}
	return out
}

func edgeShardObjectKeys(shards []EdgeShard) []string {
	objects := make([]IndexObject, 0, len(shards))
	for _, shard := range shards {
		objects = append(objects, shard.Objects...)
	}
	return uniqueObjectKeys(objects)
}

func countParquetObjects(objects []ObjectInfo) int {
	count := 0
	for _, object := range objects {
		if strings.HasSuffix(object.Key, ".parquet") {
			count++
		}
	}
	return count
}
