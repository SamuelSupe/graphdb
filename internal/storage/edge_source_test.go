package storage

import (
	"context"
	"errors"
	"testing"

	"graphdb/internal/graph"
)

func TestIngestEdgeSuppressionIsNotFailureOrDeadLetter(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	seedManualRunsOnEdge(t, ctx, store, "tenant-a", "manual-edge")

	request := IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "edge-field-1",
		IdempotencyKey: "edge-field-1",
		Items: []IngestItem{{
			ExternalID: "agent-edge",
			Edge: &graph.Edge{
				ID: "agent-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
				Confidence: 1, Fields: graph.Fields{"note": "collector"},
			},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	conflict := result.Conflicts[0]
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if conflict.ResourceType != "edge" || conflict.CanonicalID != edgeID || conflict.IncomingID != "agent-edge" || conflict.IncomingPriority != 100 {
		t.Fatalf("conflict = %#v", conflict)
	}
	deadletters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(deadletters) != 0 {
		t.Fatalf("deadletters = %#v", deadletters)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed.Skipped || replayed.Suppressed != 1 {
		t.Fatalf("replayed = %#v", replayed)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	edge := g.Edges[edgeID]
	if edge.Fields["note"] != "manual" || !graph.EdgeSourceAliasMatches(edge, "agent-edge") {
		t.Fatalf("edge = %#v", edge)
	}
}

func TestIngestEdgeSuppressionReportsItemLocation(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	seedManualRunsOnEdge(t, ctx, store, "tenant-a", "manual-edge")

	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "edge-conflict-location",
		Items: []IngestItem{
			{ExternalID: "host-extra", Entity: &graph.Entity{ID: "host:extra", Kind: "host"}},
			{
				ExternalID: "agent-edge",
				Edge: &graph.Edge{
					ID: "agent-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
					Confidence: 1, Fields: graph.Fields{"note": "collector"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	conflict := result.Conflicts[0]
	if conflict.Index != 1 || conflict.ExternalID != "agent-edge" || conflict.IncomingID != "agent-edge" {
		t.Fatalf("conflict location = %#v", conflict)
	}
}

func TestIngestEdgeWithoutIDUsesExternalIDAlias(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	if _, err := store.Commit(ctx, "tenant-a", edgeEndpointMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed endpoints: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "edge-no-id",
		IdempotencyKey: "edge-no-id",
		Items: []IngestItem{{
			ExternalID: "collector-edge-a",
			Edge: &graph.Edge{
				Type: "runs_on", From: "service:api", To: "host:app-01",
				Fields: graph.Fields{"port": 8080},
			},
		}},
	})
	if err != nil {
		t.Fatalf("ingest edge without id: %v", err)
	}
	if result.Failed != 0 || result.Applied != 1 {
		t.Fatalf("result = %#v", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge, ok := g.Edges[edgeID]
	if !ok || edge.ID != edgeID || edge.Source != "agent" || edge.SourceRank != 100 {
		t.Fatalf("edge = %#v", edge)
	}
	if !graph.EdgeSourceAliasMatches(edge, "collector-edge-a") {
		t.Fatalf("source aliases = %#v", edge.Sources)
	}
}

func TestIngestDeleteEdgeSuppressionIsNotFailureOrDeadLetter(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	seedManualRunsOnEdge(t, ctx, store, "tenant-a", "manual-edge")

	request := IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "edge-delete-1",
		IdempotencyKey: "edge-delete-1",
		Items: []IngestItem{{
			ExternalID: "manual-edge",
			DeleteEdge: &graph.EdgeDeleteRequest{
				ID: "manual-edge", Confidence: 1, Reason: "collector no longer sees relation",
			},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest delete: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.Conflicts[0].Field != "__existence__" || result.Conflicts[0].ResourceType != "edge" {
		t.Fatalf("conflict = %#v", result.Conflicts[0])
	}
	deadletters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(deadletters) != 0 {
		t.Fatalf("deadletters = %#v", deadletters)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if _, ok := g.Edges[edgeID]; !ok {
		t.Fatal("suppressed delete removed edge")
	}
}

func TestIngestDeleteEdgeInheritsItemExternalIDAlias(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	seedAgentRunsOnEdge(t, ctx, store, "tenant-a", "agent-edge")

	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:         "manual",
		CollectorID:    "operator",
		BatchID:        "delete-edge-external-id",
		IdempotencyKey: "delete-edge-external-id",
		Items: []IngestItem{{
			ExternalID: "agent-edge",
			DeleteEdge: &graph.EdgeDeleteRequest{
				Reason: "operator removed relation",
			},
		}},
	})
	if err != nil {
		t.Fatalf("ingest delete by external id: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 0 || result.Applied != 1 {
		t.Fatalf("result = %#v", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if _, ok := g.Edges[edgeID]; ok {
		t.Fatal("manual delete by item external_id did not remove edge")
	}
}

func TestSourcePolicyRewritesEdgeDeletePriority(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	seedAgentRunsOnEdge(t, ctx, store, "tenant-a", "agent-edge")

	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:         "manual",
		CollectorID:    "operator",
		BatchID:        "manual-delete",
		IdempotencyKey: "manual-delete",
		Items: []IngestItem{{
			DeleteEdge: &graph.EdgeDeleteRequest{Type: "runs_on", From: "service:api", To: "host:app-01"},
		}},
	})
	if err != nil {
		t.Fatalf("manual delete ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 0 {
		t.Fatalf("result = %#v", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if _, ok := g.Edges[edgeID]; ok {
		t.Fatal("manual delete did not remove lower priority edge")
	}
}

func TestCompactPreservesCanonicalEdgeSourceMetadata(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	seedManualRunsOnEdge(t, ctx, store, "tenant-a", "manual-edge")
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load compacted: %v", err)
	}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge := g.Edges[edgeID]
	if edge.ID != edgeID || edge.ExistenceSource == nil || edge.ExistenceSource.Source != "manual" {
		t.Fatalf("edge existence source = %#v", edge)
	}
	if edge.FieldSources["note"].Source != "manual" || !graph.EdgeSourceAliasMatches(edge, "manual-edge") {
		t.Fatalf("edge sources = %#v", edge)
	}
}

func TestIncrementalEdgeShardUsesCanonicalIDWithoutDuplicates(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", edgeEndpointMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{{
		ID: "collector-a", Type: "runs_on", From: "service:api", To: "host:app-01",
		Source: "agent-a", ExternalID: "raw-a",
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("edge a: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{{
		ID: "collector-b", Type: "runs_on", From: "service:api", To: "host:app-01",
		Source: "agent-b", ExternalID: "raw-b", Fields: graph.Fields{"protocol": "http"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("edge b: %v", err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	lookup := PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	edges, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}})
	if err != nil || !ok {
		t.Fatalf("lookup ok=%v err=%v", ok, err)
	}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if len(edges) != 1 || edges[0].ID != edgeID || !graph.EdgeSourceAliasMatches(edges[0], "collector-b") {
		t.Fatalf("edges = %#v", edges)
	}
}

func TestIncrementalEdgeShardDeleteRequestTrimsAlias(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	putEdgePolicy(t, ctx, store, "tenant-a")
	seedAgentRunsOnEdge(t, ctx, store, "tenant-a", "agent-edge")
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	shardKey := requireIndexObjectKey(t, requireEdgeShardSpec(t, catalog, "runs_on", edgeShardID("service:api")).Objects, "shard")
	if _, err := store.Objects.Get(ctx, shardKey); err != nil {
		t.Fatalf("edge shard before delete: %v", err)
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{DeleteEdgeRequests: []graph.EdgeDeleteRequest{{
		ID: " agent-edge ", Source: "agent",
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("delete edge by spaced alias: %v", err)
	}
	if len(result.Suppressed) != 0 || len(result.IndexWarnings) != 0 {
		t.Fatalf("result = %#v", result)
	}
	if _, err := store.Objects.Get(ctx, shardKey); err != nil {
		t.Fatalf("edge shard should remain until GC: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("index catalog: %v", err)
	}
	for _, shard := range catalog.EdgeShards {
		if shard.RelationType == "runs_on" && shard.Shard == edgeShardID("service:api") {
			t.Fatalf("deleted edge shard remains in catalog: %#v", shard)
		}
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if edge, ok := g.Edges[edgeID]; ok {
		t.Fatalf("edge was not deleted: %#v", edge)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if _, err := store.Objects.Get(ctx, shardKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("edge shard after GC err=%v, want ErrNotFound", err)
	}
}

func putEdgePolicy(t *testing.T, ctx context.Context, store *TenantStore, tenantID string) {
	t.Helper()
	_, err := store.PutSourcePolicy(ctx, tenantID, graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	})
	if err != nil {
		t.Fatalf("put policy: %v", err)
	}
}

func seedManualRunsOnEdge(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, edgeID string) {
	t.Helper()
	if _, err := store.Commit(ctx, tenantID, graph.Mutations{
		UpsertEntities: edgeEndpointMutations().UpsertEntities,
		UpsertEdges: []graph.Edge{{
			ID: edgeID, Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "manual", Confidence: 0.9, Fields: graph.Fields{"note": "manual"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed manual edge: %v", err)
	}
}

func seedAgentRunsOnEdge(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, edgeID string) {
	t.Helper()
	if _, err := store.Commit(ctx, tenantID, graph.Mutations{
		UpsertEntities: edgeEndpointMutations().UpsertEntities,
		UpsertEdges: []graph.Edge{{
			ID: edgeID, Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", Confidence: 0.9, Fields: graph.Fields{"note": "agent"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed agent edge: %v", err)
	}
}

func edgeEndpointMutations() graph.Mutations {
	return graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "service:api", Kind: "service"},
		{ID: "host:app-01", Kind: "host"},
	}}
}
