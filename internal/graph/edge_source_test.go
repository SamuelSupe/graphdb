package graph

import (
	"reflect"
	"testing"
	"time"
)

func TestCanonicalEdgeIDUsesStableTripleHash(t *testing.T) {
	got := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	want := "edge:4b2a5fd74bd5ed2dd3747821c5920685"
	if got != want {
		t.Fatalf("canonical edge id = %q, want %q", got, want)
	}
	if got != CanonicalEdgeIDParts(" runs_on ", " service:api ", " host:app-01 ") {
		t.Fatalf("canonical edge id should trim identity parts")
	}
}

func TestSameTripleEdgesMergeToCanonicalEdge(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	if err := g.ApplyCommit(Commit{
		ID:      "first",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "collector-edge-a", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent-a", ExternalID: "raw-a", SourceRank: 100, Fields: Fields{"port": 8080},
		}}},
	}); err != nil {
		t.Fatalf("first edge: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "second",
		Version: 2,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "collector-edge-b", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent-b", ExternalID: "raw-b", SourceRank: 100, Fields: Fields{"protocol": "http"},
		}}},
	}); err != nil {
		t.Fatalf("second edge: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge, ok := g.Edges[edgeID]
	if !ok || len(g.Edges) != 1 {
		t.Fatalf("edges = %#v", g.Edges)
	}
	if edge.ID != edgeID || edge.Fields["port"] != float64(8080) || edge.Fields["protocol"] != "http" {
		t.Fatalf("merged edge = %#v", edge)
	}
	if !EdgeSourceAliasMatches(edge, "collector-edge-a") || !EdgeSourceAliasMatches(edge, "collector-edge-b") {
		t.Fatalf("source aliases = %#v", edge.Sources)
	}
}

func TestUpsertEdgeWithoutIDUsesCanonicalTripleID(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	if err := g.ApplyCommit(Commit{
		ID:      "edge",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", ExternalID: "collector-edge-a", Fields: Fields{"port": 8080},
		}}},
	}); err != nil {
		t.Fatalf("edge without id: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge, ok := g.Edges[edgeID]
	if !ok || edge.ID != edgeID || edge.Fields["port"] != float64(8080) {
		t.Fatalf("edge = %#v", g.Edges)
	}
	if !EdgeSourceAliasMatches(edge, "collector-edge-a") {
		t.Fatalf("source aliases = %#v", edge.Sources)
	}
}

func TestCanonicalEdgeSourcesStampObservedAt(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	observedAt := time.Date(2026, 6, 19, 10, 30, 0, 0, time.UTC)
	if err := g.ApplyCommit(Commit{
		ID:        "first",
		Version:   1,
		CreatedAt: observedAt,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "collector-edge-a", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent-a", ExternalID: "raw-a", SourceRank: 100,
		}}},
	}); err != nil {
		t.Fatalf("edge: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge := g.Edges[edgeID]
	if len(edge.Sources) != 1 || !edge.Sources[0].ObservedAt.Equal(observedAt) {
		t.Fatalf("sources = %#v", edge.Sources)
	}
}

func TestSnapshotRoundTripDoesNotAddDuplicateCanonicalEdgeSource(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	if err := g.ApplyCommit(Commit{
		ID:      "first",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "collector-edge-a", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent-a", ExternalID: "raw-a", SourceRank: 100,
		}}},
	}); err != nil {
		t.Fatalf("edge: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	before := g.Edges[edgeID]
	loaded, err := FromSnapshot(g.Snapshot())
	if err != nil {
		t.Fatalf("from snapshot: %v", err)
	}
	after := loaded.Edges[edgeID]
	if !reflect.DeepEqual(after.Sources, before.Sources) {
		t.Fatalf("sources after snapshot = %#v, want %#v", after.Sources, before.Sources)
	}
	for _, source := range after.Sources {
		if source.Source == "agent-a" && source.ExternalID == "raw-a" && source.EdgeID == "" {
			t.Fatalf("snapshot load added duplicate primary source without edge alias: %#v", after.Sources)
		}
	}
}

func TestEdgeSourcePolicySuppressesLowerPriorityField(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	policy := edgePolicy()
	manual := Edge{
		ID: "manual-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
		Source: "manual", Confidence: 0.9, Fields: Fields{"note": "manual"},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{ID: "manual", Version: 1, Mutations: Mutations{UpsertEdges: []Edge{manual}}}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual edge: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent",
		Version: 2,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "agent-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", Confidence: 1, Fields: Fields{"note": "collector"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent edge: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge := g.Edges[edgeID]
	if edge.Fields["note"] != "manual" {
		t.Fatalf("edge note = %#v", edge.Fields["note"])
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].ResourceType != "edge" || report.Suppressed[0].CanonicalID != edgeID || report.Suppressed[0].IncomingID != "agent-edge" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
	if edge.FieldSources["note"].Source != "manual" || edge.FieldSources["note"].Priority != 1000 {
		t.Fatalf("field source = %#v", edge.FieldSources["note"])
	}
}

func TestIncomingEdgeSourcesCannotElevateWriteOwner(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	policy := edgePolicy()
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "manual-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "manual", Fields: Fields{"note": "manual"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual edge: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-spoof",
		Version: 2,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "agent-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", Fields: Fields{"note": "collector"},
			Sources: []EdgeSource{{Source: "manual", EdgeID: "spoofed-manual"}},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent spoof edge: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge := g.Edges[edgeID]
	if edge.Fields["note"] != "manual" {
		t.Fatalf("spoofed source elevated edge write owner: %#v", edge)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].IncomingSource != "agent" || report.Suppressed[0].IncomingPriority != 100 {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestIncomingEdgeSourcesCannotSpoofAlias(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	g.Entities["host:app-02"] = Entity{ID: "host:app-02", Kind: "host", Fields: Fields{}}
	g.rebuildIndexes()
	if err := g.ApplyCommit(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "manual-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "manual", SourceRank: 1000,
		}}},
	}); err != nil {
		t.Fatalf("manual edge: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "agent-spoof",
		Version: 2,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "agent-edge", Type: "runs_on", From: "service:api", To: "host:app-02",
			Source: "agent", SourceRank: 100,
			Sources: []EdgeSource{{Source: "manual", EdgeID: "manual-edge", Priority: 1000}},
		}}},
	}); err != nil {
		t.Fatalf("agent spoof edge: %v", err)
	}
	agentID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-02")
	agent := g.Edges[agentID]
	if EdgeSourceAliasMatches(agent, "manual-edge") {
		t.Fatalf("agent edge accepted spoofed manual alias: %#v", agent.Sources)
	}
	if !EdgeSourceAliasMatches(agent, "agent-edge") {
		t.Fatalf("agent edge lost own incoming alias: %#v", agent.Sources)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-delete",
		Version: 3,
		Mutations: Mutations{DeleteEdgeRequests: []EdgeDeleteRequest{{
			ID: "manual-edge", Source: "agent", SourceRank: 100,
		}}},
	}, ApplyOptions{})
	if err != nil {
		t.Fatalf("agent delete by manual alias: %v", err)
	}
	if _, ok := g.Edges[agentID]; !ok {
		t.Fatal("agent edge was deleted through spoofed manual alias")
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].CanonicalID != CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01") {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestEdgeSourceMergeKeepsHigherPriorityAliasRecord(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	if err := g.ApplyCommit(Commit{
		ID:      "high-priority",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "collector-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "collector", ExternalID: "raw-edge", SourceRank: 1000, Confidence: 0.1,
			Fields: Fields{"note": "manual"},
		}}},
	}); err != nil {
		t.Fatalf("high priority edge: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "lower-priority-confidence",
		Version: 2,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "collector-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "collector", ExternalID: "raw-edge", SourceRank: 100, Confidence: 1,
			Fields: Fields{"protocol": "http"},
		}}},
	}); err != nil {
		t.Fatalf("lower priority edge: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge := g.Edges[edgeID]
	if len(edge.Sources) != 1 {
		t.Fatalf("sources = %#v", edge.Sources)
	}
	if edge.Sources[0].Priority != 1000 || edge.Sources[0].Confidence != 0.1 {
		t.Fatalf("edge source alias was downgraded by lower priority write: %#v", edge.Sources[0])
	}
}

func TestNewEdgeStampsOwnershipFromEffectiveSource(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	policy := edgePolicy()
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "agent-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", Fields: Fields{"note": "collector"},
			ExistenceSource: &FieldSource{Source: "manual", Priority: 1000},
			FieldSources: map[string]FieldSource{
				"note": {Source: "manual", Priority: 1000},
			},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent edge: %v", err)
	}
	if len(report.Suppressed) != 0 {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge := g.Edges[edgeID]
	if edge.ExistenceSource == nil || edge.ExistenceSource.Source != "agent" || edge.ExistenceSource.Priority != 100 {
		t.Fatalf("existence source = %#v", edge.ExistenceSource)
	}
	if edge.FieldSources["note"].Source != "agent" || edge.FieldSources["note"].Priority != 100 {
		t.Fatalf("field source = %#v", edge.FieldSources["note"])
	}
}

func TestSuppressedEdgeWithoutIDReportsExternalID(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	policy := edgePolicy()
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "manual-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "manual", Fields: Fields{"note": "manual"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual edge: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent",
		Version: 2,
		Mutations: Mutations{UpsertEdges: []Edge{{
			Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", ExternalID: "collector-edge-a", Fields: Fields{"note": "collector"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent edge: %v", err)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].IncomingID != "collector-edge-a" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestEmptyIncomingEdgeFieldDoesNotClearExistingValue(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	policy := edgePolicy()
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "agent-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", Fields: Fields{"note": "collector", "port": ""},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("agent edge: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual-empty",
		Version: 2,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "manual-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "manual", Fields: Fields{"note": "", "port": "443"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("manual edge: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge := g.Edges[edgeID]
	if edge.Fields["note"] != "collector" {
		t.Fatalf("note was cleared: %#v", edge.Fields)
	}
	if edge.Fields["port"] != "443" {
		t.Fatalf("empty edge field was not filled: %#v", edge.Fields)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].ResourceType != "edge" || report.Suppressed[0].Field != "note" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestEdgeSourceAwareDeleteHonorsExistenceOwner(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	policy := edgePolicy()
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "manual-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "manual", Confidence: 0.9,
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual edge: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-delete",
		Version: 2,
		Mutations: Mutations{DeleteEdgeRequests: []EdgeDeleteRequest{{
			ID: "manual-edge", Source: "agent", Confidence: 1, Reason: "collector vanished",
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent delete: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if _, ok := g.Edges[edgeID]; !ok {
		t.Fatal("low priority delete removed manual edge")
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "__existence__" || report.Suppressed[0].ResourceType != "edge" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}

	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual-delete",
		Version: 3,
		Mutations: Mutations{DeleteEdgeRequests: []EdgeDeleteRequest{{
			Type: "runs_on", From: "service:api", To: "host:app-01", Source: "manual",
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual delete: %v", err)
	}
	if _, ok := g.Edges[edgeID]; ok {
		t.Fatal("high priority delete did not remove edge")
	}
}

func TestDeleteEdgesForceDeleteResolvesSourceAlias(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	if err := g.ApplyCommit(Commit{
		ID:      "edge",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "collector-edge", Type: "runs_on", From: "service:api", To: "host:app-01",
			Source: "agent", ExternalID: "raw-edge",
		}}},
	}); err != nil {
		t.Fatalf("edge: %v", err)
	}
	if err := g.ApplyCommit(Commit{ID: "delete", Version: 2, Mutations: Mutations{DeleteEdges: []string{"collector-edge"}}}); err != nil {
		t.Fatalf("delete alias: %v", err)
	}
	if len(g.Edges) != 0 {
		t.Fatalf("edges = %#v", g.Edges)
	}
}

func TestDeleteEdgesRejectsAmbiguousSourceAlias(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	if err := g.ApplyCommit(Commit{
		ID:      "edges",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{ID: "host:app-02", Kind: "host"}}, UpsertEdges: []Edge{
			{ID: "collector-edge", Type: "runs_on", From: "service:api", To: "host:app-01", Source: "agent"},
			{ID: "collector-edge", Type: "runs_on", From: "service:api", To: "host:app-02", Source: "agent"},
		}},
	}); err != nil {
		t.Fatalf("edges: %v", err)
	}
	err := g.ApplyCommit(Commit{ID: "delete", Version: 2, Mutations: Mutations{DeleteEdges: []string{"collector-edge"}}})
	if err == nil {
		t.Fatal("expected ambiguous source alias error")
	}
	if len(g.Edges) != 2 {
		t.Fatalf("ambiguous delete changed edges: %#v", g.Edges)
	}
}

func TestDeleteEdgeRequestRejectsAmbiguousSourceAlias(t *testing.T) {
	g := graphWithEdgeEndpoints(t)
	if err := g.ApplyCommit(Commit{
		ID:      "edges",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{ID: "host:app-02", Kind: "host"}}, UpsertEdges: []Edge{
			{ID: "collector-edge", Type: "runs_on", From: "service:api", To: "host:app-01", Source: "agent", SourceRank: 100},
			{ID: "collector-edge", Type: "runs_on", From: "service:api", To: "host:app-02", Source: "agent", SourceRank: 100},
		}},
	}); err != nil {
		t.Fatalf("edges: %v", err)
	}
	err := g.ApplyCommit(Commit{
		ID:      "delete",
		Version: 2,
		Mutations: Mutations{DeleteEdgeRequests: []EdgeDeleteRequest{{
			ID: "collector-edge", Source: "agent", SourceRank: 100,
		}}},
	})
	if err == nil {
		t.Fatal("expected ambiguous source alias error")
	}
	if len(g.Edges) != 2 {
		t.Fatalf("ambiguous delete request changed edges: %#v", g.Edges)
	}
}

func TestSnapshotLoadCanonicalizesDuplicateTriples(t *testing.T) {
	snapshot := Snapshot{
		Version: 7,
		Entities: []Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host"},
		},
		Edges: []Edge{
			{ID: "old-edge-a", Type: "runs_on", From: "service:api", To: "host:app-01", Source: "agent-a", SourceRank: 50, Fields: Fields{"port": 8080}},
			{ID: "old-edge-b", Type: "runs_on", From: "service:api", To: "host:app-01", Source: "agent-b", SourceRank: 50, Fields: Fields{"protocol": "http"}},
		},
	}
	g, err := FromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	edgeID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	edge, ok := g.Edges[edgeID]
	if !ok || len(g.Edges) != 1 {
		t.Fatalf("edges = %#v", g.Edges)
	}
	if !EdgeSourceAliasMatches(edge, "old-edge-a") || !EdgeSourceAliasMatches(edge, "old-edge-b") {
		t.Fatalf("source aliases = %#v", edge.Sources)
	}
	if len(g.Neighbors("service:api", "out", "runs_on")) != 1 {
		t.Fatal("duplicate triple leaked into adjacency")
	}
}

func TestSnapshotLoadPreservesMultipleEdgesWithoutIDs(t *testing.T) {
	snapshot := Snapshot{
		Version: 7,
		Entities: []Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host"},
			{ID: "host:app-02", Kind: "host"},
		},
		Edges: []Edge{
			{Type: "runs_on", From: "service:api", To: "host:app-01", Source: "agent", ExternalID: "raw-a"},
			{Type: "runs_on", From: "service:api", To: "host:app-02", Source: "agent", ExternalID: "raw-b"},
		},
	}
	g, err := FromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %#v", g.Edges)
	}
	for _, edge := range g.Edges {
		if edge.ID != CanonicalEdgeID(edge) {
			t.Fatalf("edge was not canonicalized: %#v", edge)
		}
	}
	if len(g.Neighbors("service:api", "out", "runs_on")) != 2 {
		t.Fatal("snapshot load lost an edge without id")
	}
}

func TestMergeEdgeSetUsesStableWriteOrderForDuplicateTriples(t *testing.T) {
	olderAt := time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
	newerAt := olderAt.Add(time.Minute)
	canonicalID := CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	for i := 0; i < 50; i++ {
		merged, _ := mergeEdgeSet(map[string]Edge{
			"legacy-new": {
				ID: "legacy-new", Type: "runs_on", From: "service:api", To: "host:app-01",
				Source: "agent", SourceRank: 100, Confidence: 0.5, Version: 2, UpdatedAt: newerAt,
				Fields: Fields{"port": 9090},
			},
			"legacy-old": {
				ID: "legacy-old", Type: "runs_on", From: "service:api", To: "host:app-01",
				Source: "agent", SourceRank: 100, Confidence: 0.5, Version: 1, UpdatedAt: olderAt,
				Fields: Fields{"port": 8080},
			},
		}, 3, newerAt)
		edge, ok := merged[canonicalID]
		if !ok || len(merged) != 1 {
			t.Fatalf("merged edges = %#v", merged)
		}
		if edge.Fields["port"] != 9090 {
			t.Fatalf("merged edge port = %#v, want newer edge value", edge.Fields["port"])
		}
		if edge.FieldSources["port"].Version != 2 {
			t.Fatalf("merged edge field source = %#v", edge.FieldSources["port"])
		}
	}
}

func graphWithEdgeEndpoints(t *testing.T) *Graph {
	t.Helper()
	g := New()
	g.Entities["service:api"] = Entity{ID: "service:api", Kind: "service", Fields: Fields{}}
	g.Entities["host:app-01"] = Entity{ID: "host:app-01", Kind: "host", Fields: Fields{}}
	g.rebuildIndexes()
	return g
}

func edgePolicy() SourcePolicy {
	return SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}
}
