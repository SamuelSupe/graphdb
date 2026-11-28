package graph

import (
	"fmt"
	"testing"
	"time"
)

func TestApplySplitRejectsEmptyIDAndTracksNormalizedID(t *testing.T) {
	g := New()
	now := time.Unix(1700000000, 0).UTC()
	if err := g.ApplyCommit(Commit{
		ID:      "c1",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes:  []CIType{{Name: "host"}},
			UpsertEntities: []Entity{{ID: "host:src", Kind: "host"}},
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	// Empty replacement ID without source+external_id must fail cleanly.
	_, _, err := g.ApplyCommitCopyWithOptions(Commit{
		ID:      "c2-bad",
		Version: 2,
		Mutations: Mutations{
			SplitEntities: []SplitRequest{{
				SourceID: "host:src",
				Entities: []Entity{{ID: "   ", Kind: "host"}},
			}},
		},
		CreatedAt: now.Add(time.Second),
	}, ApplyOptions{})
	if err == nil {
		t.Fatal("expected split with blank replacement ID to fail")
	}

	// Valid replacement with whitespace-padded ID must trim & index under normalized ID.
	next, _, err := g.ApplyCommitCopyWithOptions(Commit{
		ID:      "c2-ok",
		Version: 2,
		Mutations: Mutations{
			SplitEntities: []SplitRequest{{
				SourceID: "host:src",
				Entities: []Entity{{ID: " host:child ", Kind: "host"}},
			}},
		},
		CreatedAt: now.Add(time.Second),
	}, ApplyOptions{})
	if err != nil {
		t.Fatalf("split failed: %v", err)
	}
	if _, ok := next.GetEntity("host:child"); !ok {
		t.Fatal("normalized split replacement entity not found")
	}
	if _, ok := next.GetEntity("host:src"); ok {
		t.Fatal("source entity should be removed after split")
	}
}

func TestEntityPartitionsInheritedAcrossCommitCopy(t *testing.T) {
	g := New()
	now := time.Unix(1700000000, 0).UTC()
	if err := g.ApplyCommit(Commit{
		ID:      "c1",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes:  []CIType{{Name: "host"}},
			UpsertEntities: []Entity{{ID: "host:1", Kind: "host"}},
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	// Trigger partition index build on base graph.
	if err := g.VisitEntityStorageShard(EntityStorageShard("host:1"), func(Entity) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if g.entityPartitions == nil {
		t.Fatal("expected entityPartitions to be populated after VisitEntityStorageShard")
	}
	next, _, err := g.ApplyCommitCopyWithOptions(Commit{
		ID:      "c2",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "host:2", Kind: "host"}},
		},
		CreatedAt: now.Add(time.Second),
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if next.entityPartitions == nil {
		t.Fatal("expected CoW commit clone to inherit entityPartitions")
	}
	h1, err := next.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := FromSnapshot(next.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := rebuilt.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatalf("inherited entityPartitions hash mismatch: got %s want %s", h1, h2)
	}
}

func TestCopyEntityAndEffectiveFieldsDeepCloneContainers(t *testing.T) {
	g := New()
	now := time.Unix(1700000000, 0).UTC()
	if err := g.ApplyCommit(Commit{
		ID:      "c1",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "svc",
				Fields: map[string]FieldSpec{
					"tags": {
						Type:    "array",
						Default: []any{"a", "b"},
					},
					"tier": {
						Type: "string",
						Enum: []any{"gold", "silver"},
					},
				},
			}},
			UpsertEntities: []Entity{{
				ID:   "svc:1",
				Kind: "svc",
				Fields: Fields{
					"str_map": map[string]string{"env": "prod"},
					"matrix":  [][]string{{"r1c1"}},
				},
			}},
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	raw := Entity{
		ID:   "svc:raw",
		Kind: "svc",
		Fields: Fields{
			"str_map": map[string]string{"env": "prod"},
			"matrix":  [][]string{{"r1c1"}},
		},
	}
	copied := CopyEntity(raw)
	copied.Fields["str_map"].(map[string]string)["env"] = "mutated"
	copied.Fields["matrix"].([][]string)[0][0] = "mutated"
	if raw.Fields["str_map"].(map[string]string)["env"] != "prod" {
		t.Fatalf("CopyEntity leaked map[string]string reference: %#v", raw.Fields["str_map"])
	}
	if raw.Fields["matrix"].([][]string)[0][0] != "r1c1" {
		t.Fatalf("CopyEntity leaked [][]string reference: %#v", raw.Fields["matrix"])
	}

	specs, err := g.EffectiveFields("svc")
	if err != nil {
		t.Fatal(err)
	}
	specs["tags"].Default.([]any)[0] = "corrupted"
	specs["tier"].Enum[0] = "corrupted"

	specsAgain, err := g.EffectiveFields("svc")
	if err != nil {
		t.Fatal(err)
	}
	if specsAgain["tags"].Default.([]any)[0] != "a" || specsAgain["tier"].Enum[0] != "gold" {
		t.Fatalf("EffectiveFields leaked Default/Enum slices: tags=%+v tier=%+v", specsAgain["tags"], specsAgain["tier"])
	}
}

func TestRemoveEntityFieldIndexCleansUpEmptyFields(t *testing.T) {
	g := New()
	now := time.Unix(1700000000, 0).UTC()
	if err := g.ApplyCommit(Commit{
		ID:      "c1",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes:  []CIType{{Name: "host"}},
			UpsertEntities: []Entity{{ID: "host:1", Kind: "host", Fields: Fields{"ephemeral": "v1"}}},
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if !g.HasFieldIndex("host", "ephemeral") {
		t.Fatal("expected field index for ephemeral field to exist")
	}
	next, _, err := g.ApplyCommitCopyWithOptions(Commit{
		ID:      "c2",
		Version: 2,
		Mutations: Mutations{
			DeleteEntities: []string{"host:1"},
		},
		CreatedAt: now.Add(time.Second),
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if next.HasFieldIndex("host", "ephemeral") {
		t.Fatal("expected empty ephemeral field index entry to be deleted")
	}
	if _, exists := next.fieldIndex["host"]; exists {
		t.Fatal("expected empty kind map in fieldIndex to be deleted")
	}
	// Original graph snapshot must remain untouched by COW deletion.
	if !g.HasFieldIndex("host", "ephemeral") {
		t.Fatal("COW deletion mutated original graph fieldIndex")
	}
}

func TestAdjacencyAndEntityPartitionsShardedCOWIsolation(t *testing.T) {
	g := New()
	now := time.Unix(1700000000, 0).UTC()
	entities := make([]Entity, 0, 50)
	entities = append(entities, Entity{ID: "hub:0", Kind: "host"})
	edges := make([]Edge, 0, 48)
	for i := 1; i <= 48; i++ {
		id := fmt.Sprintf("leaf:%02d", i)
		entities = append(entities, Entity{ID: id, Kind: "host"})
		edges = append(edges, Edge{ID: fmt.Sprintf("edge:%02d", i), Type: "depends_on", From: "hub:0", To: id})
	}
	if err := g.ApplyCommit(Commit{
		ID:      "c1",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes:  []CIType{{Name: "host"}},
			UpsertEntities: entities,
			UpsertEdges:    edges,
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	// Populate entityPartitions before COW commit.
	hubShard := EntityStorageShard("hub:0")
	baseCount := 0
	if err := g.VisitEntityStorageShard(hubShard, func(Entity) error { baseCount++; return nil }); err != nil {
		t.Fatal(err)
	}
	if g.NeighborScanSize("hub:0", "out") != 48 {
		t.Fatalf("expected 48 outgoing edges, got %d", g.NeighborScanSize("hub:0", "out"))
	}

	next, _, err := g.ApplyCommitCopyWithOptions(Commit{
		ID:      "c2",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "leaf:49", Kind: "host"}},
			UpsertEdges:    []Edge{{ID: "edge:49", Type: "depends_on", From: "hub:0", To: "leaf:49"}},
			DeleteEdges:    []string{"edge:01"},
		},
		CreatedAt: now.Add(time.Second),
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Old graph must remain unchanged.
	if g.NeighborScanSize("hub:0", "out") != 48 {
		t.Fatalf("COW mutated base graph out degree: got %d", g.NeighborScanSize("hub:0", "out"))
	}
	if next.NeighborScanSize("hub:0", "out") != 48 {
		t.Fatalf("expected 48 out edges after +1/-1, got %d", next.NeighborScanSize("hub:0", "out"))
	}
	if next.NeighborScanSize("leaf:01", "in") != 0 || g.NeighborScanSize("leaf:01", "in") != 1 {
		t.Fatalf("in-degree COW isolation failed: base=%d next=%d", g.NeighborScanSize("leaf:01", "in"), next.NeighborScanSize("leaf:01", "in"))
	}
	if next.NeighborScanSize("leaf:49", "in") != 1 || g.NeighborScanSize("leaf:49", "in") != 0 {
		t.Fatalf("new edge COW isolation failed: base=%d next=%d", g.NeighborScanSize("leaf:49", "in"), next.NeighborScanSize("leaf:49", "in"))
	}
	// Verify entityPartitions COW isolation on leaf:49's shard.
	newShard := EntityStorageShard("leaf:49")
	baseHas49, nextHas49 := false, false
	_ = g.VisitEntityStorageShard(newShard, func(e Entity) error {
		if e.ID == "leaf:49" {
			baseHas49 = true
		}
		return nil
	})
	_ = next.VisitEntityStorageShard(newShard, func(e Entity) error {
		if e.ID == "leaf:49" {
			nextHas49 = true
		}
		return nil
	})
	if baseHas49 || !nextHas49 {
		t.Fatalf("entityPartitions COW isolation failed: baseHas49=%v nextHas49=%v", baseHas49, nextHas49)
	}
}
