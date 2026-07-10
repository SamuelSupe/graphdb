package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"graphdb/internal/graph"
)

var benchmarkIndexBuildArtifacts indexBuildArtifacts

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
