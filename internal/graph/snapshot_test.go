package graph

import "testing"

func TestFromSnapshotRejectsCardinalityViolations(t *testing.T) {
	_, err := FromSnapshot(Snapshot{
		Version: 1,
		RelationTypes: []RelationType{{
			Name:        "works_at",
			FromKind:    "person",
			ToKind:      "company",
			Directed:    true,
			Cardinality: ManyToOne,
		}},
		Entities: []Entity{
			{ID: "person:alice", Kind: "person"},
			{ID: "company:one", Kind: "company"},
			{ID: "company:two", Kind: "company"},
		},
		Edges: []Edge{
			{Type: "works_at", From: "person:alice", To: "company:one"},
			{Type: "works_at", From: "person:alice", To: "company:two"},
		},
	})
	if err == nil {
		t.Fatal("expected snapshot cardinality violation")
	}
}

func TestFromSnapshotRebuildsAuthoritativeIndexes(t *testing.T) {
	loaded, err := FromSnapshot(Snapshot{
		Version: 1,
		CITypes: []CIType{{
			Name:   "host",
			Fields: map[string]FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		Entities: []Entity{{
			ID: "host:app-01", Kind: "host", Fields: Fields{"hostname": "app-01"},
		}},
		Index: &IndexSnapshot{
			Version: 1,
			Field: map[string]map[string]map[string][]string{
				"host": {"hostname": {"s:stale": {"host:app-01"}}},
			},
			Out:      map[string][]string{},
			In:       map[string][]string{},
			Identity: map[string]map[string]string{},
		},
	})
	if err != nil {
		t.Fatalf("from snapshot: %v", err)
	}
	matches := loaded.MatchEntities("host", Fields{"hostname": "app-01"})
	if len(matches) != 1 || matches[0].ID != "host:app-01" {
		t.Fatalf("snapshot loaded stale embedded index: %#v", matches)
	}
}

func TestFromSnapshotPreservesExplicitSourceIdentities(t *testing.T) {
	owner := FieldSource{Source: "manual", Priority: 1000}
	t.Run("entities", func(t *testing.T) {
		g, err := FromSnapshot(Snapshot{Version: 3, Entities: []Entity{
			{ID: "host:aws", Kind: "host", Source: "manual", ExternalID: "host-1", SourceRank: 1000, ExistenceSource: &owner,
				Sources: []EntitySource{{Source: "aws", ExternalID: "host-1", Priority: 50}}},
			{ID: "host:manual", Kind: "host", Source: "manual", ExternalID: "host-1", SourceRank: 1000, ExistenceSource: &owner,
				Sources: []EntitySource{{Source: "manual", ExternalID: "host-1", Priority: 1000}}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if g.Entities.Len() != 2 {
			t.Fatalf("snapshot invented an identity shared by independent sources: entities=%+v", g.Snapshot().Entities)
		}
		entity, ok := g.Entities.Get("host:aws")
		if !ok || len(entity.Sources) != 1 || entity.Sources[0].Source != "aws" {
			t.Fatalf("explicit AWS identity changed: %+v", entity)
		}
	})
	t.Run("edges", func(t *testing.T) {
		edge := Edge{Type: "calls", From: "host:left", To: "host:right", Source: "manual", ExternalID: "cloud-edge", SourceRank: 1000,
			ExistenceSource: &owner, Sources: []EdgeSource{{Source: "aws", ExternalID: "cloud-edge", EdgeID: "aws-edge-id", Priority: 50}}}
		edge.ID = CanonicalEdgeID(edge)
		g, err := FromSnapshot(Snapshot{Version: 3,
			Entities:      []Entity{{ID: "host:left", Kind: "host"}, {ID: "host:right", Kind: "host"}},
			RelationTypes: []RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true, Cardinality: ManyToMany}},
			Edges:         []Edge{edge},
		})
		if err != nil {
			t.Fatal(err)
		}
		got, ok := g.Edges.Get(edge.ID)
		if !ok || len(got.Sources) != 1 || got.Sources[0].Source != "aws" || got.Sources[0].EdgeID != "aws-edge-id" {
			t.Fatalf("snapshot invented a source alias for the edge: %+v", got)
		}
	})
}
