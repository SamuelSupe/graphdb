package graph

import "testing"

func TestSnapshotPersistsIndexes(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"hostname": {Type: "string", Indexed: true},
				},
				IdentityKeys: []IdentityKey{{Name: "hostname", Fields: []string{"hostname"}}},
			}},
			UpsertEntities: []Entity{{ID: "host:a", Kind: "host", Fields: Fields{"hostname": "app-01"}}},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	snapshot := g.Snapshot()
	if snapshot.Index == nil {
		t.Fatal("snapshot missing persistent index")
	}
	loaded, err := FromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("from snapshot: %v", err)
	}
	matches := loaded.MatchEntities("host", Fields{"hostname": "app-01"})
	if len(matches) != 1 || matches[0].ID != "host:a" {
		t.Fatalf("indexed snapshot match = %#v", matches)
	}
}

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
