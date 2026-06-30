package graph

import "testing"

func TestApplyCommitUpsertsEntitiesRelationAndEdge(t *testing.T) {
	g := New()
	commit := Commit{
		ID:      "c1",
		Version: 1,
		Mutations: Mutations{
			UpsertRelationTypes: []RelationType{{
				Name:        "works_at",
				FromKind:    "person",
				ToKind:      "company",
				Directed:    true,
				Cardinality: ManyToOne,
			}},
			UpsertEntities: []Entity{
				{ID: "person:alice", Kind: "person", Fields: Fields{"age": 31}},
				{ID: "company:acme", Kind: "company", Fields: Fields{"name": "ACME"}},
			},
			UpsertEdges: []Edge{{
				ID:     "edge:alice-acme",
				Type:   "works_at",
				From:   "person:alice",
				To:     "company:acme",
				Fields: Fields{"role": "engineer"},
			}},
		},
	}
	if err := g.ApplyCommit(commit); err != nil {
		t.Fatalf("apply commit: %v", err)
	}
	if g.Version != 1 {
		t.Fatalf("version = %d, want 1", g.Version)
	}
	entity, ok := g.GetEntity("person:alice")
	if !ok {
		t.Fatal("person:alice missing")
	}
	if entity.Fields["age"] != float64(31) {
		t.Fatalf("age = %#v, want JSON-normalized 31", entity.Fields["age"])
	}
	if len(g.Neighbors("person:alice", "out", "works_at")) != 1 {
		t.Fatal("expected one outgoing works_at neighbor")
	}
}

func TestApplyCommitIsAtomicOnInvalidEdge(t *testing.T) {
	g := graphWithCompany(t)
	err := g.ApplyCommit(Commit{
		ID:      "bad",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "person:bob", Kind: "person"}},
			UpsertEdges: []Edge{{
				ID:   "edge:bob-missing",
				Type: "works_at",
				From: "person:bob",
				To:   "company:missing",
			}},
		},
	})
	if err == nil {
		t.Fatal("expected invalid edge error")
	}
	if g.Version != 1 {
		t.Fatalf("version changed to %d", g.Version)
	}
	if _, ok := g.GetEntity("person:bob"); ok {
		t.Fatal("invalid commit leaked entity")
	}
}

func TestRelationEndpointKindsAreEnforced(t *testing.T) {
	g := graphWithCompany(t)
	err := g.ApplyCommit(Commit{
		ID:      "bad-kind",
		Version: 2,
		Mutations: Mutations{
			UpsertEdges: []Edge{{
				ID:   "edge:wrong",
				Type: "works_at",
				From: "company:acme",
				To:   "person:alice",
			}},
		},
	})
	if err == nil {
		t.Fatal("expected endpoint kind error")
	}
}

func TestEntityKindChangeIsRejected(t *testing.T) {
	g := graphWithCompany(t)
	err := g.ApplyCommit(Commit{
		ID:      "bad-kind-change",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID:     "person:alice",
				Kind:   "company",
				Fields: Fields{"registration": "ACME-2"},
			}},
		},
	})
	if err == nil {
		t.Fatal("expected entity kind change error")
	}
	alice, ok := g.GetEntity("person:alice")
	if !ok {
		t.Fatal("person:alice missing after rejected commit")
	}
	if alice.Kind != "person" {
		t.Fatalf("person:alice kind = %q, want person", alice.Kind)
	}
	if _, ok := alice.Fields["registration"]; ok {
		t.Fatalf("rejected kind change leaked field: %#v", alice.Fields)
	}
	if len(g.Neighbors("person:alice", "out", "works_at")) != 1 {
		t.Fatal("rejected kind change changed existing edges")
	}
}

func TestApplyCommitRejectsEmptyTopLevelFieldNames(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "bad-entity-field",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:empty-field", Kind: "host", Fields: Fields{"": "bad"},
		}}},
	})
	if err == nil {
		t.Fatal("expected empty entity field name error")
	}
	if g.Version != 0 || len(g.Entities) != 0 {
		t.Fatalf("invalid entity field commit changed graph: version=%d entities=%#v", g.Version, g.Entities)
	}

	g = graphWithEdgeEndpoints(t)
	err = g.ApplyCommit(Commit{
		ID:      "bad-edge-field",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID: "edge-empty-field", Type: "runs_on", From: "service:api", To: "host:app-01",
			Fields: Fields{"": "bad"},
		}}},
	})
	if err == nil {
		t.Fatal("expected empty edge field name error")
	}
	if len(g.Edges) != 0 {
		t.Fatalf("invalid edge field commit changed graph: edges=%#v", g.Edges)
	}
}

func TestManyToOneCardinalityIsEnforced(t *testing.T) {
	g := graphWithCompany(t)
	err := g.ApplyCommit(Commit{
		ID:      "bad-cardinality",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "company:other", Kind: "company"}},
			UpsertEdges: []Edge{{
				ID:   "edge:alice-other",
				Type: "works_at",
				From: "person:alice",
				To:   "company:other",
			}},
		},
	})
	if err == nil {
		t.Fatal("expected cardinality error")
	}
	if len(g.Neighbors("person:alice", "out", "works_at")) != 1 {
		t.Fatal("invalid cardinality commit changed graph")
	}
}

func TestOneToOneCardinalityChecksBothEndpoints(t *testing.T) {
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "seed-one-to-one",
		Version: 1,
		Mutations: Mutations{
			UpsertRelationTypes: []RelationType{{
				Name:        "primary_owner",
				FromKind:    "service",
				ToKind:      "person",
				Directed:    true,
				Cardinality: OneToOne,
			}},
			UpsertEntities: []Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "service:web", Kind: "service"},
				{ID: "person:alice", Kind: "person"},
				{ID: "person:bob", Kind: "person"},
			},
			UpsertEdges: []Edge{{Type: "primary_owner", From: "service:api", To: "person:alice"}},
		},
	})
	if err != nil {
		t.Fatalf("seed one-to-one graph: %v", err)
	}
	for _, edge := range []Edge{
		{Type: "primary_owner", From: "service:api", To: "person:bob"},
		{Type: "primary_owner", From: "service:web", To: "person:alice"},
	} {
		err := g.ApplyCommit(Commit{
			ID:      "bad-one-to-one",
			Version: g.Version + 1,
			Mutations: Mutations{
				UpsertEdges: []Edge{edge},
			},
		})
		if err == nil {
			t.Fatalf("expected one_to_one violation for edge %#v", edge)
		}
	}
}

func graphWithCompany(t *testing.T) *Graph {
	t.Helper()
	g := New()
	err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertRelationTypes: []RelationType{{
				Name:        "works_at",
				FromKind:    "person",
				ToKind:      "company",
				Directed:    true,
				Cardinality: ManyToOne,
			}},
			UpsertEntities: []Entity{
				{ID: "person:alice", Kind: "person"},
				{ID: "company:acme", Kind: "company"},
			},
			UpsertEdges: []Edge{{
				ID:   "edge:alice-acme",
				Type: "works_at",
				From: "person:alice",
				To:   "company:acme",
			}},
		},
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	return g
}
