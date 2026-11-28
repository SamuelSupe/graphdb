package graph

import (
	"testing"
	"time"
)

func TestRound5SourcePolicyAppliesAliasesAndFieldPrioritiesToSingleSourcesEntry(t *testing.T) {
	policy := SourcePolicy{
		Sources: []SourcePolicyItem{
			{Name: "cmdb", Priority: 100},
			{Name: "monitoring", Priority: 50},
		},
		FieldAliases: []FieldAliasRule{
			{Source: "cmdb", Kind: "host", Aliases: map[string]string{"host_name": "hostname"}},
		},
		FieldPriorities: []FieldPriorityRule{
			{Source: "cmdb", Kind: "host", Fields: map[string]int{"owner": 10}},
		},
	}

	g := New()
	if err := g.ApplyCommit(Commit{
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{{
				ID:         "host:1",
				Kind:       "host",
				Source:     "monitoring",
				ExternalID: "m-1",
				SourceRank: 50,
				Fields:     Fields{"owner": "sre-team"},
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	incoming := Mutations{
		UpsertEntities: []Entity{{
			ID:   "host:1",
			Kind: "host",
			Sources: []EntitySource{{
				Source:     "cmdb",
				ExternalID: "c-1",
			}},
			Fields: Fields{"host_name": "web-01", "owner": "wrong-owner"},
		}},
	}
	applied, _, err := ApplySourcePolicy(incoming, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{Version: 2, Mutations: applied}); err != nil {
		t.Fatal(err)
	}
	got, ok := g.GetEntity("host:1")
	if !ok {
		t.Fatal("host:1 missing")
	}
	if got.Fields["hostname"] != "web-01" {
		t.Fatalf("alias host_name was not mapped to hostname: %+v", got.Fields)
	}
	if got.Fields["owner"] != "sre-team" {
		t.Fatalf("field priority rule bypassed via entity.Sources: owner=%v, want sre-team", got.Fields["owner"])
	}
}

func TestRound5EntityAndEdgeMergePreservePerFieldPrecedence(t *testing.T) {
	now := time.Now().UTC()
	g := New()
	if err := g.ApplyCommit(Commit{
		Version:   1,
		CreatedAt: now,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "svc:a", Kind: "service", Source: "scanner", ExternalID: "sa", SourceRank: 10, Fields: Fields{"env": "dev"}},
				{ID: "svc:b", Kind: "service", Source: "monitoring", ExternalID: "mb", SourceRank: 50, Fields: Fields{"env": "prod"}},
				{ID: "svc:x", Kind: "service", Source: "cmdb", ExternalID: "cx", SourceRank: 100},
			},
			UpsertEdges: []Edge{
				{Type: "depends_on", From: "svc:a", To: "svc:x", Source: "scanner", ExternalID: "ea", SourceRank: 10, Fields: Fields{"env": "dev"}},
				{Type: "depends_on", From: "svc:b", To: "svc:x", Source: "monitoring", ExternalID: "eb", SourceRank: 50, Fields: Fields{"env": "prod"}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{
		Version:   2,
		CreatedAt: now.Add(time.Second),
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "svc:a", Kind: "service", Source: "cmdb", ExternalID: "ca", SourceRank: 100, Fields: Fields{"owner": "sre"}},
			},
			UpsertEdges: []Edge{
				{Type: "depends_on", From: "svc:a", To: "svc:x", Source: "cmdb", ExternalID: "ea2", SourceRank: 100, Fields: Fields{"port": 443}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{
		Version:   3,
		CreatedAt: now.Add(2 * time.Second),
		Mutations: Mutations{
			MergeEntities: []MergeRequest{{TargetID: "svc:b", SourceIDs: []string{"svc:a"}}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	b, ok := g.GetEntity("svc:b")
	if !ok {
		t.Fatal("svc:b missing")
	}
	if b.Fields["env"] != "prod" {
		t.Fatalf("entity merge overwrote rank-50 env with rank-10 env: got %v", b.Fields["env"])
	}
	if b.Fields["owner"] != "sre" {
		t.Fatalf("entity merge dropped rank-100 owner: got %v", b.Fields["owner"])
	}

	edgeID := CanonicalEdgeIDParts("depends_on", "svc:b", "svc:x")
	edge, ok := g.Edges.Get(edgeID)
	if !ok {
		t.Fatal("merged edge svc:b -> svc:x missing")
	}
	if edge.Fields["env"] != "prod" {
		t.Fatalf("edge merge overwrote rank-50 env with rank-10 env: got %v (FieldSources=%+v)", edge.Fields["env"], edge.FieldSources)
	}
	if edge.ExistenceSource == nil || edge.ExistenceSource.Source != "cmdb" || edge.ExistenceSource.Priority != 100 {
		t.Fatalf("edge merge corrupted ExistenceSource: %+v", edge.ExistenceSource)
	}
}
