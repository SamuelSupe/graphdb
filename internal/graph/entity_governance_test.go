package graph

import "testing"

func TestSourceExternalIDGeneratesCanonicalEntityID(t *testing.T) {
	g := New()
	policy := entityPolicy()
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			Kind: "host", Source: "aws", ExternalID: "i-123", Fields: Fields{"hostname": "app-01"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	id := CanonicalEntityIDParts("host", "aws", "i-123")
	entity, ok := g.GetEntity(id)
	if !ok || entity.ID != id || entity.Fields["hostname"] != "app-01" {
		t.Fatalf("entity = %#v ok=%v", entity, ok)
	}
	if len(report.CanonicalEntities) != 1 || report.CanonicalEntities[0].CanonicalID != id {
		t.Fatalf("canonical entities = %#v", report.CanonicalEntities)
	}
}

func TestSourceExternalIDMergesDifferentIncomingEntityIDs(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "first",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:first", Kind: "host", Source: "aws", ExternalID: "i-123", SourceRank: 50, Fields: Fields{"region": "us-east-1"},
		}}},
	}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "second",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:second", Kind: "host", Source: "aws", ExternalID: "i-123", SourceRank: 50, Fields: Fields{"az": "a"},
		}}},
	}); err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(g.Entities) != 1 {
		t.Fatalf("entities = %#v", g.Entities)
	}
	entity, ok := g.GetEntityByReference("host:second")
	if !ok || entity.ID != "host:first" || entity.Fields["az"] != "a" {
		t.Fatalf("alias lookup entity = %#v ok=%v", entity, ok)
	}
}

func TestDifferentSourcesMergeByCIIdentityAndKeepAliases(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-type",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{
			Name:   "host",
			Fields: map[string]FieldSpec{"hostname": {Type: "string"}},
			IdentityKeys: []IdentityKey{{
				Name:   "hostname",
				Fields: []string{"hostname"},
			}},
		}}},
	}); err != nil {
		t.Fatalf("seed type: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "agent",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:agent", Kind: "host", Source: "agent", ExternalID: "agent-1",
			Fields: Fields{"hostname": "app-01", "region": "agent-r0"},
		}}},
	}); err != nil {
		t.Fatalf("agent: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "cloud",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:cloud", Kind: "host", Source: "cloud", ExternalID: "cloud-1",
			Fields: Fields{"hostname": "app-01", "zone": "cloud-z0"},
		}}},
	}); err != nil {
		t.Fatalf("cloud: %v", err)
	}
	if len(g.Entities) != 1 {
		t.Fatalf("entities = %#v", g.Entities)
	}
	entity, ok := g.GetEntity("host:agent")
	if !ok || entity.Fields["zone"] != "cloud-z0" || len(entity.Sources) != 2 {
		t.Fatalf("merged entity = %#v ok=%v", entity, ok)
	}
	if !hasEntitySource(entity, "agent", "agent-1") || !hasEntitySource(entity, "cloud", "cloud-1") {
		t.Fatalf("sources = %#v", entity.Sources)
	}
}

func hasEntitySource(entity Entity, source string, externalID string) bool {
	for _, item := range entity.Sources {
		if item.Source == source && item.ExternalID == externalID {
			return true
		}
	}
	return false
}

func TestSourceAwareEntityDeleteSuppressesLowerPriority(t *testing.T) {
	g := New()
	policy := entityPolicy()
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", ExternalID: "asset-1", Fields: Fields{"owner": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-delete",
		Version: 2,
		Mutations: Mutations{DeleteEntityRequests: []EntityDeleteRequest{{
			ID: "host:1", Source: "agent",
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent delete: %v", err)
	}
	if _, ok := g.GetEntity("host:1"); !ok {
		t.Fatal("low priority delete removed manual entity")
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].ResourceType != "entity" || report.Suppressed[0].Field != "__existence__" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual-delete",
		Version: 3,
		Mutations: Mutations{DeleteEntityRequests: []EntityDeleteRequest{{
			ID: "host:1", Source: "manual",
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual delete: %v", err)
	}
	if _, ok := g.GetEntity("host:1"); ok {
		t.Fatal("high priority delete did not remove entity")
	}
}

func TestMarkSourceStaleMarksMissingAliasAndUpsertClearsIt(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:1", Kind: "host", Source: "aws", ExternalID: "i-1", SourceRank: 50},
			{ID: "host:2", Kind: "host", Source: "aws", ExternalID: "i-2", SourceRank: 50},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "stale",
		Version: 2,
		Mutations: Mutations{MarkSourceStale: []SourceStaleRequest{{
			Source: "aws", ObservedExternalIDs: []string{"i-1"},
		}}},
	}); err != nil {
		t.Fatalf("stale: %v", err)
	}
	entity, _ := g.GetEntity("host:2")
	if len(entity.Sources) != 1 || !entity.Sources[0].Stale {
		t.Fatalf("host:2 sources = %#v", entity.Sources)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "seen-again",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:2b", Kind: "host", Source: "aws", ExternalID: "i-2", SourceRank: 50, Fields: Fields{"seen": true},
		}}},
	}); err != nil {
		t.Fatalf("seen again: %v", err)
	}
	entity, _ = g.GetEntity("host:2")
	if entity.Sources[0].Stale {
		t.Fatalf("source should no longer be stale: %#v", entity.Sources)
	}
}

func TestSourceStaleDeleteUsesEntityExistencePriority(t *testing.T) {
	g := New()
	policy := entityPolicy()
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:manual", Kind: "host", Source: "manual", ExternalID: "m-1"},
			{ID: "host:agent", Kind: "host", Source: "agent", ExternalID: "a-1"},
		}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-stale-delete",
		Version: 2,
		Mutations: Mutations{MarkSourceStale: []SourceStaleRequest{{
			Source: "agent", Action: "delete",
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent stale delete: %v", err)
	}
	if _, ok := g.GetEntity("host:agent"); ok {
		t.Fatal("agent stale delete did not remove agent entity")
	}
	if _, ok := g.GetEntity("host:manual"); !ok {
		t.Fatal("agent stale delete removed manual entity")
	}
	if len(report.Suppressed) != 0 {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func entityPolicy() SourcePolicy {
	return SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
			{Name: "aws", Priority: 50},
		},
	}
}
