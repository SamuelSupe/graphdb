package graph

import "testing"

func TestSourcePolicyFieldOwnershipSuppressesLowerPriority(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"owner": "collector"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("aws write: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"owner": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws-again",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"owner": "collector-2"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("aws rewrite: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "platform" {
		t.Fatalf("owner = %#v", entity.Fields["owner"])
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "owner" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
	if entity.FieldSources["owner"].Source != "manual" || entity.FieldSources["owner"].Priority != 1000 {
		t.Fatalf("field source = %#v", entity.FieldSources["owner"])
	}
}

func TestIncomingEntitySourcesCannotElevateWriteOwner(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"owner": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-spoof",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent", Fields: Fields{"owner": "collector"},
			Sources: []EntitySource{{Source: "manual", ExternalID: "spoofed-manual"}},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent spoof write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "platform" {
		t.Fatalf("spoofed source elevated write owner: %#v", entity)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].IncomingSource != "agent" || report.Suppressed[0].IncomingPriority != 100 {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestIncomingEntitySourcesCannotSpoofIdentityAlias(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:manual", Kind: "host", Source: "manual", ExternalID: "manual-raw",
			SourceRank: 1000, Fields: Fields{"owner": "platform"},
		}}},
	}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "agent-spoof",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:agent", Kind: "host", Source: "agent", ExternalID: "agent-raw",
			Sources: []EntitySource{{Source: "manual", ExternalID: "manual-raw", Priority: 1000}},
			Fields:  Fields{"owner": "collector"},
		}}},
	}); err != nil {
		t.Fatalf("agent spoof write: %v", err)
	}
	manual, ok := g.GetEntity("host:manual")
	if !ok || manual.Fields["owner"] != "platform" {
		t.Fatalf("manual entity = %#v ok=%v", manual, ok)
	}
	agent, ok := g.GetEntity("host:agent")
	if !ok || agent.Fields["owner"] != "collector" {
		t.Fatalf("agent entity = %#v ok=%v", agent, ok)
	}
	if len(g.Entities) != 2 {
		t.Fatalf("entities = %#v, spoofed source alias merged into manual entity", g.Entities)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "manual-alias",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:manual-rename", Kind: "host", Source: "manual", ExternalID: "manual-raw",
			Fields: Fields{"rack": "r1"},
		}}},
	}); err != nil {
		t.Fatalf("top-level alias merge: %v", err)
	}
	manual, _ = g.GetEntity("host:manual")
	if len(g.Entities) != 2 || manual.Fields["rack"] != "r1" {
		t.Fatalf("top-level source alias did not merge: entities=%#v manual=%#v", g.Entities, manual)
	}
}

func TestEqualPriorityHigherConfidenceWins(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "low",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent-a", SourceRank: 10, Confidence: 0.4, Fields: Fields{"region": "old"},
		}}},
	}); err != nil {
		t.Fatalf("low confidence: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "high",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent-b", SourceRank: 10, Confidence: 0.9, Fields: Fields{"region": "new"},
		}}},
	}); err != nil {
		t.Fatalf("high confidence: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["region"] != "new" {
		t.Fatalf("region = %#v", entity.Fields["region"])
	}
}

func TestEntitySourceMergeKeepsHigherPriorityAliasRecord(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "high-priority",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "collector", ExternalID: "raw-host",
			SourceRank: 1000, Confidence: 0.1, Fields: Fields{"region": "manual"},
		}}},
	}); err != nil {
		t.Fatalf("high priority: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "lower-priority-confidence",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "collector", ExternalID: "raw-host",
			SourceRank: 100, Confidence: 1, Fields: Fields{"zone": "collector-zone"},
		}}},
	}); err != nil {
		t.Fatalf("lower priority: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if len(entity.Sources) != 1 {
		t.Fatalf("sources = %#v", entity.Sources)
	}
	if entity.Sources[0].Priority != 1000 || entity.Sources[0].Confidence != 0.1 {
		t.Fatalf("source alias was downgraded by lower priority write: %#v", entity.Sources[0])
	}
}

func TestEmptyIncomingFieldDoesNotClearExistingValue(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"owner": "collector", "team": ""},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("aws write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual-empty",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"owner": "", "team": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("manual write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "collector" {
		t.Fatalf("owner was cleared: %#v", entity.Fields)
	}
	if entity.Fields["team"] != "platform" {
		t.Fatalf("empty field was not filled: %#v", entity.Fields)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "owner" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestNoPolicyKeepsSourcePriorityBehaviorAndSnapshotFieldSources(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "low",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", SourceRank: 10, Fields: Fields{"owner": "collector"},
		}}},
	}); err != nil {
		t.Fatalf("low priority: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "high",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", SourceRank: 100, Fields: Fields{"owner": "manual"},
		}}},
	}); err != nil {
		t.Fatalf("high priority: %v", err)
	}
	loaded, err := FromSnapshot(g.Snapshot())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	entity, _ := loaded.GetEntity("host:1")
	if entity.Fields["owner"] != "manual" || entity.FieldSources["owner"].Source != "manual" {
		t.Fatalf("entity after snapshot = %#v", entity)
	}
}
