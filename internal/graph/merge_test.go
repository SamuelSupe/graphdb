package graph

import "testing"

func TestManualMergeAndSplitMutations(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:a", Kind: "host", Fields: Fields{"hostname": "a"}},
			{ID: "host:b", Kind: "host", Fields: Fields{"hostname": "b"}},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "merge",
		Version: 2,
		Mutations: Mutations{MergeEntities: []MergeRequest{{
			TargetID:  "host:a",
			SourceIDs: []string{"host:b"},
		}}},
	}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("merged source still exists")
	}
	merged, _ := g.GetEntity("host:a")
	if len(merged.MergedFrom) != 1 || merged.MergedFrom[0] != "host:b" {
		t.Fatalf("merged_from = %#v", merged.MergedFrom)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "split",
		Version: 3,
		Mutations: Mutations{SplitEntities: []SplitRequest{{
			SourceID: "host:a",
			Entities: []Entity{
				{ID: "host:a1", Kind: "host", Fields: Fields{"hostname": "a1"}},
				{ID: "host:a2", Kind: "host", Fields: Fields{"hostname": "a2"}},
			},
		}}},
	}); err != nil {
		t.Fatalf("split: %v", err)
	}
	if _, ok := g.GetEntity("host:a"); ok {
		t.Fatal("split source still exists")
	}
	a1, ok := g.GetEntity("host:a1")
	if !ok || a1.SplitFrom != "host:a" {
		t.Fatalf("split replacement missing split_from: %#v", a1)
	}
}

func TestMergeAndSplitMaintainFieldSources(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:a", Kind: "host", Source: "manual", SourceRank: 1000, Fields: Fields{"owner": "platform"}},
			{ID: "host:b", Kind: "host", Source: "agent", SourceRank: 100, Fields: Fields{"region": "us-east-1"}},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "merge",
		Version: 2,
		Mutations: Mutations{MergeEntities: []MergeRequest{{
			TargetID:  "host:a",
			SourceIDs: []string{"host:b"},
		}}},
	}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	merged, _ := g.GetEntity("host:a")
	if merged.Fields["region"] != "us-east-1" {
		t.Fatalf("region was not merged: %#v", merged.Fields)
	}
	if merged.FieldSources["region"].Source != "agent" || merged.FieldSources["region"].Priority != 100 {
		t.Fatalf("merged field source = %#v", merged.FieldSources["region"])
	}

	if err := g.ApplyCommit(Commit{
		ID:      "split",
		Version: 3,
		Mutations: Mutations{SplitEntities: []SplitRequest{{
			SourceID: "host:a",
			Entities: []Entity{{
				ID: "host:a1", Kind: "host", Source: "manual", SourceRank: 1000, Fields: Fields{"owner": "platform"},
			}},
		}}},
	}); err != nil {
		t.Fatalf("split: %v", err)
	}
	split, _ := g.GetEntity("host:a1")
	if split.FieldSources["owner"].Source != "manual" || split.FieldSources["owner"].Priority != 1000 {
		t.Fatalf("split field source = %#v", split.FieldSources["owner"])
	}
}

func TestManualMergeKeepsHigherPriorityTargetField(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{
				ID: "host:a", Kind: "host", Source: "manual", SourceRank: 1000, Confidence: 0.1,
				Fields: Fields{"owner": "platform"},
			},
			{
				ID: "host:b", Kind: "host", Source: "agent", SourceRank: 999, Confidence: 1,
				Fields: Fields{"owner": "collector", "region": "us-east-1"},
			},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "merge",
		Version: 2,
		Mutations: Mutations{MergeEntities: []MergeRequest{{
			TargetID:  "host:a",
			SourceIDs: []string{"host:b"},
		}}},
	}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	merged, _ := g.GetEntity("host:a")
	if merged.Fields["owner"] != "platform" {
		t.Fatalf("lower-priority high-confidence field overwrote target: %#v", merged.Fields)
	}
	if merged.Fields["region"] != "us-east-1" {
		t.Fatalf("missing source-only field after merge: %#v", merged.Fields)
	}
	if merged.FieldSources["owner"].Priority != 1000 {
		t.Fatalf("owner field source = %#v", merged.FieldSources["owner"])
	}
}

func TestMergeRejectsCardinalityViolationAcrossSourceEdges(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-cardinality-merge",
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
				{ID: "person:target", Kind: "person"},
				{ID: "person:left", Kind: "person"},
				{ID: "person:right", Kind: "person"},
				{ID: "company:left", Kind: "company"},
				{ID: "company:right", Kind: "company"},
			},
			UpsertEdges: []Edge{
				{Type: "works_at", From: "person:left", To: "company:left"},
				{Type: "works_at", From: "person:right", To: "company:right"},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := g.ApplyCommit(Commit{
		ID:      "bad-merge",
		Version: 2,
		Mutations: Mutations{MergeEntities: []MergeRequest{{
			TargetID:  "person:target",
			SourceIDs: []string{"person:left", "person:right"},
		}}},
	})
	if err == nil {
		t.Fatal("expected merge to reject resulting many_to_one violation")
	}
	if g.Version != 1 {
		t.Fatalf("version = %d, want original version", g.Version)
	}
	if _, ok := g.GetEntity("person:left"); !ok {
		t.Fatal("failed merge removed source entity")
	}
	if len(g.Neighbors("person:target", "out", "works_at")) != 0 {
		t.Fatal("failed merge leaked rewritten edges onto target")
	}
}

func TestSplitRejectsDuplicateIdentityReplacement(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-identity-split",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name:   "host",
				Fields: map[string]FieldSpec{"hostname": {Type: "string"}},
				IdentityKeys: []IdentityKey{{
					Name:   "hostname",
					Fields: []string{"hostname"},
				}},
			}},
			UpsertEntities: []Entity{
				{ID: "host:source", Kind: "host", Fields: Fields{"hostname": "source"}},
				{ID: "host:existing", Kind: "host", Fields: Fields{"hostname": "app-01"}},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := g.ApplyCommit(Commit{
		ID:      "bad-split-identity",
		Version: 2,
		Mutations: Mutations{SplitEntities: []SplitRequest{{
			SourceID: "host:source",
			Entities: []Entity{{
				ID: "host:replacement", Kind: "host", Fields: Fields{"hostname": "app-01"},
			}},
		}}},
	})
	if err == nil {
		t.Fatal("expected split duplicate identity error")
	}
	if g.Version != 1 {
		t.Fatalf("version = %d, want original version", g.Version)
	}
	if _, ok := g.GetEntity("host:source"); !ok {
		t.Fatal("failed split removed source entity")
	}
	if _, ok := g.GetEntity("host:replacement"); ok {
		t.Fatal("failed split leaked replacement entity")
	}
}

func TestSplitReplacementCannotSpoofSourceAlias(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-source-alias-split",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{
				ID: "host:manual", Kind: "host", Source: "manual", ExternalID: "manual-raw",
				SourceRank: 1000, Fields: Fields{"owner": "platform"},
			},
			{ID: "host:source", Kind: "host", Fields: Fields{"hostname": "source"}},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "split-spoof",
		Version: 2,
		Mutations: Mutations{SplitEntities: []SplitRequest{{
			SourceID: "host:source",
			Entities: []Entity{{
				ID: "host:replacement", Kind: "host", Source: "agent", ExternalID: "agent-raw",
				Sources: []EntitySource{{Source: "manual", ExternalID: "manual-raw", Priority: 1000}},
				Fields:  Fields{"hostname": "replacement"},
			}},
		}}},
	}); err != nil {
		t.Fatalf("split: %v", err)
	}
	replacement, ok := g.GetEntity("host:replacement")
	if !ok {
		t.Fatal("replacement missing")
	}
	if len(replacement.Sources) != 1 || replacement.Sources[0].Source != "agent" || replacement.Sources[0].ExternalID != "agent-raw" {
		t.Fatalf("replacement sources = %#v, want sanitized top-level source only", replacement.Sources)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "manual-alias",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:manual-update", Kind: "host", Source: "manual", ExternalID: "manual-raw",
			Fields: Fields{"rack": "r1"},
		}}},
	}); err != nil {
		t.Fatalf("manual alias update: %v", err)
	}
	manual, _ := g.GetEntity("host:manual")
	if manual.Fields["rack"] != "r1" {
		t.Fatalf("manual alias update did not hit manual entity: %#v", manual)
	}
	replacement, _ = g.GetEntity("host:replacement")
	if _, ok := replacement.Fields["rack"]; ok {
		t.Fatalf("spoofed source alias merged into replacement: %#v", replacement)
	}
}
