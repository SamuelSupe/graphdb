package graph

import "testing"

func TestPreviewStorageEntityNoopPreservesLogicalNoopReport(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			Kind:       "host",
			Source:     "agent",
			ExternalID: "i-1",
			Fields:     Fields{"state": "ready"},
		}}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before, err := g.ContentFingerprint()
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	report, noop, err := g.PreviewStorageEntityNoop(Commit{
		ID:      "noop",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			Kind:       "host",
			Source:     "agent",
			ExternalID: "i-1",
			Fields:     Fields{"state": "ready"},
		}}},
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if !noop || report.Changed {
		t.Fatalf("preview = (%#v, %v), want logical noop", report, noop)
	}
	if report.ContentFingerprint != before {
		t.Fatalf("fingerprint = %q, want %q", report.ContentFingerprint, before)
	}
	if len(report.AffectedEntityIDs) != 1 {
		t.Fatalf("affected entities = %#v", report.AffectedEntityIDs)
	}
	if len(report.CanonicalEntities) != 1 ||
		report.CanonicalEntities[0].CanonicalID != report.AffectedEntityIDs[0] {
		t.Fatalf("canonical entities = %#v", report.CanonicalEntities)
	}
}

func TestPreviewStorageEntityNoopPreservesSuppressedConflict(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			Kind:       "host",
			Source:     "agent",
			ExternalID: "i-1",
			SourceRank: 10,
			Fields:     Fields{"state": "ready"},
		}}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	report, noop, err := g.PreviewStorageEntityNoop(Commit{
		ID:      "suppressed",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			Kind:       "host",
			Source:     "agent",
			ExternalID: "i-1",
			SourceRank: 1,
			Fields:     Fields{"state": "changed"},
		}}},
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if !noop {
		t.Fatal("suppressed field update was not recognized as a logical noop")
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "state" {
		t.Fatalf("suppressed conflicts = %#v", report.Suppressed)
	}
}

func TestPreviewStorageEntityNoopRejectsMixedMutation(t *testing.T) {
	g := New()
	_, noop, err := g.PreviewStorageEntityNoop(Commit{
		ID:      "mixed",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "host:a", Kind: "host"}},
			UpsertEdges:    []Edge{{Type: "link", From: "host:a", To: "host:b"}},
		},
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if noop {
		t.Fatal("mixed mutation was accepted by entity noop preview")
	}
}
