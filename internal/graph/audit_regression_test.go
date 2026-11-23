package graph

import (
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
