package graph

import "testing"

func TestMergeAndSplitReportAffectedResources(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-affected-resources",
		Version: 1,
		Mutations: Mutations{
			UpsertRelationTypes: []RelationType{{
				Name:           "link",
				AllowCrossKind: true,
				Cardinality:    ManyToMany,
			}},
			UpsertEntities: []Entity{
				{ID: "node:target", Kind: "node"},
				{ID: "node:source", Kind: "node"},
				{ID: "node:sink", Kind: "node"},
			},
			UpsertEdges: []Edge{{
				Type: "link", From: "node:source", To: "node:sink",
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	oldEdgeID := CanonicalEdgeIDParts(
		"link", "node:source", "node:sink",
	)

	next, mergeReport, err := g.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "merge-affected-resources",
		Version: 2,
		Mutations: Mutations{
			MergeEntities: []MergeRequest{{
				TargetID:  "node:target",
				SourceIDs: []string{"node:source"},
			}},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newEdgeID := CanonicalEdgeIDParts(
		"link", "node:target", "node:sink",
	)
	assertStringSet(t, mergeReport.AffectedEntityIDs, map[string]struct{}{
		"node:target": {}, "node:source": {},
	})
	assertStringSet(t, mergeReport.AffectedEdgeIDs, map[string]struct{}{
		oldEdgeID: {}, newEdgeID: {},
	})

	_, splitReport, err := next.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "split-affected-resources",
		Version: 3,
		Mutations: Mutations{
			SplitEntities: []SplitRequest{{
				SourceID: "node:target",
				Entities: []Entity{
					{ID: "node:left", Kind: "node"},
					{ID: "node:right", Kind: "node"},
				},
			}},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertStringSet(t, splitReport.AffectedEntityIDs, map[string]struct{}{
		"node:target": {}, "node:left": {}, "node:right": {},
	})
	assertStringSet(t, splitReport.AffectedEdgeIDs, map[string]struct{}{
		newEdgeID: {},
	})
}

func assertStringSet(
	t *testing.T,
	got []string,
	want map[string]struct{},
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values = %#v, want %#v", got, want)
	}
	for _, value := range got {
		if _, ok := want[value]; !ok {
			t.Fatalf("unexpected value %q in %#v", value, got)
		}
	}
}
