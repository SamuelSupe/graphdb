package graph

import "testing"

func TestMergeRejectsNewCompositeIdentityCollision(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-composite-identities",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"region":   {Type: "string"},
					"hostname": {Type: "string"},
				},
				IdentityKeys: []IdentityKey{{
					Name:   "region-hostname",
					Fields: []string{"region", "hostname"},
				}},
			}},
			UpsertEntities: []Entity{
				{
					ID: "host:existing", Kind: "host",
					Fields: Fields{"region": "us-east", "hostname": "app-1"},
				},
				{
					ID: "host:target", Kind: "host",
					Fields: Fields{"region": "us-east"},
				},
				{
					ID: "host:source", Kind: "host",
					Fields: Fields{"hostname": "app-1"},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	err := g.ApplyCommit(Commit{
		ID:      "merge-composite-collision",
		Version: 2,
		Mutations: Mutations{
			MergeEntities: []MergeRequest{{
				TargetID:  "host:target",
				SourceIDs: []string{"host:source"},
			}},
		},
	})
	if err == nil {
		t.Fatal("merge accepted a newly formed duplicate composite identity")
	}
	if g.Version != 1 {
		t.Fatalf("failed merge changed graph version to %d", g.Version)
	}
	if _, ok := g.GetEntity("host:source"); !ok {
		t.Fatal("failed merge removed source entity")
	}
}

func TestBatchMergeTracksRedirectedEdgeFingerprint(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-merge-edges",
		Version: 1,
		Mutations: Mutations{
			UpsertRelationTypes: []RelationType{{
				Name:           "link",
				AllowCrossKind: true,
				Cardinality:    ManyToMany,
			}},
			UpsertEntities: []Entity{
				{ID: "node:final", Kind: "node"},
				{ID: "node:middle", Kind: "node"},
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

	next, report, err := g.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "batch-merge",
		Version: 2,
		Mutations: Mutations{
			MergeEntities: []MergeRequest{
				{
					TargetID:  "node:middle",
					SourceIDs: []string{"node:source"},
				},
				{
					TargetID:  "node:final",
					SourceIDs: []string{"node:middle"},
				},
			},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	edgeID := CanonicalEdgeIDParts("link", "node:final", "node:sink")
	if edge, ok := next.Edges[edgeID]; !ok ||
		edge.From != "node:final" ||
		edge.To != "node:sink" {
		t.Fatalf("redirected edge = %#v, found=%v", edge, ok)
	}

	recomputed := next.Clone()
	recomputed.contentFingerprintMu.Lock()
	recomputed.contentFingerprint = [16]byte{}
	recomputed.contentFingerprintReady = false
	recomputed.contentFingerprintMu.Unlock()
	want, err := recomputed.ContentFingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if report.ContentFingerprint != want {
		t.Fatalf(
			"incremental fingerprint = %q, recomputed = %q",
			report.ContentFingerprint,
			want,
		)
	}
}
