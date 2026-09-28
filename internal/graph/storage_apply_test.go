package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"testing"
	"time"
)

func TestAffectedEntityIDsRemainUniqueInInsertionOrder(t *testing.T) {
	g := New()
	report, err := g.ApplyCommitWithOptions(Commit{ID: "affected", Version: 1, Mutations: Mutations{
		UpsertEntities: []Entity{
			{ID: "host:b", Kind: "host"},
			{ID: "host:a", Kind: "host"},
			{ID: "host:b", Kind: "host"},
		},
	}}, ApplyOptions{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(report.AffectedEntityIDs) != 2 || report.AffectedEntityIDs[0] != "host:b" || report.AffectedEntityIDs[1] != "host:a" {
		t.Fatalf("affected ids = %#v, want [host:b host:a]", report.AffectedEntityIDs)
	}
}

func TestApplyCommitStorageCopyKeepsSourceGraphImmutable(t *testing.T) {
	source := graphWithCompany(t)
	if err := source.ApplyCommit(Commit{ID: "add-age", Version: 2, Mutations: Mutations{UpsertEntities: []Entity{{
		ID: "person:alice", Kind: "person", Fields: Fields{"age": 31},
	}}}}); err != nil {
		t.Fatalf("add source age: %v", err)
	}
	beforeHash, err := source.ContentHash()
	if err != nil {
		t.Fatalf("hash source: %v", err)
	}

	next, report, err := source.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "storage-copy",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "person:alice", Kind: "person", Fields: Fields{"age": 32},
		}}},
	}, ApplyOptions{})
	if err != nil {
		t.Fatalf("apply storage copy: %v", err)
	}
	if len(report.AffectedEntityIDs) != 1 || report.AffectedEntityIDs[0] != "person:alice" {
		t.Fatalf("affected ids = %#v", report.AffectedEntityIDs)
	}
	if got := source.MatchFieldIndex("person", "age", []any{float64(31)}); len(got) != 1 {
		t.Fatalf("source age index changed: %#v", got)
	}
	if got := source.MatchFieldIndex("person", "age", []any{float64(32)}); len(got) != 0 {
		t.Fatalf("source gained next age index: %#v", got)
	}
	if got := next.MatchFieldIndex("person", "age", []any{float64(32)}); len(got) != 1 {
		t.Fatalf("next age index = %#v", got)
	}
	afterHash, err := source.ContentHash()
	if err != nil {
		t.Fatalf("hash source after copy: %v", err)
	}
	if afterHash != beforeHash {
		t.Fatalf("source hash changed from %q to %q", beforeHash, afterHash)
	}
}

func TestApplyCommitStorageCopyFailureDoesNotLeakMutations(t *testing.T) {
	source := graphWithCompany(t)
	beforeHash, err := source.ContentHash()
	if err != nil {
		t.Fatalf("hash source: %v", err)
	}

	_, _, err = source.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "bad-storage-copy",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "person:alice", Kind: "person", Fields: Fields{"age": 32}}},
			UpsertEdges:    []Edge{{Type: "works_at", From: "person:alice", To: "company:missing"}},
		},
	}, ApplyOptions{})
	if err == nil {
		t.Fatal("expected invalid edge error")
	}
	afterHash, hashErr := source.ContentHash()
	if hashErr != nil {
		t.Fatalf("hash source after failure: %v", hashErr)
	}
	if afterHash != beforeHash {
		t.Fatalf("failed storage copy changed source hash from %q to %q", beforeHash, afterHash)
	}
}

func TestApplyCommitStorageCopyDoesNotShareStaleSourceMutation(t *testing.T) {
	source := New()
	if err := source.ApplyCommit(Commit{ID: "seed", Version: 1, Mutations: Mutations{UpsertEntities: []Entity{{
		ID: "host:a", Kind: "host", Sources: []EntitySource{{Source: "agent", ExternalID: "a"}},
	}}}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	next, _, err := source.ApplyCommitStorageCopyWithOptions(Commit{ID: "stale", Version: 2, Mutations: Mutations{
		MarkSourceStale: []SourceStaleRequest{{Source: "agent", Action: "mark_stale"}},
	}}, ApplyOptions{})
	if err != nil {
		t.Fatalf("mark stale: %v", err)
	}
	original, _ := source.GetEntity("host:a")
	updated, _ := next.GetEntity("host:a")
	if original.Sources[0].Stale {
		t.Fatal("storage copy mutated source entity sources")
	}
	if !updated.Sources[0].Stale {
		t.Fatal("storage copy did not mark next entity source stale")
	}
}

func TestApplyCommitStorageCopyMutationClassesKeepSourceImmutable(t *testing.T) {
	cases := []struct {
		name      string
		mutations func(*Graph) Mutations
	}{
		{
			name: "ci type",
			mutations: func(g *Graph) Mutations {
				updated := g.CITypes["node"]
				updated.DisplayName = "updated"
				return Mutations{UpsertCITypes: []CIType{updated}}
			},
		},
		{
			name: "relation type",
			mutations: func(*Graph) Mutations {
				return Mutations{UpsertRelationTypes: []RelationType{{
					Name: "unused", FromKind: "node", ToKind: "node",
				}}}
			},
		},
		{
			name: "entity",
			mutations: func(*Graph) Mutations {
				return Mutations{UpsertEntities: []Entity{{
					ID: "node:a", Kind: "node", Fields: Fields{"state": "updated"},
				}}}
			},
		},
		{
			name: "edge",
			mutations: func(*Graph) Mutations {
				return Mutations{UpsertEdges: []Edge{{
					Type: "link", From: "node:a", To: "node:b",
					Fields: Fields{"state": "updated"},
				}}}
			},
		},
		{
			name: "edge delete",
			mutations: func(*Graph) Mutations {
				return Mutations{DeleteEdges: []string{
					CanonicalEdgeIDParts("link", "node:a", "node:b"),
				}}
			},
		},
		{
			name: "entity delete",
			mutations: func(*Graph) Mutations {
				return Mutations{DeleteEntities: []string{"node:a"}}
			},
		},
		{
			name: "relation type delete",
			mutations: func(*Graph) Mutations {
				return Mutations{DeleteRelationTypes: []string{"link"}}
			},
		},
		{
			name: "source stale delete",
			mutations: func(*Graph) Mutations {
				return Mutations{MarkSourceStale: []SourceStaleRequest{{
					Source: "agent", Action: "delete",
				}}}
			},
		},
		{
			name: "merge",
			mutations: func(*Graph) Mutations {
				return Mutations{MergeEntities: []MergeRequest{{
					TargetID: "node:a", SourceIDs: []string{"node:c"},
				}}}
			},
		},
		{
			name: "split",
			mutations: func(*Graph) Mutations {
				return Mutations{SplitEntities: []SplitRequest{{
					SourceID: "node:c",
					Entities: []Entity{
						{ID: "node:x", Kind: "node"},
						{ID: "node:y", Kind: "node"},
					},
				}}}
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := storageCopyMutationFixture(t)
			before := source.Snapshot()
			borrowed := source.SnapshotForStorage()
			borrowedBefore, _ := json.Marshal(borrowed)
			updated, report, err := source.ApplyCommitStorageCopyWithOptions(Commit{
				ID:        "storage-copy-" + test.name,
				Version:   2,
				Mutations: test.mutations(source),
			}, ApplyOptions{})
			if err != nil {
				t.Fatalf("apply storage copy: %v", err)
			}
			if !report.Changed {
				t.Fatal("mutation did not change the copied graph")
			}
			borrowedAfter, _ := json.Marshal(borrowed)
			if !bytes.Equal(borrowedBefore, borrowedAfter) {
				t.Fatal("borrowed snapshot changed across a commit")
			}
			for _, g := range []*Graph{source, updated} {
				entities := NewShardedMap[Entity]()
				for shard := 0; shard < 64; shard++ {
					if err := g.VisitEntityStorageShard(fmt.Sprintf("%02x", shard), func(entity Entity) error {
						entities.Set(entity.ID, entity)
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(maps.Collect(entities.All()), maps.Collect(g.Entities.All())) {
					t.Fatal("partition membership lost or retained an entity")
				}
				for _, reverse := range []bool{false, true} {
					edges := NewShardedMap[Edge]()
					for kind := range g.RelationTypes {
						for shard := 0; shard < 64; shard++ {
							if err := g.VisitEdgeStorageShard(context.Background(), kind, fmt.Sprintf("%02x", shard), reverse, func(edge Edge) error {
								edges.Set(edge.ID, edge)
								return nil
							}); err != nil {
								t.Fatal(err)
							}
						}
					}
					if !reflect.DeepEqual(maps.Collect(edges.All()), maps.Collect(g.Edges.All())) {
						t.Fatal("partition adjacency differs from graph edges")
					}
				}
			}
			if after := source.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("storage copy mutated source graph: before=%#v after=%#v", before, after)
			}
		})
	}
}

func storageCopyMutationFixture(t *testing.T) *Graph {
	t.Helper()
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-storage-copy-classes",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "node",
				Fields: map[string]FieldSpec{
					"state": {Type: "string"},
				},
			}},
			UpsertRelationTypes: []RelationType{{
				Name: "link", FromKind: "node", ToKind: "node",
				Cardinality: ManyToMany,
			}},
			UpsertEntities: []Entity{
				{
					ID: "node:a", Kind: "node", Source: "agent",
					ExternalID: "a", Fields: Fields{"state": "ready"},
				},
				{ID: "node:b", Kind: "node", Fields: Fields{"state": "ready"}},
				{ID: "node:c", Kind: "node", Fields: Fields{"state": "ready"}},
			},
			UpsertEdges: []Edge{
				{Type: "link", From: "node:a", To: "node:b"},
				{Type: "link", From: "node:c", To: "node:b"},
			},
		},
	}); err != nil {
		t.Fatalf("seed storage copy fixture: %v", err)
	}
	return g
}

func TestApplyCommitInPlaceForStorageReplaysPrivateGraph(t *testing.T) {
	g := New()
	if err := g.ApplyCommitInPlaceForStorage(Commit{ID: "replay", Version: 1, Mutations: Mutations{
		UpsertEntities: []Entity{{ID: "host:a", Kind: "host"}},
	}}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if g.Version != 1 {
		t.Fatalf("version = %d, want 1", g.Version)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatal("replayed entity missing")
	}
}

func TestApplyCommitBatchStorageCopyWithOptionsPreservesOrder(t *testing.T) {
	source := New()
	next, reports, err := source.ApplyCommitBatchStorageCopyWithOptions([]Commit{
		{
			ID:        "upsert",
			Version:   1,
			CreatedAt: time.Unix(1, 0).UTC(),
			Mutations: Mutations{UpsertEntities: []Entity{{ID: "host:1", Kind: "host"}}},
		},
		{
			ID:        "delete",
			Version:   2,
			CreatedAt: time.Unix(2, 0).UTC(),
			Mutations: Mutations{DeleteEntities: []string{"host:1"}},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 || len(reports) != 2 || !reports[0].Changed || !reports[1].Changed {
		t.Fatalf("batch result version/reports = %d/%#v", next.Version, reports)
	}
	if _, ok := next.GetEntity("host:1"); ok {
		t.Fatal("upsert then delete was reordered")
	}
	if source.Version != 0 || source.Entities.Len() != 0 {
		t.Fatal("batch apply mutated the source graph")
	}
}

func TestApplyCommitBatchStorageCopyRequiresIsolationForNoop(t *testing.T) {
	source := New()
	if err := source.ApplyCommit(Commit{
		ID:        "seed",
		Version:   1,
		CreatedAt: time.Unix(1, 0).UTC(),
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Fields: Fields{"name": "app"},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	_, _, err := source.ApplyCommitBatchStorageCopyWithOptions([]Commit{{
		ID:        "noop",
		Version:   2,
		CreatedAt: time.Unix(2, 0).UTC(),
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Fields: Fields{"name": "app"},
		}}},
	}}, nil)
	if !errors.Is(err, ErrBatchApplyRequiresIsolation) {
		t.Fatalf("batch err = %v, want ErrBatchApplyRequiresIsolation", err)
	}
	if source.Version != 1 {
		t.Fatalf("source version = %d, want 1", source.Version)
	}
}
