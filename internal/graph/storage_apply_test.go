package graph

import "testing"

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
	beforeHash, err := source.ContentMD5()
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
	afterHash, err := source.ContentMD5()
	if err != nil {
		t.Fatalf("hash source after copy: %v", err)
	}
	if afterHash != beforeHash {
		t.Fatalf("source hash changed from %q to %q", beforeHash, afterHash)
	}
}

func TestApplyCommitStorageCopyFailureDoesNotLeakMutations(t *testing.T) {
	source := graphWithCompany(t)
	beforeHash, err := source.ContentMD5()
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
	afterHash, hashErr := source.ContentMD5()
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
