package graph

import "testing"

func TestRound4EntityExistenceSourcePromotedOnHigherPriorityUpsert(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "host:1", Kind: "host", Source: "scanner", ExternalID: "s-1", SourceRank: 10, Fields: Fields{"name": "low"}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "host:1", Kind: "host", Source: "cmdb", ExternalID: "c-1", SourceRank: 100, Fields: Fields{"name": "high"}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(Commit{
		Version: 3,
		Mutations: Mutations{
			DeleteEntityRequests: []EntityDeleteRequest{
				{ID: "host:1", Source: "scanner", SourceRank: 10},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.Entities.Get("host:1"); !ok {
		t.Fatal("low-priority source deleted entity owned by higher-priority authoritative source")
	}
}

func TestRound4IncomingEntityStripsClientMergedFrom(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "host:attacker", Kind: "host", MergedFrom: []string{"host:victim"}, SplitFrom: "host:victim"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	stored, ok := g.Entities.Get("host:attacker")
	if !ok {
		t.Fatal("expected host:attacker to exist")
	}
	if len(stored.MergedFrom) != 0 || stored.SplitFrom != "" {
		t.Fatalf("client-supplied MergedFrom/SplitFrom was not stripped: %+v", stored)
	}
	if resolved := g.ResolveEntityReference("host:victim"); resolved != "" {
		t.Fatalf("unsanitized MergedFrom hijacked alias host:victim -> %q", resolved)
	}
}
