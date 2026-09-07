package graph

import (
	"fmt"
	"testing"
)

func TestMatchEntityIDsAndFieldIndexIDsAreSorted(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:b", Kind: "host", Fields: Fields{"region": "us-east-1"}},
			{ID: "service:a", Kind: "service", Fields: Fields{"region": "us-east-1"}},
			{ID: "host:a", Kind: "host", Fields: Fields{"region": "us-west-2"}},
			{ID: "host:c", Kind: "host", Fields: Fields{"region": "us-east-1"}},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, []string{"host:a", "host:b", "host:c"}) {
		t.Fatalf("host ids = %#v", ids)
	}
	if ids := g.MatchFieldIndexIDs("host", "region", []any{"us-east-1", "us-east-1"}); !sameStrings(ids, []string{"host:b", "host:c"}) {
		t.Fatalf("field index ids = %#v", ids)
	}
	values := []any{"us-east-1", "us-west-2", "us-east-1"}
	if ids := g.MatchFieldIndexIDs("host", "region", values); !sameStrings(ids, []string{"host:a", "host:b", "host:c"}) {
		t.Fatalf("multi-value field index ids = %#v", ids)
	}
	if count := g.FieldIndexCount("host", "region", values); count != 3 {
		t.Fatalf("multi-value field index count = %d, want 3", count)
	}
}

func TestReadOrderCachesInvalidateAfterMutation(t *testing.T) {
	g := New()
	eastIDs := make([]string, 0, minCachedFieldIndexOrder)
	seedEntities := make([]Entity, 0, minCachedFieldIndexOrder)
	for i := 0; i < minCachedFieldIndexOrder; i++ {
		id := fmt.Sprintf("host:east-%02d", i)
		eastIDs = append(eastIDs, id)
		seedEntities = append(seedEntities, Entity{
			ID: id, Kind: "host", Fields: Fields{"region": "us-east-1"},
		})
	}
	if err := g.ApplyCommit(Commit{
		ID: "seed", Version: 1,
		Mutations: Mutations{UpsertEntities: seedEntities},
	}); err != nil {
		t.Fatal(err)
	}
	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, eastIDs) {
		t.Fatalf("initial ids = %#v", ids)
	}
	visited := make([]string, 0, len(eastIDs))
	count, err := g.VisitFieldIndexIDs("host", "region", []any{"us-east-1"}, func(id string) error {
		visited = append(visited, id)
		return nil
	})
	if err != nil || count != len(eastIDs) || !sameStrings(visited, eastIDs) {
		t.Fatalf("initial field index ids = %#v, err = %v", visited, err)
	}
	updatedEastIDs := append([]string(nil), eastIDs[1:]...)
	updatedEastIDs = append(updatedEastIDs, "host:east-new")
	updatedIDs := append([]string{"host:east-00"}, updatedEastIDs...)
	if err := g.ApplyCommit(Commit{
		ID: "update", Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:east-00", Kind: "host", Fields: Fields{"region": "us-west-2"}},
			{ID: "host:east-new", Kind: "host", Fields: Fields{"region": "us-east-1"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, updatedIDs) {
		t.Fatalf("updated ids = %#v", ids)
	}
	visited = visited[:0]
	count, err = g.VisitFieldIndexIDs("host", "region", []any{"us-east-1", "us-west-2"}, func(id string) error {
		visited = append(visited, id)
		return nil
	})
	if err != nil || count != len(updatedIDs) || !sameStrings(visited, updatedIDs) {
		t.Fatalf("updated field index ids = %#v, err = %v", visited, err)
	}
}

func TestStorageCopyReadOrderCachesAreVersionIsolated(t *testing.T) {
	source := storageCopyReadOrderFixture(t, 64)
	warmReadOrderCaches(source)

	baseHosts := makeEntityIDs("host:", 64)
	baseEast := append([]string(nil), baseHosts...)
	basePlatform := append([]string(nil), baseHosts...)
	assertReadOrderResults(t, source, baseHosts, baseEast, basePlatform)

	t.Run("field update", func(t *testing.T) {
		next := applyStorageCopyReadOrderCommit(t, source, 2, Mutations{
			UpsertEntities: []Entity{{
				ID: "host:000", Kind: "host",
				Fields: Fields{"region": "us-west-2", "owner": "platform"},
			}},
		})
		assertReadOrderResults(t, source, baseHosts, baseEast, basePlatform)
		assertReadOrderResults(t, next, baseHosts, withoutID(baseEast, "host:000"), basePlatform)
		if ids := next.MatchFieldIndexIDs("host", "region", []any{"us-west-2"}); !sameStrings(ids, []string{"host:000"}) {
			t.Fatalf("updated region ids = %#v", ids)
		}
	})

	t.Run("entity add", func(t *testing.T) {
		next := applyStorageCopyReadOrderCommit(t, source, 2, Mutations{
			UpsertEntities: []Entity{{
				ID: "host:064", Kind: "host",
				Fields: Fields{"region": "us-east-1", "owner": "platform"},
			}},
		})
		expected := append(append([]string(nil), baseHosts...), "host:064")
		assertReadOrderResults(t, source, baseHosts, baseEast, basePlatform)
		assertReadOrderResults(t, next, expected, expected, expected)
	})

	t.Run("entity delete", func(t *testing.T) {
		next := applyStorageCopyReadOrderCommit(t, source, 2, Mutations{
			DeleteEntities: []string{"host:063"},
		})
		expected := withoutID(baseHosts, "host:063")
		assertReadOrderResults(t, source, baseHosts, baseEast, basePlatform)
		assertReadOrderResults(t, next, expected, expected, expected)
	})

}

func BenchmarkStorageCopyFirstKindPageAfterFieldUpdate(b *testing.B) {
	for _, entityCount := range []int{10_000, 100_000} {
		entityCount := entityCount
		b.Run(fmt.Sprintf("entities-%d", entityCount), func(b *testing.B) {
			source := New()
			entities := make([]Entity, 0, entityCount)
			for i := 0; i < entityCount; i++ {
				entities = append(entities, Entity{
					ID: fmt.Sprintf("host:%06d", i), Kind: "host",
					Fields: Fields{"payload": i},
				})
			}
			if err := source.ApplyCommit(Commit{
				ID: "benchmark-seed", Version: 1,
				Mutations: Mutations{UpsertEntities: entities},
			}); err != nil {
				b.Fatalf("seed: %v", err)
			}
			source.MatchEntityIDs("host")
			commit := Commit{
				ID: "benchmark-field-update", Version: 2,
				Mutations: Mutations{UpsertEntities: []Entity{{
					ID: "host:000000", Kind: "host", Fields: Fields{"payload": -1},
				}}},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				next, _, err := source.ApplyCommitStorageCopyWithOptions(commit, ApplyOptions{})
				if err != nil {
					b.Fatalf("storage copy: %v", err)
				}
				b.StartTimer()
				visited := 0
				err = next.VisitEntitiesByID("host", "", func(Entity) (bool, error) {
					visited++
					return visited < 11, nil
				})
				b.StopTimer()
				if err != nil {
					b.Fatalf("visit first kind page: %v", err)
				}
				if visited != 11 {
					b.Fatalf("visited = %d, want 11", visited)
				}
			}
		})
	}
}

func storageCopyReadOrderFixture(t *testing.T, count int) *Graph {
	t.Helper()
	g := New()
	entities := make([]Entity, 0, count)
	for i := 0; i < count; i++ {
		entities = append(entities, Entity{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
			Fields: Fields{"region": "us-east-1", "owner": "platform"},
		})
	}
	if err := g.ApplyCommit(Commit{
		ID: "seed-read-order", Version: 1,
		Mutations: Mutations{UpsertEntities: entities},
	}); err != nil {
		t.Fatalf("seed read-order fixture: %v", err)
	}
	return g
}

func warmReadOrderCaches(g *Graph) {
	_ = g.MatchEntityIDs("")
	_ = g.MatchEntityIDs("host")
	_ = g.MatchFieldIndexIDs("host", "region", []any{"us-east-1"})
	_ = g.MatchFieldIndexIDs("host", "owner", []any{"platform"})
}

func applyStorageCopyReadOrderCommit(t *testing.T, source *Graph, version int64, mutations Mutations) *Graph {
	t.Helper()
	next, _, err := source.ApplyCommitStorageCopyWithOptions(Commit{
		ID: fmt.Sprintf("read-order-%d", version), Version: version, Mutations: mutations,
	}, ApplyOptions{})
	if err != nil {
		t.Fatalf("storage copy version %d: %v", version, err)
	}
	return next
}

func assertReadOrderResults(t *testing.T, g *Graph, wantAll, wantRegion, wantOwner []string) {
	t.Helper()
	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, wantAll) {
		t.Fatalf("host ids = %#v, want %#v", ids, wantAll)
	}
	if ids := g.MatchFieldIndexIDs("host", "region", []any{"us-east-1"}); !sameStrings(ids, wantRegion) {
		t.Fatalf("region ids = %#v, want %#v", ids, wantRegion)
	}
	if ids := g.MatchFieldIndexIDs("host", "owner", []any{"platform"}); !sameStrings(ids, wantOwner) {
		t.Fatalf("owner ids = %#v, want %#v", ids, wantOwner)
	}
}

func makeEntityIDs(prefix string, count int) []string {
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		ids = append(ids, fmt.Sprintf("%s%03d", prefix, i))
	}
	return ids
}

func withoutID(ids []string, excluded string) []string {
	filtered := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != excluded {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

func sameStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
