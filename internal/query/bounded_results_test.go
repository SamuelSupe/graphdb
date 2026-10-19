package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

var benchmarkBoundedResults []Result

func TestBoundedResultsMatchesFullSort(t *testing.T) {
	entities := []graph.Entity{
		{ID: "host:f", Fields: graph.Fields{"score": 2, "zone": "b"}},
		{ID: "host:b", Fields: graph.Fields{"score": 1, "zone": "a"}},
		{ID: "host:e", Fields: graph.Fields{"score": 2, "zone": "a"}},
		{ID: "host:a", Fields: graph.Fields{"score": 1, "zone": "a"}},
		{ID: "host:d", Fields: graph.Fields{"score": 3, "zone": "c"}},
		{ID: "host:c", Fields: graph.Fields{"score": 3, "zone": "b"}},
	}
	for i := 0; i < 64; i++ {
		entities = append(entities, graph.Entity{
			ID: fmt.Sprintf("host:%03d", i),
			Fields: graph.Fields{
				"score": (i * 37) % 17, "zone": fmt.Sprintf("zone-%d", i%3),
				graph.ReservedLabelsField: []any{fmt.Sprintf("label-%d", i%5)},
			},
			Identity:  map[string]any{"hostname": fmt.Sprintf("name-%d", (i*13)%11)},
			CreatedAt: time.Unix(int64((i*7)%19), 0).UTC(),
		})
	}
	all := make([]Result, len(entities))
	for i := range entities {
		all[i] = Result{Entity: &entities[i]}
	}

	for _, test := range []struct {
		name  string
		specs []SortSpec
		keep  int
	}{
		{name: "ascending", specs: []SortSpec{{Field: "score"}}, keep: 3},
		{name: "descending", specs: []SortSpec{{Field: "score", Desc: true}}, keep: 4},
		{name: "multiple", specs: []SortSpec{{Field: "score", Desc: true}, {Field: "zone"}}, keep: 5},
		{name: "one", specs: []SortSpec{{Field: "score"}}, keep: 1},
		{name: "all", specs: []SortSpec{{Field: "zone"}, {Field: "score"}}, keep: len(entities)},
		{name: "large-window", specs: []SortSpec{{Field: "zone"}, {Field: "score", Desc: true}}, keep: 10_000},
		{name: "id-fallback", keep: 13},
		{name: "explicit-fields", specs: []SortSpec{{Field: "fields.score"}, {Field: "fields.zone", Desc: true}}, keep: 17},
		{name: "identity", specs: []SortSpec{{Field: "identity.hostname", Desc: true}}, keep: 23},
		{name: "labels", specs: []SortSpec{{Field: "labels"}}, keep: 31},
		{name: "timestamp", specs: []SortSpec{{Field: "created_at", Desc: true}}, keep: 9},
		{name: "empty", specs: []SortSpec{{Field: "id"}}, keep: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected := append([]Result(nil), all...)
			sortResults(expected, test.specs)
			if len(expected) > test.keep {
				expected = expected[:test.keep]
			}
			bounded := newBoundedResults(test.specs, test.keep)
			for _, result := range all {
				bounded.Add(result)
			}
			actual := bounded.Sorted()
			if len(actual) != len(expected) {
				t.Fatalf("len(results) = %d, want %d", len(actual), len(expected))
			}
			for i := range expected {
				if resultIdentity(actual[i]) != resultIdentity(expected[i]) {
					t.Fatalf("result[%d] = %s, want %s", i, resultIdentity(actual[i]), resultIdentity(expected[i]))
				}
			}

			boundedEntities := newBoundedEntities(test.specs, test.keep)
			for _, entity := range entities {
				boundedEntities.Add(&entity)
			}
			actualEntities := boundedEntities.Sorted()
			if len(actualEntities) != len(expected) {
				t.Fatalf("len(entity results) = %d, want %d", len(actualEntities), len(expected))
			}
			for i := range expected {
				if actualEntities[i].ID != expected[i].Entity.ID {
					t.Fatalf("entity[%d] = %s, want %s", i, actualEntities[i].ID, expected[i].Entity.ID)
				}
			}
		})
	}
}

func BenchmarkBoundedResultsTopK(b *testing.B) {
	const count = 10_000
	const keep = 101
	entities := make([]graph.Entity, count)
	values := make([]Result, count)
	for i := range entities {
		entities[i] = graph.Entity{ID: fmt.Sprintf("host:%05d", i), Fields: graph.Fields{"score": (i * 7919) % count}}
		values[i] = Result{Entity: &entities[i]}
	}
	specs := []SortSpec{{Field: "score", Desc: true}}

	b.Run("repeated-sort", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			results := make([]Result, 0, keep)
			for _, result := range values {
				results = appendBoundedSortedReference(results, result, specs, keep)
			}
			benchmarkBoundedResults = results
		}
	})
	b.Run("bounded-heap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			results := newBoundedResults(specs, keep)
			for _, result := range values {
				results.Add(result)
			}
			benchmarkBoundedResults = results.Sorted()
		}
	})
}

func appendBoundedSortedReference(results []Result, result Result, specs []SortSpec, keep int) []Result {
	if keep <= 0 {
		return nil
	}
	if len(results) >= keep && compareResults(result, results[len(results)-1], specs) >= 0 {
		return results
	}
	results = append(results, result)
	sortResults(results, specs)
	if len(results) > keep {
		results = results[:keep]
	}
	return results
}

func TestMixedNumericSortHasConsistentPagesAndTopK(t *testing.T) {
	values := []any{float64(2), float64(10), "11", nil, false, "2"}
	for _, a := range values {
		for _, b := range values {
			for _, c := range values {
				if compareAny(a, b) < 0 && compareAny(b, c) < 0 && compareAny(a, c) >= 0 {
					t.Fatalf("nontransitive: %v < %v < %v", a, b, c)
				}
			}
		}
	}
	g := graph.New()
	var entities []graph.Entity
	for i, value := range values {
		entities = append(entities, graph.Entity{ID: fmt.Sprintf("item:%d", i), Kind: "item", Fields: graph.Fields{"value": value}})
	}
	if err := g.ApplyCommit(graph.Commit{ID: "mixed-sort", Version: 1, Mutations: graph.Mutations{UpsertEntities: entities}}); err != nil {
		t.Fatal(err)
	}
	for _, desc := range []bool{false, true} {
		request := Request{Op: "match", Kind: "item", Sort: []SortSpec{{Field: "value", Desc: desc}}, Limit: 100}
		all, err := Execute(g, request)
		if err != nil {
			t.Fatal(err)
		}
		request.Limit = 1
		for i, want := range all.Results {
			page, err := Execute(g, request)
			if err != nil || len(page.Results) != 1 || page.Results[0].Entity.ID != want.Entity.ID {
				t.Fatalf("desc=%v page=%d results=%+v err=%v", desc, i, page.Results, err)
			}
			request.Cursor = page.NextCursor
		}
		if request.Cursor != "" {
			t.Fatal("unexpected final cursor")
		}
	}
}
