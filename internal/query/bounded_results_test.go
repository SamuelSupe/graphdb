package query

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
