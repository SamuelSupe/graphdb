package graph

import (
	"fmt"
	"testing"
)

func BenchmarkMergeIsolatedEntitiesInLargeGraph(b *testing.B) {
	g := largeIsolatedMutationGraph(b, 10000, 50000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("merge-%d", i),
			Version: 1,
			Mutations: Mutations{
				MergeEntities: []MergeRequest{{
					TargetID:  "node:target",
					SourceIDs: []string{"node:source"},
				}},
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
