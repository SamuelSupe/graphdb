package graph

import (
	"fmt"
	"testing"
)

func BenchmarkSplitIsolatedEntityInLargeGraph(b *testing.B) {
	g := largeIsolatedMutationGraph(b, 10000, 50000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("split-%d", i),
			Version: 1,
			Mutations: Mutations{
				SplitEntities: []SplitRequest{{
					SourceID: "node:source",
					Entities: []Entity{
						{ID: "node:left", Kind: "node"},
						{ID: "node:right", Kind: "node"},
					},
				}},
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
