package graph

import (
	"fmt"
	"testing"
)

func BenchmarkSortedEntityIDs(b *testing.B) {
	const entityCount = 20000
	entities := make(map[string]Entity, entityCount)
	for i := 0; i < entityCount; i++ {
		id := fmt.Sprintf("entity:%05d", i)
		entities[id] = Entity{ID: id, Kind: "node"}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ids := sortedEntityIDs(entities); len(ids) != entityCount {
			b.Fatalf("sorted ids = %d, want %d", len(ids), entityCount)
		}
	}
}
