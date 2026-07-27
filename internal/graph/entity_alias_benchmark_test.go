package graph

import (
	"fmt"
	"testing"
)

func BenchmarkDeleteEntitiesByMergedAlias(b *testing.B) {
	const (
		entityCount = 20_000
		deleteCount = 128
	)
	g := New()
	g.Version = 1
	aliases := make([]string, 0, deleteCount)
	for index := 0; index < entityCount; index++ {
		alias := fmt.Sprintf("legacy-node-%05d", index)
		entity := Entity{
			ID:         fmt.Sprintf("node:%05d", index),
			Kind:       "node",
			MergedFrom: []string{alias},
		}
		g.Entities[entity.ID] = entity
		if index < deleteCount {
			aliases = append(aliases, alias)
		}
	}
	g.rebuildIndexes()
	if err := g.ensureContentFingerprint(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("delete-aliases-%d", index),
			Version: 2,
			Mutations: Mutations{
				DeleteEntities: aliases,
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
