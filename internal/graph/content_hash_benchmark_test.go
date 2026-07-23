package graph

import (
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkContentMD5(b *testing.B) {
	g := New()
	entities := make([]Entity, 10_000)
	for i := range entities {
		entities[i] = Entity{
			ID:     fmt.Sprintf("host:%05d", i),
			Kind:   "host",
			Fields: Fields{"state": "ready", "region": fmt.Sprintf("r-%02d", i%16)},
		}
	}
	if err := g.ApplyCommit(Commit{ID: "seed", Version: 1, Mutations: Mutations{UpsertEntities: entities}}); err != nil {
		b.Fatal(err)
	}

	b.Run("stream", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := g.ContentMD5(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("snapshot-reference", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(g.logicalSnapshot()); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("storage-copy-and-hash", func(b *testing.B) {
		if _, err := g.ContentMD5(); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			next, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
				ID:      fmt.Sprintf("update-%d", i),
				Version: 2,
				Mutations: Mutations{UpsertEntities: []Entity{{
					ID: "host:00000", Kind: "host",
					Fields: Fields{"state": "updated", "region": "r-00"},
				}}},
			}, ApplyOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := next.ContentMD5(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
