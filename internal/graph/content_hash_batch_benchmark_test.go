package graph

import (
	"fmt"
	"testing"
)

func BenchmarkStorageCopyContentHashBatch(b *testing.B) {
	const (
		existing = 20_000
		added    = 2_000
	)
	g := New()
	seed := make([]Entity, existing)
	for i := range seed {
		seed[i] = Entity{
			ID:     fmt.Sprintf("host:%06d", 100_000+i),
			Kind:   "host",
			Fields: Fields{"sequence": i},
		}
	}
	if err := g.ApplyCommit(Commit{
		ID: "seed", Version: 1,
		Mutations: Mutations{UpsertEntities: seed},
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := g.ContentMD5(); err != nil {
		b.Fatal(err)
	}
	inserts := make([]Entity, added)
	for i := range inserts {
		inserts[i] = Entity{
			ID:     fmt.Sprintf("host:%06d", i),
			Kind:   "host",
			Fields: Fields{"sequence": existing + i},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID: fmt.Sprintf("batch-%d", i), Version: 2,
			Mutations: Mutations{UpsertEntities: inserts},
		}, ApplyOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := next.ContentMD5(); err != nil {
			b.Fatal(err)
		}
	}
}
