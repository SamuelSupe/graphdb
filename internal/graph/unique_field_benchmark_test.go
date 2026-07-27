package graph

import (
	"fmt"
	"testing"
)

func BenchmarkBatchInsertUniqueEntities(b *testing.B) {
	const (
		existingCount = 4000
		insertCount   = 1000
	)
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{
			Name: "asset",
			Fields: map[string]FieldSpec{
				"serial": {Type: "string", Unique: true},
			},
		}}},
	}); err != nil {
		b.Fatal(err)
	}
	existing := make([]Entity, 0, existingCount)
	for i := 0; i < existingCount; i++ {
		existing = append(existing, Entity{
			ID:     fmt.Sprintf("asset:existing:%05d", i),
			Kind:   "asset",
			Fields: Fields{"serial": fmt.Sprintf("existing-%05d", i)},
		})
	}
	if err := g.ApplyCommit(Commit{
		ID:        "seed",
		Version:   2,
		Mutations: Mutations{UpsertEntities: existing},
	}); err != nil {
		b.Fatal(err)
	}
	inserts := make([]Entity, 0, insertCount)
	for i := 0; i < insertCount; i++ {
		inserts = append(inserts, Entity{
			ID:     fmt.Sprintf("asset:new:%05d", i),
			Kind:   "asset",
			Fields: Fields{"serial": fmt.Sprintf("new-%05d", i)},
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:        fmt.Sprintf("insert-%d", i),
			Version:   3,
			Mutations: Mutations{UpsertEntities: inserts},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchInsertComplexUniqueEntities(b *testing.B) {
	const (
		existingCount = 4000
		insertCount   = 1000
	)
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{
			Name: "asset",
			Fields: map[string]FieldSpec{
				"identity": {Type: "object", Unique: true},
			},
		}}},
	}); err != nil {
		b.Fatal(err)
	}
	existing := make([]Entity, 0, existingCount)
	for i := 0; i < existingCount; i++ {
		existing = append(existing, Entity{
			ID:   fmt.Sprintf("asset:existing:%05d", i),
			Kind: "asset",
			Fields: Fields{"identity": map[string]any{
				"provider": "existing",
				"id":       fmt.Sprintf("%05d", i),
			}},
		})
	}
	if err := g.ApplyCommit(Commit{
		ID:        "seed",
		Version:   2,
		Mutations: Mutations{UpsertEntities: existing},
	}); err != nil {
		b.Fatal(err)
	}
	inserts := make([]Entity, 0, insertCount)
	for i := 0; i < insertCount; i++ {
		inserts = append(inserts, Entity{
			ID:   fmt.Sprintf("asset:new:%05d", i),
			Kind: "asset",
			Fields: Fields{"identity": map[string]any{
				"provider": "new",
				"id":       fmt.Sprintf("%05d", i),
			}},
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:        fmt.Sprintf("insert-%d", i),
			Version:   3,
			Mutations: Mutations{UpsertEntities: inserts},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
