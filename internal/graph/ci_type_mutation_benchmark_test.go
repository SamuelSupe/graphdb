package graph

import (
	"fmt"
	"testing"
)

func BenchmarkAddUnusedCITypesToLargeGraph(b *testing.B) {
	const (
		entityCount = 50000
		typeCount   = 256
	)
	g := New()
	entities := make([]Entity, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		entities = append(entities, Entity{
			ID:     fmt.Sprintf("node:%05d", i),
			Kind:   "node",
			Fields: Fields{"ordinal": i},
		})
	}
	if err := g.ApplyCommit(Commit{
		ID:        "seed",
		Version:   1,
		Mutations: Mutations{UpsertEntities: entities},
	}); err != nil {
		b.Fatal(err)
	}
	if err := g.ensureContentFingerprint(); err != nil {
		b.Fatal(err)
	}
	ciTypes := make([]CIType, 0, typeCount)
	for i := 0; i < typeCount; i++ {
		ciTypes = append(ciTypes, CIType{
			Name: fmt.Sprintf("ontology_type_%03d", i),
			Fields: map[string]FieldSpec{
				"name": {Type: "string"},
			},
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("ontology-%d", i),
			Version: 2,
			Mutations: Mutations{
				UpsertCITypes: ciTypes,
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdateInheritedCITypeOnLargeGraph(b *testing.B) {
	const (
		entityCount = 20000
		fieldCount  = 12
		depth       = 8
	)
	g := New()
	rootFields := make(map[string]FieldSpec, fieldCount)
	entityFields := make(Fields, fieldCount)
	for i := 0; i < fieldCount; i++ {
		name := fmt.Sprintf("field_%02d", i)
		rootFields[name] = FieldSpec{Type: "number"}
		entityFields[name] = i
	}
	ciTypes := []CIType{{Name: "node_0", Fields: rootFields}}
	for i := 1; i < depth; i++ {
		ciTypes = append(ciTypes, CIType{
			Name:    fmt.Sprintf("node_%d", i),
			Extends: []string{fmt.Sprintf("node_%d", i-1)},
		})
	}
	entities := make([]Entity, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		entities = append(entities, Entity{
			ID:     fmt.Sprintf("node:%05d", i),
			Kind:   fmt.Sprintf("node_%d", depth-1),
			Fields: entityFields,
		})
	}
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes:  ciTypes,
			UpsertEntities: entities,
		},
	}); err != nil {
		b.Fatal(err)
	}
	if err := g.ensureContentFingerprint(); err != nil {
		b.Fatal(err)
	}
	updatedRoot := ciTypes[0]
	updatedRoot.DisplayName = "updated"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("schema-%d", i),
			Version: 2,
			Mutations: Mutations{
				UpsertCITypes: []CIType{updatedRoot},
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchInsertInheritedEntities(b *testing.B) {
	const (
		entityCount = 20000
		fieldCount  = 12
		depth       = 8
	)
	g := New()
	rootFields := make(map[string]FieldSpec, fieldCount)
	entityFields := make(Fields, fieldCount)
	for i := 0; i < fieldCount; i++ {
		name := fmt.Sprintf("field_%02d", i)
		rootFields[name] = FieldSpec{Type: "number"}
		entityFields[name] = i
	}
	ciTypes := []CIType{{Name: "node_0", Fields: rootFields}}
	for i := 1; i < depth; i++ {
		ciTypes = append(ciTypes, CIType{
			Name:    fmt.Sprintf("node_%d", i),
			Extends: []string{fmt.Sprintf("node_%d", i-1)},
		})
	}
	if err := g.ApplyCommit(Commit{
		ID:        "schema",
		Version:   1,
		Mutations: Mutations{UpsertCITypes: ciTypes},
	}); err != nil {
		b.Fatal(err)
	}
	if err := g.ensureContentFingerprint(); err != nil {
		b.Fatal(err)
	}
	entities := make([]Entity, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		entities = append(entities, Entity{
			ID:     fmt.Sprintf("node:%05d", i),
			Kind:   fmt.Sprintf("node_%d", depth-1),
			Fields: entityFields,
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:        fmt.Sprintf("entities-%d", i),
			Version:   2,
			Mutations: Mutations{UpsertEntities: entities},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddDiamondOntology(b *testing.B) {
	const depth = 18
	ciTypes := []CIType{{
		Name: "level_0",
		Fields: map[string]FieldSpec{
			"root": {Type: "string"},
		},
	}}
	previous := []string{"level_0"}
	for level := 1; level <= depth; level++ {
		left := fmt.Sprintf("level_%d_left", level)
		right := fmt.Sprintf("level_%d_right", level)
		ciTypes = append(ciTypes,
			CIType{Name: left, Extends: append([]string(nil), previous...)},
			CIType{Name: right, Extends: append([]string(nil), previous...)},
		)
		previous = []string{left, right}
	}
	ciTypes = append(ciTypes, CIType{
		Name:    "leaf",
		Extends: append([]string(nil), previous...),
	})
	g := New()
	if err := g.ensureContentFingerprint(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("ontology-%d", i),
			Version: 1,
			Mutations: Mutations{
				UpsertCITypes: ciTypes,
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
