package graph

import (
	"fmt"
	"testing"
)

func BenchmarkDeleteEdgesBySourceAlias(b *testing.B) {
	const (
		edgeCount   = 20_000
		deleteCount = 128
	)
	g := New()
	g.Version = 1
	g.RelationTypes["link"] = RelationType{
		Name:           "link",
		AllowCrossKind: true,
		Cardinality:    ManyToMany,
	}
	g.Entities["node:sink"] = Entity{
		ID: "node:sink", Kind: "node",
	}
	aliases := make([]string, 0, deleteCount)
	for index := 0; index < edgeCount; index++ {
		entityID := fmt.Sprintf("node:%05d", index)
		alias := fmt.Sprintf("collector-edge-%05d", index)
		g.Entities[entityID] = Entity{ID: entityID, Kind: "node"}
		edge := Edge{
			Type: "link",
			From: entityID,
			To:   "node:sink",
			Sources: []EdgeSource{{
				Source: "collector",
				EdgeID: alias,
			}},
		}
		edge.ID = CanonicalEdgeID(edge)
		g.Edges[edge.ID] = edge
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
				DeleteEdges: aliases,
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
