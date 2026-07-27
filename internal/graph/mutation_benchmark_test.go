package graph

import (
	"fmt"
	"testing"
)

func largeIsolatedMutationGraph(
	tb testing.TB,
	entityCount int,
	edgeCount int,
) *Graph {
	tb.Helper()
	g := New()
	g.RelationTypes["link"] = RelationType{
		Name:           "link",
		AllowCrossKind: true,
		Cardinality:    ManyToMany,
	}
	g.Entities["node:target"] = Entity{ID: "node:target", Kind: "node"}
	g.Entities["node:source"] = Entity{ID: "node:source", Kind: "node"}
	for i := 0; i < entityCount; i++ {
		id := fmt.Sprintf("node:%05d", i)
		g.Entities[id] = Entity{ID: id, Kind: "node"}
	}
	for i := 0; i < edgeCount; i++ {
		from := fmt.Sprintf("node:%05d", i%entityCount)
		to := fmt.Sprintf(
			"node:%05d",
			(i*17+i/entityCount+1)%entityCount,
		)
		edge := Edge{Type: "link", From: from, To: to}
		edge.ID = CanonicalEdgeID(edge)
		g.Edges[edge.ID] = edge
	}
	g.rebuildIndexes()
	if err := g.ensureContentFingerprint(); err != nil {
		tb.Fatal(err)
	}
	return g
}
