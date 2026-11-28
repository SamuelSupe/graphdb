package query

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestMaterializedShortestPathMatchesOrderedBFS(t *testing.T) {
	random := rand.New(rand.NewPCG(51, 912))
	for range 12 {
		const count = 14
		entities := make([]graph.Entity, count)
		for i := range entities {
			entities[i] = graph.Entity{ID: fmt.Sprint(i), Kind: "node"}
		}
		var edges []graph.Edge
		for i := range entities {
			for j := range entities {
				if random.IntN(5) == 0 {
					edges = append(edges, graph.Edge{Type: "links", From: entities[i].ID, To: entities[j].ID, Fields: graph.Fields{"open": (i+j)%3 != 0}})
				}
			}
		}
		g := shortestTestGraph(t, entities, edges)
		for _, direction := range []string{"out", "in", "both"} {
			for i := range entities {
				for j := range entities {
					if i == j {
						continue
					}
					request := Request{Direction: direction, Depth: 2 + random.IntN(5)}
					if (i+j)%2 == 0 {
						request.EdgeWhere = []Filter{{Field: "open", Op: "eq", Value: true}}
					}
					haveBudget := &budget{ctx: t.Context(), limit: 1000}
					wantBudget := &budget{ctx: t.Context(), limit: 1000}
					start, _ := g.GetEntity(entities[i].ID)
					target, _ := g.GetEntity(entities[j].ID)
					have, found, err := shortestPath(g, start, target, request, haveBudget)
					want, wantFound, wantErr := shortestPathBFS(g, start, target, request, wantBudget)
					if err != nil || wantErr != nil || found != wantFound || found && !reflect.DeepEqual(have, want) {
						t.Fatalf("%s %s -> %s up to %d hops: %+v (%v, %v); want %+v (%v, %v)",
							direction, entities[i].ID, entities[j].ID, request.Depth, have, found, err, want, wantFound, wantErr)
					}
				}
			}
		}
	}
}

func TestMaterializedShortestPathAvoidsForwardBranches(t *testing.T) {
	entities := []graph.Entity{{ID: "start", Kind: "node"}, {ID: "finish", Kind: "node"}}
	var edges []graph.Edge
	level := []string{"start"}
	for range 4 {
		var next []string
		for _, parent := range level {
			for range 6 {
				child := fmt.Sprint(len(entities))
				entities = append(entities, graph.Entity{ID: child, Kind: "node"})
				edges = append(edges, graph.Edge{Type: "links", From: parent, To: child})
				next = append(next, child)
			}
		}
		level = next
	}
	edges = append(edges, graph.Edge{Type: "links", From: level[len(level)-1], To: "finish"})
	g := shortestTestGraph(t, entities, edges)
	request := Request{Direction: "out", Depth: 5}
	haveBudget, wantBudget := &budget{ctx: t.Context(), limit: 10000}, &budget{ctx: t.Context(), limit: 10000}
	start, _ := g.GetEntity("start")
	target, _ := g.GetEntity("finish")
	have, found, err := shortestPath(g, start, target, request, haveBudget)
	want, wantFound, wantErr := shortestPathBFS(g, start, target, request, wantBudget)
	if !found || !wantFound || err != nil || wantErr != nil || !reflect.DeepEqual(have, want) {
		t.Fatalf("branching graph changed the path: %+v (%v), want %+v (%v)", have, err, want, wantErr)
	}
	t.Logf("scanned edges: forward BFS %d, bidirectional BFS %d", wantBudget.cost, haveBudget.cost)
	if haveBudget.cost*10 >= wantBudget.cost {
		t.Fatal("bidirectional search still inspected most of the branching graph")
	}
}

func shortestTestGraph(t *testing.T, entities []graph.Entity, edges []graph.Edge) *graph.Graph {
	t.Helper()
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID: "links", Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: entities, UpsertEdges: edges,
			UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "node", ToKind: "node", Directed: true}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return g
}
