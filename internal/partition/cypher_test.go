package partition

import (
	"context"
	"reflect"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func TestCypherDistributedViewSurvivesConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	left, right := "left", "right"
	for Bucket(left)%2 == Bucket(right)%2 {
		right += "x"
	}
	_, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "cypher-seed", Mutations: graph.Mutations{UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}}, UpsertEntities: []graph.Entity{{ID: left, Kind: "host", Fields: graph.Fields{"name": "old-a"}}, {ID: right, Kind: "host", Fields: graph.Fields{"name": "old-b"}}}, UpsertEdges: []graph.Edge{{Type: "links", From: left, To: right}}}})
	if err != nil {
		t.Fatal(err)
	}
	g, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.ParseCypher(query.CypherRequest{Query: `MATCH (a:host)-[:links]->(b:host) RETURN a.name AS left, b.name AS right`})
	if err != nil {
		t.Fatal(err)
	}
	response, err := ExecutePinnedCypher(ctx, remote, engine.Home, g, plan, func(version int64) error {
		if version != g.Version {
			t.Fatalf("pin version=%d want=%d", version, g.Version)
		}
		_, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "cypher-update", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: left, Kind: "host", Fields: graph.Fields{"name": "new-a"}}, {ID: right, Kind: "host", Fields: graph.Fields{"name": "new-b"}}}}})
		return err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Version != g.Version || !reflect.DeepEqual(response.Rows, [][]any{{"old-a", "old-b"}}) {
		t.Fatalf("mixed view: %#v", response)
	}
	latest, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil {
		t.Fatal(err)
	}
	response, err = ExecutePinnedCypher(ctx, remote, engine.Home, latest, plan, nil, nil)
	if err != nil || !reflect.DeepEqual(response.Rows, [][]any{{"new-a", "new-b"}}) {
		t.Fatalf("latest=%#v err=%v", response, err)
	}
}
