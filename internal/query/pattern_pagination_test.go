package query

import (
	"fmt"
	"sort"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPatternPaginationContinuesPastInternalLookahead(t *testing.T) {
	g := largeTraversalPagingGraph(t, 1003)
	request := Request{
		Op:    "pattern",
		Kind:  "service",
		Where: []Filter{{Field: "id", Op: "eq", Value: "service:start"}},
		Path: PathFilter{Steps: []PathStep{{
			Direction:     "out",
			RelationTypes: []string{"runs_on"},
			NodeKinds:     []string{"host"},
		}}},
		Limit: 1000,
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first.Results) != 1000 || first.NextCursor == "" {
		t.Fatalf(
			"first page results=%d cursor=%q, want 1000 and a cursor",
			len(first.Results), first.NextCursor,
		)
	}
	request.Cursor = first.NextCursor
	second, err := Execute(g, request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second.Results) != 3 || second.NextCursor != "" {
		t.Fatalf(
			"second page results=%d cursor=%q, want final 3 results",
			len(second.Results), second.NextCursor,
		)
	}
}

func TestPatternDefaultOrderDoesNotLoseLaterStartPaths(t *testing.T) {
	targets := patternTargetsWithLateSmallestEdge(t)
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "pattern-start-order",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{
				Name: "runs_on", AllowCrossKind: true, Directed: true,
			}},
			UpsertEntities: []graph.Entity{
				{ID: "service:a", Kind: "service"},
				{ID: "service:b", Kind: "service"},
				{ID: "service:c", Kind: "service"},
				{ID: targets[0], Kind: "host"},
				{ID: targets[1], Kind: "host"},
				{ID: targets[2], Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{Type: "runs_on", From: "service:a", To: targets[0]},
				{Type: "runs_on", From: "service:b", To: targets[1]},
				{Type: "runs_on", From: "service:c", To: targets[2]},
			},
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	request := Request{
		Op:   "pattern",
		Kind: "service",
		Path: PathFilter{Steps: []PathStep{{
			Direction:     "out",
			RelationTypes: []string{"runs_on"},
			NodeKinds:     []string{"host"},
		}}},
		Limit: 1,
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	ordered := []string{
		"path:" + graph.CanonicalEdgeIDParts("runs_on", "service:a", targets[0]),
		"path:" + graph.CanonicalEdgeIDParts("runs_on", "service:b", targets[1]),
		"path:" + graph.CanonicalEdgeIDParts("runs_on", "service:c", targets[2]),
	}
	sort.Strings(ordered)
	if got := pathResultIDs(first.Results); fmt.Sprint(got) !=
		fmt.Sprint(ordered[:1]) {
		t.Fatalf("first page = %v, want globally first path", got)
	}
	request.Cursor = first.NextCursor
	second, err := Execute(g, request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if got := pathResultIDs(second.Results); fmt.Sprint(got) !=
		fmt.Sprint(ordered[1:2]) {
		t.Fatalf("second page = %v, want next path", got)
	}
}

func TestPatternCursorDistinguishesOppositePathsOnSameEdge(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "pattern-opposite-paths",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{
				Name: "links", AllowCrossKind: true, Directed: true,
			}},
			UpsertEntities: []graph.Entity{
				{ID: "node:a", Kind: "node"},
				{ID: "node:b", Kind: "node"},
				{ID: "node:c", Kind: "node"},
				{ID: "node:d", Kind: "node"},
			},
			UpsertEdges: []graph.Edge{
				{Type: "links", From: "node:a", To: "node:b"},
				{Type: "links", From: "node:c", To: "node:d"},
			},
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	request := Request{
		Op: "pattern", Kind: "node",
		Path: PathFilter{Steps: []PathStep{{
			Direction:     "both",
			RelationTypes: []string{"links"},
			NodeKinds:     []string{"node"},
		}}},
		Limit: 1,
	}
	seen := map[string]struct{}{}
	for page := 0; page < 5; page++ {
		response, err := Execute(g, request)
		if err != nil {
			t.Fatalf("page %d: %v", page+1, err)
		}
		if len(response.Results) != 1 {
			t.Fatalf("page %d results = %#v", page+1, response.Results)
		}
		entities := fmt.Sprint(pathEntityIDs(*response.Results[0].Path))
		if _, duplicate := seen[entities]; duplicate {
			t.Fatalf("page %d repeated path %s", page+1, entities)
		}
		seen[entities] = struct{}{}
		request.Cursor = response.NextCursor
		if request.Cursor == "" {
			if len(seen) != 4 {
				t.Fatalf("pagination ended after %d of 4 paths", len(seen))
			}
			return
		}
	}
	t.Fatalf("pagination did not finish after 4 paths")
}

func pathEntityIDs(path graph.Path) []string {
	ids := make([]string, 0, len(path.Entities))
	for _, entity := range path.Entities {
		ids = append(ids, entity.ID)
	}
	return ids
}

func patternTargetsWithLateSmallestEdge(t *testing.T) [3]string {
	t.Helper()
	targets := [3]string{}
	edgeIDs := [3]string{}
	for startIndex, start := range []string{
		"service:a", "service:b", "service:c",
	} {
		for candidate := 0; candidate < 10_000; candidate++ {
			target := fmt.Sprintf("host:%d:%04d", startIndex, candidate)
			edgeID := graph.CanonicalEdgeIDParts("runs_on", start, target)
			if targets[startIndex] == "" ||
				(startIndex < 2 && edgeID > edgeIDs[startIndex]) ||
				(startIndex == 2 && edgeID < edgeIDs[startIndex]) {
				targets[startIndex] = target
				edgeIDs[startIndex] = edgeID
			}
		}
	}
	if edgeIDs[2] >= edgeIDs[0] || edgeIDs[2] >= edgeIDs[1] {
		t.Fatalf("could not build late-start ordering fixture: %v", edgeIDs)
	}
	return targets
}
