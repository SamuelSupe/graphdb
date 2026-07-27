package graph

import "testing"

func TestEdgeAliasIndexStorageCopyIsolation(t *testing.T) {
	source := graphWithEdgeEndpoints(t)
	if err := source.ApplyCommit(Commit{
		ID:      "seed-edge",
		Version: 1,
		Mutations: Mutations{UpsertEdges: []Edge{{
			ID:   "collector-edge",
			Type: "runs_on",
			From: "service:api",
			To:   "host:app-01",
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	edgeID, err := source.resolveEdgeReference("collector-edge")
	if err != nil || edgeID == "" {
		t.Fatalf("source alias = %q, err=%v", edgeID, err)
	}

	next, _, err := source.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "delete-edge",
		Version: 2,
		Mutations: Mutations{
			DeleteEdges: []string{"collector-edge"},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved, err := next.resolveEdgeReference("collector-edge"); err != nil ||
		resolved != "" {
		t.Fatalf("next alias = %q, err=%v", resolved, err)
	}
	if resolved, err := source.resolveEdgeReference("collector-edge"); err != nil ||
		resolved != edgeID {
		t.Fatalf("source alias after copy = %q, err=%v", resolved, err)
	}
}
