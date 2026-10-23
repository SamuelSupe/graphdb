package graph

import (
	"fmt"
	"testing"
	"time"
)

func TestColdNeighborOrderDoesNotBlockAnotherNode(t *testing.T) {
	g, err := FromSnapshot(Snapshot{Entities: []Entity{{ID: "a", Kind: "host"}, {ID: "b", Kind: "host"}, {ID: "c", Kind: "host"}}, Edges: []Edge{{ID: "ac", Type: "connects_to", From: "a", To: "c"}, {ID: "bc", Type: "connects_to", From: "b", To: "c"}}})
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		g.sortedNeighborRefs("a", "out", func() error { close(entered); <-release; return nil })
	}()
	<-entered
	other := make(chan struct{})
	go func() { defer close(other); g.sortedNeighborRefs("b", "out", nil) }()
	blocked := false
	select {
	case <-other:
	case <-time.After(200 * time.Millisecond):
		blocked = true
	}
	close(release)
	<-done
	<-other
	if blocked {
		t.Fatal("cold adjacency build held a graph-wide cache lock")
	}
}
func TestNeighborOrderCacheIsBounded(t *testing.T) {
	g := New()
	for i := range 300 {
		g.sortedNeighborRefs(fmt.Sprintf("host:%d", i), "out", nil)
	}
	if len(g.neighborOrder) > 256 {
		t.Fatalf("unbounded adjacency orders: %d", len(g.neighborOrder))
	}
}

func TestScalarCoverageAndKindIDsSurviveStorageCopies(t *testing.T) {
	g, err := FromSnapshot(Snapshot{Version: 1, Entities: []Entity{{ID: "host:a", Kind: "host", Fields: Fields{"value": []any{1}}}, {ID: "host:b", Kind: "host", Fields: Fields{"value": 2}}}})
	if err != nil {
		t.Fatal(err)
	}
	if g.ScalarIndexCoversField("host", "value") {
		t.Fatal("array missing from coverage counts")
	}
	next, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{ID: "change", Version: 2, Mutations: Mutations{DeleteEntities: []string{"host:a"}, UpsertEntities: []Entity{{ID: "db:a", Kind: "database", Fields: Fields{"value": 3}}}}}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if g.ScalarIndexCoversField("host", "value") || len(g.MatchEntityIDs("host")) != 2 {
		t.Fatal("storage copy mutated published graph indexes")
	}
	if !next.ScalarIndexCoversField("host", "value") || len(next.MatchEntityIDs("host")) != 1 || len(next.MatchEntityIDs("database")) != 1 {
		t.Fatal("storage copy indexes stale")
	}
	clone := next.Clone()
	if err = clone.ApplyCommit(Commit{ID: "delete", Version: 3, Mutations: Mutations{DeleteEntities: []string{"host:b"}}}); err != nil {
		t.Fatal(err)
	}
	if len(next.MatchEntityIDs("host")) != 1 || len(clone.MatchEntityIDs("host")) != 0 {
		t.Fatal("public clone shared kind IDs")
	}
}
