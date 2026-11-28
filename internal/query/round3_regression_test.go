package query

import (
	"errors"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestRound3GQLRejectsOver53BitInteger(t *testing.T) {
	if _, err := ParseGQL(`FIND host WHERE id_num = 9007199254740992`); err != nil {
		t.Fatalf("2^53 integer should be accepted: %v", err)
	}
	if _, err := ParseGQL(`FIND host WHERE id_num = 9007199254740993`); !errors.Is(err, ErrInvalid) {
		t.Fatalf("2^53+1 integer should be rejected with ErrInvalid, got: %v", err)
	}
}

func TestRound3PrefixNumericAtOneMillionMatchesIndexedAndScan(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "host:1m", Kind: "host", Fields: map[string]any{"code": float64(1000000)}},
				{ID: "host:2m", Kind: "host", Fields: map[string]any{"code": float64(2000000)}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	req := Request{
		Op:   "match",
		Kind: "host",
		Where: []Filter{
			{Field: "code", Op: "prefix", Value: "1000"},
		},
	}
	// Unindexed / direct evaluation path
	res, err := Execute(g, req)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].Entity.ID != "host:1m" {
		t.Fatalf("PREFIX 1000 on float64(1000000) returned %+v", res.Results)
	}

	// Indexed range scan + IndexScanKeyMatcher path
	matcher := IndexScanKeyMatcher(req.Where)
	if !matcher("n:1e+06") {
		t.Fatalf("IndexScanKeyMatcher did not match n:1e+06 for PREFIX 1000")
	}
	if matcher("n:2e+06") {
		t.Fatalf("IndexScanKeyMatcher unexpectedly matched n:2e+06 for PREFIX 1000")
	}
	bounds := runtimeIndexBounds(req.Where)
	ids, err := g.ScanFieldIndexRangeIDs("host", "code", bounds, func(key string) (bool, error) {
		return matcher(key), nil
	})
	if err != nil {
		t.Fatalf("ScanFieldIndexRangeIDs failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "host:1m" {
		t.Fatalf("indexed PREFIX 1000 returned %v", ids)
	}
}
