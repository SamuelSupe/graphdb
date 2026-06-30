package query

import "testing"

func TestMaxPathResultsCapsExplicitMaxPaths(t *testing.T) {
	got := maxPathResults(Request{Path: PathFilter{MaxPaths: 1_000_000}})
	if got != maxQueryLimit+1 {
		t.Fatalf("maxPathResults = %d, want %d", got, maxQueryLimit+1)
	}
}

func TestMaxPathResultsKeepsSmallExplicitMaxPaths(t *testing.T) {
	got := maxPathResults(Request{Limit: 100, Path: PathFilter{MaxPaths: 7}})
	if got != 7 {
		t.Fatalf("maxPathResults = %d, want explicit small cap", got)
	}
}
