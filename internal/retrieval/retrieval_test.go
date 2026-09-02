package retrieval

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestNormalizeRequestAppliesBoundedDefaults(t *testing.T) {
	request, err := NormalizeRequest(SearchRequest{
		Query: "  why is checkout failing?  ",
		Kinds: []string{"TextChunk", "TextChunk"},
	})
	if err != nil {
		t.Fatalf("NormalizeRequest: %v", err)
	}
	if request.Query != "why is checkout failing?" ||
		request.TopK != DefaultTopK ||
		request.VectorTopK != DefaultCandidateK ||
		request.LexicalTopK != DefaultCandidateK ||
		request.Expansion == nil ||
		request.Expansion.Depth() != DefaultMaxDepth ||
		request.Expansion.Direction != "both" ||
		request.Expansion.MaxSeeds != DefaultMaxSeeds ||
		request.Expansion.MaxVisited != DefaultMaxVisited {
		t.Fatalf("normalized request = %#v", request)
	}
	if !reflect.DeepEqual(request.Kinds, []string{"TextChunk"}) {
		t.Fatalf("kinds = %#v", request.Kinds)
	}

	withExpansion, err := NormalizeRequest(SearchRequest{
		Query:     "follow mentions",
		Expansion: &ExpansionRequest{Direction: "out"},
	})
	if err != nil {
		t.Fatalf("NormalizeRequest partial expansion: %v", err)
	}
	if withExpansion.Expansion.Depth() != DefaultMaxDepth {
		t.Fatalf("partial expansion depth = %d", withExpansion.Expansion.Depth())
	}

	zero := 0
	disabled, err := NormalizeRequest(SearchRequest{
		Query:     "exact lookup",
		Expansion: &ExpansionRequest{MaxDepth: &zero},
	})
	if err != nil {
		t.Fatalf("NormalizeRequest disabled expansion: %v", err)
	}
	if disabled.Expansion.Depth() != 0 {
		t.Fatalf("max depth = %d, want 0", disabled.Expansion.Depth())
	}
}

func TestNormalizeRequestRejectsUnboundedExpansion(t *testing.T) {
	depth := MaxDepth + 1
	_, err := NormalizeRequest(SearchRequest{
		Query:     "deep search",
		Expansion: &ExpansionRequest{MaxDepth: &depth},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestExactCosineUsesStableIDTieBreak(t *testing.T) {
	candidates, err := ExactCosine(
		[]float32{1, 0},
		[]VectorRecord{
			{ID: "chunk:b", Vector: []float32{1, 0}},
			{ID: "chunk:c", Vector: []float32{0, 1}},
			{ID: "chunk:a", Vector: []float32{1, 0}},
		},
		3,
	)
	if err != nil {
		t.Fatalf("ExactCosine: %v", err)
	}
	got := []string{candidates[0].ID, candidates[1].ID, candidates[2].ID}
	if !reflect.DeepEqual(got, []string{"chunk:a", "chunk:b", "chunk:c"}) {
		t.Fatalf("order = %#v", got)
	}
	for i := range candidates {
		if candidates[i].Rank != i+1 {
			t.Fatalf("candidate %d rank = %d", i, candidates[i].Rank)
		}
	}
}

func TestExactCosineRejectsDimensionMismatch(t *testing.T) {
	_, err := ExactCosine(
		[]float32{1, 0},
		[]VectorRecord{{ID: "chunk:a", Vector: []float32{1}}},
		1,
	)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v, want ErrInvalid", err)
	}
}

func TestExactBM25PreservesExactIdentifierSignal(t *testing.T) {
	candidates, err := ExactBM25(
		"ERR-42 checkout",
		[]TextRecord{
			{ID: "chunk:generic", Text: "checkout failed after a network timeout"},
			{ID: "chunk:exact", Text: "ERR-42 checkout validation failure"},
		},
		2,
	)
	if err != nil {
		t.Fatalf("ExactBM25: %v", err)
	}
	if len(candidates) != 2 || candidates[0].ID != "chunk:exact" {
		t.Fatalf("candidates = %#v", candidates)
	}
}

func TestReciprocalRankFusionKeepsChannelBreakdown(t *testing.T) {
	candidates, err := ReciprocalRankFusion([]RankedList{
		{
			Name:   "vector",
			Weight: 1,
			Candidates: []RankedCandidate{
				{ID: "chunk:a", Rank: 1, Score: 0.9},
				{ID: "chunk:b", Rank: 2, Score: 0.8},
			},
		},
		{
			Name:   "lexical",
			Weight: 2,
			Candidates: []RankedCandidate{
				{ID: "chunk:b", Rank: 1, Score: 4.2},
				{ID: "chunk:a", Rank: 2, Score: 3.7},
			},
		},
	}, DefaultRRFK, 2)
	if err != nil {
		t.Fatalf("ReciprocalRankFusion: %v", err)
	}
	if len(candidates) != 2 || candidates[0].ID != "chunk:b" {
		t.Fatalf("candidates = %#v", candidates)
	}
	if len(candidates[0].Channels) != 2 ||
		candidates[0].Channels["vector"].RawScore != 0.8 ||
		candidates[0].Channels["lexical"].RawScore != 4.2 {
		t.Fatalf("channel breakdown = %#v", candidates[0].Channels)
	}
}

type retrievalTestResolver struct {
	snapshot Snapshot
	err      error
	calls    int
	min      int64
}

func (r *retrievalTestResolver) ResolveRetrievalSnapshot(
	_ context.Context,
	_ string,
	minVersion int64,
) (Snapshot, error) {
	r.calls++
	r.min = minVersion
	return r.snapshot, r.err
}

type retrievalTestEngine struct {
	calls    int
	snapshot Snapshot
	request  SearchRequest
	response SearchResponse
}

func (e *retrievalTestEngine) SearchSnapshot(
	_ context.Context,
	_ string,
	snapshot Snapshot,
	request SearchRequest,
) (SearchResponse, error) {
	e.calls++
	e.snapshot = snapshot
	e.request = request
	return e.response, nil
}

func TestServiceUsesPublishedSnapshotAsResponseFence(t *testing.T) {
	resolver := &retrievalTestResolver{snapshot: Snapshot{
		TenantGeneration:    3,
		Revision:            8,
		GraphVersion:        21,
		DefinitionRevision:  2,
		CatalogKey:          "catalog.parquet",
		CatalogHash:         "catalog-hash",
		EmbeddingGeneration: "embedding-v4",
	}}
	engine := &retrievalTestEngine{response: SearchResponse{
		Version:             999,
		RetrievalRevision:   999,
		EmbeddingGeneration: "untrusted",
		Evidence:            []Evidence{{ID: "chunk:1"}},
	}}
	service := NewService(resolver, engine)

	response, err := service.SearchEvidence(
		context.Background(),
		"tenant-a",
		SearchRequest{Query: "why", MinVersion: 20},
	)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if resolver.min != 20 || engine.calls != 1 {
		t.Fatalf("resolver min=%d engine calls=%d", resolver.min, engine.calls)
	}
	if engine.snapshot != resolver.snapshot {
		t.Fatalf("engine snapshot = %#v, want %#v", engine.snapshot, resolver.snapshot)
	}
	if response.Version != 21 ||
		response.RetrievalRevision != 8 ||
		response.EmbeddingGeneration != "embedding-v4" ||
		response.Stats.Returned != 1 {
		t.Fatalf("response fence = %#v", response)
	}
}

func TestServiceDoesNotExecuteSnapshotBelowMinVersion(t *testing.T) {
	resolver := &retrievalTestResolver{snapshot: Snapshot{
		Revision:            4,
		GraphVersion:        10,
		CatalogKey:          "catalog.parquet",
		CatalogHash:         "catalog-hash",
		EmbeddingGeneration: "embedding-v1",
	}}
	engine := &retrievalTestEngine{}
	service := NewService(resolver, engine)

	_, err := service.SearchEvidence(
		context.Background(),
		"tenant-a",
		SearchRequest{Query: "why", MinVersion: 11},
	)
	var freshness *NotFreshError
	if !errors.As(err, &freshness) ||
		freshness.VisibleVersion != 10 ||
		freshness.RequiredVersion != 11 {
		t.Fatalf("freshness err = %v", err)
	}
	if engine.calls != 0 {
		t.Fatalf("engine calls = %d, want 0", engine.calls)
	}
}
