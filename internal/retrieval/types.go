package retrieval

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	DefaultTopK         = 20
	DefaultCandidateK   = 200
	DefaultMaxDepth     = 2
	DefaultMaxSeeds     = 50
	DefaultMaxVisited   = 10_000
	MaxTopK             = 100
	MaxCandidateK       = 1_000
	MaxDepth            = 2
	MaxSeeds            = 200
	MaxVisited          = 100_000
	MaxQueryBytes       = 16 * 1024
	MaxConstraintValues = 64
)

var (
	ErrInvalid              = errors.New("invalid evidence search")
	ErrNotReady             = errors.New("retrieval index not ready")
	ErrNotFresh             = errors.New("retrieval index not fresh")
	ErrEmbeddingUnavailable = errors.New("embedding unavailable")
	ErrBudgetExceeded       = errors.New("retrieval budget exceeded")
)

type SearchRequest struct {
	Query       string            `json:"query"`
	Kinds       []string          `json:"kinds,omitempty"`
	Filters     map[string]any    `json:"filters,omitempty"`
	VectorTopK  int               `json:"vector_top_k,omitempty"`
	LexicalTopK int               `json:"lexical_top_k,omitempty"`
	TopK        int               `json:"top_k,omitempty"`
	MinVersion  int64             `json:"min_version,omitempty"`
	Explain     bool              `json:"explain,omitempty"`
	Expansion   *ExpansionRequest `json:"expansion,omitempty"`
}

type ExpansionRequest struct {
	MaxDepth      *int     `json:"max_depth,omitempty"`
	Direction     string   `json:"direction,omitempty"`
	RelationTypes []string `json:"relation_types,omitempty"`
	NodeKinds     []string `json:"node_kinds,omitempty"`
	MaxSeeds      int      `json:"max_seeds,omitempty"`
	MaxVisited    int      `json:"max_visited,omitempty"`
}

func (r *ExpansionRequest) Depth() int {
	if r == nil || r.MaxDepth == nil {
		return DefaultMaxDepth
	}
	return *r.MaxDepth
}

type SearchResponse struct {
	Version             int64       `json:"version"`
	RetrievalRevision   int64       `json:"retrieval_revision"`
	EmbeddingGeneration string      `json:"embedding_generation"`
	Evidence            []Evidence  `json:"evidence"`
	Stats               SearchStats `json:"stats"`
	Plan                *SearchPlan `json:"plan,omitempty"`
}

type Evidence struct {
	Rank   int            `json:"rank"`
	ID     string         `json:"id"`
	Score  float64        `json:"score"`
	Chunk  map[string]any `json:"chunk,omitempty"`
	Source map[string]any `json:"source,omitempty"`
	Paths  []EvidencePath `json:"paths,omitempty"`
	Scores ScoreBreakdown `json:"scores"`
}

type EvidencePath struct {
	NodeIDs []string `json:"node_ids"`
	EdgeIDs []string `json:"edge_ids"`
}

type ScoreBreakdown struct {
	Vector  float64 `json:"vector,omitempty"`
	Lexical float64 `json:"lexical,omitempty"`
	Graph   float64 `json:"graph,omitempty"`
	Fusion  float64 `json:"fusion"`
}

type SearchStats struct {
	VectorCandidates   int                `json:"vector_candidates"`
	LexicalCandidates  int                `json:"lexical_candidates"`
	ExpandedCandidates int                `json:"expanded_candidates"`
	Visited            int                `json:"visited"`
	Returned           int                `json:"returned"`
	DurationMS         map[string]float64 `json:"duration_ms,omitempty"`
}

type SearchPlan struct {
	Stages   []PlanStage `json:"stages"`
	Warnings []string    `json:"warnings,omitempty"`
}

type PlanStage struct {
	Name   string         `json:"name"`
	Detail map[string]any `json:"detail,omitempty"`
}

type Searcher interface {
	SearchEvidence(context.Context, string, SearchRequest) (SearchResponse, error)
}

type NotFreshError struct {
	VisibleVersion  int64
	RequiredVersion int64
}

func (e *NotFreshError) Error() string {
	return fmt.Sprintf(
		"%s: visible version %d is below required version %d",
		ErrNotFresh,
		e.VisibleVersion,
		e.RequiredVersion,
	)
}

func (e *NotFreshError) Unwrap() error {
	return ErrNotFresh
}

func NormalizeRequest(request SearchRequest) (SearchRequest, error) {
	request.Query = strings.TrimSpace(request.Query)
	if request.Query == "" {
		return SearchRequest{}, invalidf("query is required")
	}
	if len(request.Query) > MaxQueryBytes {
		return SearchRequest{}, invalidf("query exceeds %d bytes", MaxQueryBytes)
	}
	if request.TopK < 0 || request.TopK > MaxTopK {
		return SearchRequest{}, invalidf("top_k must be between 1 and %d", MaxTopK)
	}
	if request.TopK == 0 {
		request.TopK = DefaultTopK
	}
	if request.VectorTopK < 0 || request.VectorTopK > MaxCandidateK {
		return SearchRequest{}, invalidf("vector_top_k must be between 1 and %d", MaxCandidateK)
	}
	if request.VectorTopK == 0 {
		request.VectorTopK = DefaultCandidateK
	}
	if request.LexicalTopK < 0 || request.LexicalTopK > MaxCandidateK {
		return SearchRequest{}, invalidf("lexical_top_k must be between 1 and %d", MaxCandidateK)
	}
	if request.LexicalTopK == 0 {
		request.LexicalTopK = DefaultCandidateK
	}
	if request.MinVersion < 0 {
		return SearchRequest{}, invalidf("min_version must be >= 0")
	}
	var err error
	request.Kinds, err = normalizeNames("kinds", request.Kinds)
	if err != nil {
		return SearchRequest{}, err
	}
	if request.Filters != nil {
		request.Filters = cloneMap(request.Filters)
	}
	expansion := ExpansionRequest{
		MaxDepth:   intPointer(DefaultMaxDepth),
		Direction:  "both",
		MaxSeeds:   DefaultMaxSeeds,
		MaxVisited: DefaultMaxVisited,
	}
	if request.Expansion != nil {
		expansion = *request.Expansion
		if expansion.Direction == "" {
			expansion.Direction = "both"
		}
		if expansion.MaxSeeds == 0 {
			expansion.MaxSeeds = DefaultMaxSeeds
		}
		if expansion.MaxVisited == 0 {
			expansion.MaxVisited = DefaultMaxVisited
		}
	}
	depth := expansion.Depth()
	if depth < 0 || depth > MaxDepth {
		return SearchRequest{}, invalidf("expansion.max_depth must be between 0 and %d", MaxDepth)
	}
	expansion.MaxDepth = intPointer(depth)
	switch expansion.Direction {
	case "in", "out", "both":
	default:
		return SearchRequest{}, invalidf("unsupported expansion.direction %q", expansion.Direction)
	}
	if expansion.MaxSeeds < 1 || expansion.MaxSeeds > MaxSeeds {
		return SearchRequest{}, invalidf("expansion.max_seeds must be between 1 and %d", MaxSeeds)
	}
	if expansion.MaxVisited < 1 || expansion.MaxVisited > MaxVisited {
		return SearchRequest{}, invalidf("expansion.max_visited must be between 1 and %d", MaxVisited)
	}
	expansion.RelationTypes, err = normalizeNames("expansion.relation_types", expansion.RelationTypes)
	if err != nil {
		return SearchRequest{}, err
	}
	expansion.NodeKinds, err = normalizeNames("expansion.node_kinds", expansion.NodeKinds)
	if err != nil {
		return SearchRequest{}, err
	}
	request.Expansion = &expansion
	return request, nil
}

func intPointer(value int) *int {
	return &value
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func normalizeNames(field string, values []string) ([]string, error) {
	if len(values) > MaxConstraintValues {
		return nil, invalidf("%s supports at most %d values", field, MaxConstraintValues)
	}
	if len(values) == 0 {
		return nil, nil
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, invalidf("%s cannot contain an empty value", field)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	return normalized, nil
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		switch typed := value.(type) {
		case map[string]any:
			output[key] = cloneMap(typed)
		case []any:
			copied := make([]any, len(typed))
			copy(copied, typed)
			output[key] = copied
		default:
			output[key] = value
		}
	}
	return output
}
