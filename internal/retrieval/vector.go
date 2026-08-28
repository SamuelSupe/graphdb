package retrieval

import (
	"math"
	"sort"
)

type VectorRecord struct {
	ID     string
	Vector []float32
}

type RankedCandidate struct {
	ID    string  `json:"id"`
	Rank  int     `json:"rank"`
	Score float64 `json:"score"`
}

func ExactCosine(query []float32, records []VectorRecord, limit int) ([]RankedCandidate, error) {
	if limit < 0 {
		return nil, invalidf("vector limit must be >= 0")
	}
	if limit == 0 || len(records) == 0 {
		return nil, nil
	}
	queryNorm, err := vectorNorm(query)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(records))
	candidates := make([]RankedCandidate, 0, len(records))
	for _, record := range records {
		if record.ID == "" {
			return nil, invalidf("vector record id is required")
		}
		if _, ok := seen[record.ID]; ok {
			return nil, invalidf("duplicate vector record id %q", record.ID)
		}
		seen[record.ID] = struct{}{}
		if len(record.Vector) != len(query) {
			return nil, invalidf(
				"vector record %q has dimension %d, want %d",
				record.ID,
				len(record.Vector),
				len(query),
			)
		}
		recordNorm, err := vectorNorm(record.Vector)
		if err != nil {
			return nil, invalidf("vector record %q: %v", record.ID, err)
		}
		var dot float64
		for i := range query {
			dot += float64(query[i]) * float64(record.Vector[i])
		}
		candidates = append(candidates, RankedCandidate{
			ID:    record.ID,
			Score: dot / (queryNorm * recordNorm),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].ID < candidates[j].ID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for i := range candidates {
		candidates[i].Rank = i + 1
	}
	return candidates, nil
}

func vectorNorm(vector []float32) (float64, error) {
	if len(vector) == 0 {
		return 0, invalidf("vector must not be empty")
	}
	var squared float64
	for _, value := range vector {
		number := float64(value)
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return 0, invalidf("vector contains a non-finite value")
		}
		squared += number * number
	}
	if squared == 0 {
		return 0, invalidf("vector norm must be greater than zero")
	}
	return math.Sqrt(squared), nil
}
