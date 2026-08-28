package retrieval

import (
	"math"
	"sort"
	"strings"
	"unicode"
)

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

type TextRecord struct {
	ID   string
	Text string
}

func ExactBM25(query string, records []TextRecord, limit int) ([]RankedCandidate, error) {
	if limit < 0 {
		return nil, invalidf("lexical limit must be >= 0")
	}
	if limit == 0 || len(records) == 0 {
		return nil, nil
	}
	queryTerms := uniqueTokens(tokenize(query))
	if len(queryTerms) == 0 {
		return nil, invalidf("lexical query has no searchable terms")
	}
	type document struct {
		id     string
		length int
		terms  map[string]int
	}
	documents := make([]document, 0, len(records))
	documentFrequency := make(map[string]int)
	seen := make(map[string]struct{}, len(records))
	var totalLength int
	for _, record := range records {
		if record.ID == "" {
			return nil, invalidf("text record id is required")
		}
		if _, ok := seen[record.ID]; ok {
			return nil, invalidf("duplicate text record id %q", record.ID)
		}
		seen[record.ID] = struct{}{}
		tokens := tokenize(record.Text)
		frequencies := make(map[string]int)
		for _, token := range tokens {
			frequencies[token]++
		}
		for term := range frequencies {
			documentFrequency[term]++
		}
		documents = append(documents, document{
			id:     record.ID,
			length: len(tokens),
			terms:  frequencies,
		})
		totalLength += len(tokens)
	}
	averageLength := float64(totalLength) / float64(len(documents))
	if averageLength == 0 {
		return nil, nil
	}
	candidates := make([]RankedCandidate, 0, len(documents))
	totalDocuments := float64(len(documents))
	for _, document := range documents {
		var score float64
		for _, term := range queryTerms {
			frequency := float64(document.terms[term])
			if frequency == 0 {
				continue
			}
			df := float64(documentFrequency[term])
			idf := math.Log(1 + (totalDocuments-df+0.5)/(df+0.5))
			denominator := frequency + bm25K1*(1-bm25B+bm25B*float64(document.length)/averageLength)
			score += idf * (frequency * (bm25K1 + 1) / denominator)
		}
		if score > 0 {
			candidates = append(candidates, RankedCandidate{ID: document.id, Score: score})
		}
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

func tokenize(input string) []string {
	var tokens []string
	var word strings.Builder
	flush := func() {
		if word.Len() == 0 {
			return
		}
		tokens = append(tokens, word.String())
		word.Reset()
	}
	for _, r := range strings.ToLower(input) {
		switch {
		case unicode.In(r, unicode.Han):
			flush()
			tokens = append(tokens, string(r))
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			word.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return tokens
}

func uniqueTokens(tokens []string) []string {
	unique := make([]string, 0, len(tokens))
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		unique = append(unique, token)
	}
	return unique
}
