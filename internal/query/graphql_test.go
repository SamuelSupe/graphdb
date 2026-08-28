package query

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"
)

func TestParseGraphQLWithVariablesAndAliases(t *testing.T) {
	plan, errs := ParseGraphQL(GraphQLRequest{
		Query: `
			query FindHosts($request: QueryRequest!, $withStats: Boolean!) {
				hosts: graph(request: $request) {
					version
					items: results
					stats @include(if: $withStats)
				}
			}`,
		OperationName: "FindHosts",
		Variables: map[string]any{
			"request": map[string]any{
				"op":         "match",
				"kind":       "host",
				"minVersion": 4,
				"limit":      10,
			},
			"withStats": true,
		},
	})
	if len(errs) > 0 {
		t.Fatalf("ParseGraphQL: %v", errs)
	}
	if plan.RootName != "hosts" || plan.Request.Op != "match" ||
		plan.Request.Kind != "host" || plan.Request.MinVersion != 4 ||
		plan.Request.Limit != 10 {
		t.Fatalf("plan = %#v", plan)
	}
	data := plan.Data(Response{
		Version: 4,
		Stats:   Stats{Returned: 1},
	})
	root, ok := data["hosts"].(map[string]any)
	if !ok || root["version"] != int64(4) || root["items"] == nil || root["stats"] == nil {
		t.Fatalf("data = %#v", data)
	}
}

func TestParseGraphQLInlineRequestAndFragment(t *testing.T) {
	plan, errs := ParseGraphQL(GraphQLRequest{Query: `
		{
			graph(request: {op: "neighbors", id: "host:1", relationTypes: ["depends_on"]}) {
				...ResultFields
			}
		}
		fragment ResultFields on GraphQueryResult {
			version
			nextCursor
		}
	`})
	if len(errs) > 0 {
		t.Fatalf("ParseGraphQL: %v", errs)
	}
	if plan.Request.Op != "neighbors" || plan.Request.ID != "host:1" ||
		len(plan.Request.RelationTypes) != 1 {
		t.Fatalf("request = %#v", plan.Request)
	}
}

func TestParseGraphQLRejectsLegacyTextAndMultipleRoots(t *testing.T) {
	if _, errs := ParseGraphQL(GraphQLRequest{Query: `FIND host LIMIT 10`}); len(errs) == 0 {
		t.Fatal("legacy text query was accepted as GraphQL")
	}
	if _, errs := ParseGraphQL(GraphQLRequest{
		Query: `{ a: graph(request: {op: "match"}) { version } b: graph(request: {op: "match"}) { version } }`,
	}); len(errs) == 0 {
		t.Fatal("multiple graph roots were accepted")
	}
}

func TestParseGraphQLRejectsUnknownRequestField(t *testing.T) {
	_, errs := ParseGraphQL(GraphQLRequest{
		Query: `{ graph(request: {op: "match", typo: true}) { version } }`,
	})
	if len(errs) == 0 {
		t.Fatal("unknown graph request field was accepted")
	}
}

func TestParseGraphQLEvidenceSearch(t *testing.T) {
	plan, errs := ParseGraphQL(GraphQLRequest{
		Query: `
			query Evidence($input: EvidenceSearchInput!) {
				answer: evidenceSearch(input: $input) {
					version
					retrievalRevision
					embeddingGeneration
					evidence
					stats
					plan
				}
			}`,
		OperationName: "Evidence",
		Variables: map[string]any{
			"input": map[string]any{
				"query":       "why is checkout failing?",
				"kinds":       []any{"TextChunk"},
				"vectorTopK":  120,
				"lexicalTopK": 80,
				"topK":        10,
				"minVersion":  7,
				"explain":     true,
				"expansion": map[string]any{
					"maxDepth":      1,
					"direction":     "out",
					"relationTypes": []any{"MENTIONS"},
				},
			},
		},
	})
	if len(errs) > 0 {
		t.Fatalf("ParseGraphQL: %v", errs)
	}
	request := plan.EvidenceRequest
	if !plan.IsEvidenceSearch() ||
		plan.RootName != "answer" ||
		request.Query != "why is checkout failing?" ||
		request.VectorTopK != 120 ||
		request.LexicalTopK != 80 ||
		request.TopK != 10 ||
		request.MinVersion != 7 ||
		!request.Explain ||
		request.Expansion == nil ||
		request.Expansion.Depth() != 1 ||
		request.Expansion.Direction != "out" {
		t.Fatalf("plan = %#v", plan)
	}
	data := plan.EvidenceData(retrieval.SearchResponse{
		Version:             7,
		RetrievalRevision:   9,
		EmbeddingGeneration: "generation-2",
		Evidence: []retrieval.Evidence{{
			Rank:  1,
			ID:    "chunk:1",
			Score: 0.9,
		}},
		Stats: retrieval.SearchStats{Returned: 1},
	})
	root, ok := data["answer"].(map[string]any)
	if !ok ||
		root["version"] != int64(7) ||
		root["retrievalRevision"] != int64(9) ||
		root["embeddingGeneration"] != "generation-2" ||
		root["evidence"] == nil ||
		root["stats"] == nil {
		t.Fatalf("data = %#v", data)
	}
}

func TestParseGraphQLEvidenceSearchRejectsInvalidBounds(t *testing.T) {
	_, errs := ParseGraphQL(GraphQLRequest{
		Query: `{
			evidenceSearch(input: {
				query: "deep"
				expansion: {maxDepth: 3}
			}) {
				version
			}
		}`,
	})
	if len(errs) == 0 {
		t.Fatal("unbounded evidence expansion was accepted")
	}
}
