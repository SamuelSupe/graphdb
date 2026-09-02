package query

import (
	"fmt"
	"strings"
	"testing"
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

func TestParseGraphQLRejectsRemovedEvidenceSearch(t *testing.T) {
	_, errs := ParseGraphQL(GraphQLRequest{
		Query: `{ evidenceSearch(input: {query: "why?"}) { version } }`,
	})
	if len(errs) == 0 {
		t.Fatal("removed evidenceSearch root was accepted")
	}
	if !strings.Contains(fmt.Sprint(errs), "evidenceSearch") {
		t.Fatalf("error = %v, want evidenceSearch field diagnostic", errs)
	}
}

func TestParseGraphQLRejectsPathologicalDocuments(t *testing.T) {
	if _, errs := ParseGraphQL(GraphQLRequest{Query: strings.Repeat("x", maxTextQueryBytes+1)}); len(errs) == 0 {
		t.Fatal("oversized GraphQL document was accepted")
	}
	if _, errs := ParseGraphQL(GraphQLRequest{Query: strings.Repeat("{", maxFilterExpressionDepth+1)}); len(errs) == 0 {
		t.Fatal("deeply nested GraphQL document was accepted")
	}

	var document strings.Builder
	document.WriteString("query { ...F0 }\n")
	for i := 0; i < 9; i++ {
		fmt.Fprintf(&document, "fragment F%d on Query { ...F%d ...F%d }\n", i, i+1, i+1)
	}
	document.WriteString(`fragment F9 on Query { graph(request: {op: "match"}) { version } }`)
	if _, errs := ParseGraphQL(GraphQLRequest{Query: document.String()}); len(errs) == 0 {
		t.Fatal("exponentially expanding GraphQL fragments were accepted")
	}

	value := any("region")
	for range maxFilterExpressionDepth {
		value = map[string]any{"nested": value}
	}
	if _, errs := ParseGraphQL(GraphQLRequest{
		Query: `query Find($request: QueryRequest!) { graph(request: $request) { version } }`,
		Variables: map[string]any{
			"request": map[string]any{"op": "match", "filters": map[string]any{"region": value}},
		},
	}); len(errs) == 0 {
		t.Fatal("deeply nested GraphQL variable was accepted")
	}
}
