package query

import (
	"fmt"
	"strings"
	"testing"
)

func TestRound4GraphQLBlockStringEscapedTripleQuoteDoesNotBypassDepthGuard(t *testing.T) {
	deep := strings.Repeat("[", maxFilterExpressionDepth+5) + "1" + strings.Repeat("]", maxFilterExpressionDepth+5)
	doc := fmt.Sprintf(`query { graph(request: { op: """escaped \""" block""", nested: %s }) { version } }`, deep)
	if err := validateGraphQLDocumentShape(doc); err == nil {
		t.Fatal("escaped triple quote inside GraphQL block string bypassed nesting depth guard")
	}
}

func TestRound4GraphQLSelectionLimitEnforcedBeforeValidation(t *testing.T) {
	var b strings.Builder
	b.WriteString(`query { graph(request: { op: "match" }) { `)
	for i := 0; i < maxGraphQLSelections+50; i++ {
		b.WriteString("a: version ")
	}
	b.WriteString("} }")
	_, errs := ParseGraphQL(GraphQLRequest{Query: b.String()})
	if len(errs) == 0 {
		t.Fatal("expected GraphQL query exceeding selection limit to be rejected")
	}
	if !strings.Contains(errs[0].Message, "selections") {
		t.Fatalf("expected selection limit error before validator, got: %v", errs[0].Message)
	}
}
