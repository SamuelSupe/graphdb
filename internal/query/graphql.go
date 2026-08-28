package query

import (
	"bytes"
	"encoding/json"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/validator"
)

const graphQLSchema = `
scalar JSON
scalar Long
scalar QueryRequest

input EvidenceExpansionInput {
  maxDepth: Int
  direction: String
  relationTypes: [String!]
  nodeKinds: [String!]
  maxSeeds: Int
  maxVisited: Int
}

input EvidenceSearchInput {
  query: String!
  kinds: [String!]
  filters: JSON
  vectorTopK: Int
  lexicalTopK: Int
  topK: Int
  minVersion: Long
  explain: Boolean
  expansion: EvidenceExpansionInput
}

type Query {
  graph(request: QueryRequest!): GraphQueryResult!
  evidenceSearch(input: EvidenceSearchInput!): EvidenceSearchResult!
}

type GraphQueryResult {
  version: Long!
  results: JSON!
  nextCursor: String
  stats: JSON!
  aggregates: JSON
  groups: JSON
  plan: JSON
  profile: JSON
}

type EvidenceSearchResult {
  version: Long!
  retrievalRevision: Long!
  embeddingGeneration: String!
  evidence: JSON!
  stats: JSON!
  plan: JSON
}
`

var parsedGraphQLSchema = gqlparser.MustLoadSchema(&ast.Source{
	Name:  "ggraphdb.graphql",
	Input: graphQLSchema,
})

type GraphQLRequest struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName,omitempty"`
	Variables     map[string]any `json:"variables,omitempty"`
}

type GraphQLPlan struct {
	Request         Request
	EvidenceRequest retrieval.SearchRequest
	RootName        string
	rootKind        graphQLRootKind
	rootTypenames   []string
	resultFields    []graphQLResultField
	operationName   string
}

type graphQLRootKind string

const (
	graphQLRootGraph          graphQLRootKind = "graph"
	graphQLRootEvidenceSearch graphQLRootKind = "evidenceSearch"
)

type graphQLResultField struct {
	Name         string
	ResponseName string
}

func ParseGraphQL(request GraphQLRequest) (GraphQLPlan, gqlerror.List) {
	if request.Query == "" {
		return GraphQLPlan{}, graphQLError(nil, "GraphQL query is required")
	}
	document, errs := gqlparser.LoadQuery(parsedGraphQLSchema, request.Query)
	if len(errs) > 0 {
		return GraphQLPlan{}, errs
	}
	operation := document.Operations.ForName(request.OperationName)
	if operation == nil {
		if request.OperationName == "" {
			return GraphQLPlan{}, graphQLError(nil, "operationName is required when a document contains multiple operations")
		}
		return GraphQLPlan{}, graphQLError(nil, "unknown GraphQL operation %q", request.OperationName)
	}
	if operation.Operation != ast.Query {
		return GraphQLPlan{}, graphQLError(operation.Position, "only GraphQL query operations are supported")
	}
	variables, err := validator.VariableValues(parsedGraphQLSchema, operation, request.Variables)
	if err != nil {
		return GraphQLPlan{}, graphQLErrorFrom(err)
	}
	roots, rootTypenames, err := collectGraphQLRoots(document, operation.SelectionSet, variables)
	if err != nil {
		return GraphQLPlan{}, graphQLErrorFrom(err)
	}
	if len(roots) != 1 {
		return GraphQLPlan{}, graphQLError(
			operation.Position,
			"exactly one graph root field is required per request",
		)
	}
	root := roots[0]
	fields, err := collectGraphQLResultFields(document, root.SelectionSet, variables)
	if err != nil {
		return GraphQLPlan{}, graphQLErrorFrom(err)
	}
	plan := GraphQLPlan{
		RootName:      responseName(root),
		rootKind:      graphQLRootKind(root.Name),
		rootTypenames: rootTypenames,
		resultFields:  fields,
		operationName: operation.Name,
	}
	switch root.Name {
	case string(graphQLRootGraph):
		rawRequest, err := root.Arguments.ForName("request").Value.Value(variables)
		if err != nil {
			return GraphQLPlan{}, graphQLErrorFrom(err)
		}
		queryRequest, err := decodeGraphQLQueryRequest(rawRequest)
		if err != nil {
			return GraphQLPlan{}, graphQLError(root.Position, "%v", err)
		}
		if err := validateRequest(queryRequest); err != nil {
			return GraphQLPlan{}, graphQLError(root.Position, "%v", err)
		}
		plan.Request = queryRequest
	case string(graphQLRootEvidenceSearch):
		rawRequest, err := root.Arguments.ForName("input").Value.Value(variables)
		if err != nil {
			return GraphQLPlan{}, graphQLErrorFrom(err)
		}
		evidenceRequest, err := decodeGraphQLEvidenceRequest(rawRequest)
		if err != nil {
			return GraphQLPlan{}, graphQLError(root.Position, "%v", err)
		}
		plan.EvidenceRequest = evidenceRequest
	default:
		return GraphQLPlan{}, graphQLError(root.Position, "unsupported GraphQL root field %q", root.Name)
	}
	return plan, nil
}

func (p GraphQLPlan) Data(response Response) map[string]any {
	result := make(map[string]any, len(p.resultFields))
	for _, field := range p.resultFields {
		result[field.ResponseName] = graphQLResultValue(response, field.Name)
	}
	data := map[string]any{p.RootName: result}
	for _, alias := range p.rootTypenames {
		data[alias] = "Query"
	}
	return data
}

func (p GraphQLPlan) EvidenceData(response retrieval.SearchResponse) map[string]any {
	result := make(map[string]any, len(p.resultFields))
	for _, field := range p.resultFields {
		result[field.ResponseName] = graphQLEvidenceResultValue(response, field.Name)
	}
	data := map[string]any{p.RootName: result}
	for _, alias := range p.rootTypenames {
		data[alias] = "Query"
	}
	return data
}

func (p GraphQLPlan) IsEvidenceSearch() bool {
	return p.rootKind == graphQLRootEvidenceSearch
}

func graphQLResultValue(response Response, field string) any {
	switch field {
	case "__typename":
		return "GraphQueryResult"
	case "version":
		return response.Version
	case "results":
		return response.Results
	case "nextCursor":
		if response.NextCursor == "" {
			return nil
		}
		return response.NextCursor
	case "stats":
		return response.Stats
	case "aggregates":
		return response.Aggregates
	case "groups":
		return response.Groups
	case "plan":
		return response.Plan
	case "profile":
		return response.Profile
	default:
		return nil
	}
}

func graphQLEvidenceResultValue(response retrieval.SearchResponse, field string) any {
	switch field {
	case "__typename":
		return "EvidenceSearchResult"
	case "version":
		return response.Version
	case "retrievalRevision":
		return response.RetrievalRevision
	case "embeddingGeneration":
		return response.EmbeddingGeneration
	case "evidence":
		return response.Evidence
	case "stats":
		return response.Stats
	case "plan":
		return response.Plan
	default:
		return nil
	}
}

func decodeGraphQLQueryRequest(value any) (Request, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return Request{}, fmt.Errorf("graph request must be an object")
	}
	object = cloneGraphQLMap(object)
	if err := normalizeGraphQLQueryRequest(object); err != nil {
		return Request{}, err
	}
	data, err := json.Marshal(object)
	if err != nil {
		return Request{}, fmt.Errorf("encode graph request: %w", err)
	}
	var request Request
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return Request{}, fmt.Errorf("decode graph request: %w", err)
	}
	return request, nil
}

func decodeGraphQLEvidenceRequest(value any) (retrieval.SearchRequest, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return retrieval.SearchRequest{}, fmt.Errorf("evidence search input must be an object")
	}
	object = cloneGraphQLMap(object)
	for camel, snake := range map[string]string{
		"vectorTopK":  "vector_top_k",
		"lexicalTopK": "lexical_top_k",
		"topK":        "top_k",
		"minVersion":  "min_version",
	} {
		if err := renameGraphQLKey(object, camel, snake); err != nil {
			return retrieval.SearchRequest{}, err
		}
	}
	if expansion, ok := object["expansion"].(map[string]any); ok {
		for camel, snake := range map[string]string{
			"maxDepth":      "max_depth",
			"relationTypes": "relation_types",
			"nodeKinds":     "node_kinds",
			"maxSeeds":      "max_seeds",
			"maxVisited":    "max_visited",
		} {
			if err := renameGraphQLKey(expansion, camel, snake); err != nil {
				return retrieval.SearchRequest{}, err
			}
		}
	}
	data, err := json.Marshal(object)
	if err != nil {
		return retrieval.SearchRequest{}, fmt.Errorf("encode evidence search input: %w", err)
	}
	var request retrieval.SearchRequest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return retrieval.SearchRequest{}, fmt.Errorf("decode evidence search input: %w", err)
	}
	request, err = retrieval.NormalizeRequest(request)
	if err != nil {
		return retrieval.SearchRequest{}, err
	}
	return request, nil
}

func normalizeGraphQLQueryRequest(request map[string]any) error {
	for camel, snake := range map[string]string{
		"targetOp":          "target_op",
		"whereExpr":         "where_expr",
		"edgeWhere":         "edge_where",
		"edgeWhereExpr":     "edge_where_expr",
		"targetID":          "target_id",
		"directionStrategy": "direction_strategy",
		"relationType":      "relation_type",
		"relationTypes":     "relation_types",
		"groupBy":           "group_by",
		"havingExpr":        "having_expr",
		"timeoutMs":         "timeout_ms",
		"costLimit":         "cost_limit",
		"minVersion":        "min_version",
		"allowStale":        "allow_stale",
	} {
		if err := renameGraphQLKey(request, camel, snake); err != nil {
			return err
		}
	}
	if pathValue, ok := request["path"].(map[string]any); ok {
		if err := normalizeGraphQLPath(pathValue); err != nil {
			return err
		}
	}
	return nil
}

func normalizeGraphQLPath(path map[string]any) error {
	for camel, snake := range map[string]string{
		"nodeKinds":     "node_kinds",
		"endKind":       "end_kind",
		"endWhere":      "end_where",
		"endWhereExpr":  "end_where_expr",
		"relationTypes": "relation_types",
		"maxPaths":      "max_paths",
	} {
		if err := renameGraphQLKey(path, camel, snake); err != nil {
			return err
		}
	}
	steps, _ := path["steps"].([]any)
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		for camel, snake := range map[string]string{
			"relationTypes": "relation_types",
			"nodeKinds":     "node_kinds",
			"whereExpr":     "where_expr",
			"edgeWhere":     "edge_where",
			"edgeWhereExpr": "edge_where_expr",
		} {
			if err := renameGraphQLKey(step, camel, snake); err != nil {
				return err
			}
		}
	}
	return nil
}

func renameGraphQLKey(object map[string]any, source, target string) error {
	value, exists := object[source]
	if !exists {
		return nil
	}
	if _, duplicate := object[target]; duplicate {
		return fmt.Errorf("graph request contains both %q and %q", source, target)
	}
	delete(object, source)
	object[target] = value
	return nil
}

func cloneGraphQLMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = cloneGraphQLValue(value)
	}
	return output
}

func cloneGraphQLValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneGraphQLMap(typed)
	case []any:
		output := make([]any, len(typed))
		for i := range typed {
			output[i] = cloneGraphQLValue(typed[i])
		}
		return output
	default:
		return value
	}
}
