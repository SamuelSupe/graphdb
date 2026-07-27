package query

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/validator"
)

const graphQLSchema = `
scalar JSON
scalar Long
scalar QueryRequest

type Query {
  graph(request: QueryRequest!): GraphQueryResult!
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
	Request       Request
	RootName      string
	rootTypenames []string
	resultFields  []graphQLResultField
	operationName string
}

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
	fields, err := collectGraphQLResultFields(document, root.SelectionSet, variables)
	if err != nil {
		return GraphQLPlan{}, graphQLErrorFrom(err)
	}
	return GraphQLPlan{
		Request:       queryRequest,
		RootName:      responseName(root),
		rootTypenames: rootTypenames,
		resultFields:  fields,
		operationName: operation.Name,
	}, nil
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
