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
	if err := validateGraphQLDocumentShape(request.Query); err != nil {
		return GraphQLPlan{}, graphQLError(nil, "%v", err)
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
		rootTypenames: rootTypenames,
		resultFields:  fields,
		operationName: operation.Name,
	}
	switch root.Name {
	case "graph":
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
	default:
		return GraphQLPlan{}, graphQLError(root.Position, "unsupported GraphQL root field %q", root.Name)
	}
	return plan, nil
}

func validateGraphQLDocumentShape(document string) error {
	if len(document) > maxTextQueryBytes {
		return fmt.Errorf("GraphQL document exceeds %d bytes", maxTextQueryBytes)
	}
	depth := 0
	for i := 0; i < len(document); i++ {
		switch document[i] {
		case '#':
			for i+1 < len(document) && document[i+1] != '\n' && document[i+1] != '\r' {
				i++
			}
		case '"':
			if i+2 < len(document) && document[i:i+3] == `"""` {
				i += 3
				for i+2 < len(document) && document[i:i+3] != `"""` {
					i++
				}
				i += 2
				continue
			}
			for i++; i < len(document); i++ {
				if document[i] == '\\' {
					i++
					continue
				}
				if document[i] == '"' {
					break
				}
			}
		case '{', '(', '[':
			depth++
			if depth > maxFilterExpressionDepth {
				return fmt.Errorf("GraphQL document nesting exceeds %d", maxFilterExpressionDepth)
			}
		case '}', ')', ']':
			if depth > 0 {
				depth--
			}
		}
	}
	return nil
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
	if err := validateJSONValueShape(object); err != nil {
		return Request{}, fmt.Errorf("graph request %v", err)
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
		pathValue = cloneGraphQLMap(pathValue)
		request["path"] = pathValue
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
	steps, hasSteps := path["steps"].([]any)
	if hasSteps {
		steps = append([]any(nil), steps...)
		path["steps"] = steps
	}
	for i, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		step = cloneGraphQLMap(step)
		steps[i] = step
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
		output[key] = value
	}
	return output
}
