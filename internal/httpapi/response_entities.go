package httpapi

import (
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type queryResultJSONValue struct {
	query.Result
	Entity any `json:"entity,omitempty"`
	Path   any `json:"path,omitempty"`
}

func queryResultJSON(result query.Result) queryResultJSONValue {
	out := queryResultJSONValue{Result: result}
	if result.Entity != nil {
		out.Entity = result.Entity.JSONValue()
	}
	if result.Path != nil {
		out.Path = struct {
			*graph.Path
			Entities []any `json:"entities"`
		}{result.Path, entityJSONValues(result.Path.Entities)}
	}
	return out
}

// Use the entity's wire fields directly so the outer encoder does not have to
// validate and compact JSON produced by a nested Entity.MarshalJSON call.
func responseJSONValue(value any) any {
	switch value := value.(type) {
	case query.Response:
		var results []queryResultJSONValue
		if value.Results != nil {
			results = make([]queryResultJSONValue, len(value.Results))
		}
		for i, result := range value.Results {
			results[i] = queryResultJSON(result)
		}
		return struct {
			query.Response
			Results []queryResultJSONValue `json:"results"`
		}{value, results}
	case storage.EntityScanResult:
		return struct {
			storage.EntityScanResult
			Entities []any `json:"entities"`
		}{value, entityJSONValues(value.Entities)}
	default:
		return value
	}
}

func entityJSONValues(entities []graph.Entity) []any {
	if entities == nil {
		return nil
	}
	values := make([]any, len(entities))
	for i, entity := range entities {
		values[i] = entity.JSONValue()
	}
	return values
}
