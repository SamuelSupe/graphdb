package httpapi

import (
	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

// Use the entity's wire fields directly so the outer encoder does not have to
// validate and compact JSON produced by a nested Entity.MarshalJSON call.
func responseJSONValue(value any) any {
	switch value := value.(type) {
	case query.Response:
		type resultValue struct {
			query.Result
			Entity any `json:"entity,omitempty"`
			Path   any `json:"path,omitempty"`
		}
		var results []resultValue
		if value.Results != nil {
			results = make([]resultValue, len(value.Results))
		}
		for i, result := range value.Results {
			results[i].Result = result
			if result.Entity != nil {
				results[i].Entity = result.Entity.JSONValue()
			}
			if result.Path != nil {
				results[i].Path = struct {
					*graph.Path
					Entities []any `json:"entities"`
				}{result.Path, entityJSONValues(result.Path.Entities)}
			}
		}
		return struct {
			query.Response
			Results []resultValue `json:"results"`
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
