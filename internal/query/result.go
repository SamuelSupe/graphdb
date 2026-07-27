package query

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const timeSortLayout = "2006-01-02T15:04:05.000000000Z07:00"

func compareResults(left, right Result, specs []SortSpec) int {
	if len(specs) == 0 {
		specs = []SortSpec{{Field: "id"}}
	}
	for _, spec := range specs {
		lv := resultValue(left, spec.Field)
		rv := resultValue(right, spec.Field)
		cmp := compareAny(lv, rv)
		if cmp == 0 {
			continue
		}
		if spec.Desc {
			return -cmp
		}
		return cmp
	}
	return strings.Compare(resultIdentity(left), resultIdentity(right))
}

func resultValue(result Result, field string) any {
	if result.Entity != nil {
		return entityValue(*result.Entity, field)
	}
	if result.Edge != nil {
		switch field {
		case "id":
			return result.Edge.ID
		case "type", "relation_type":
			return result.Edge.Type
		case "from":
			return result.Edge.From
		case "to":
			return result.Edge.To
		}
		return result.Edge.Fields[field]
	}
	if result.Path != nil {
		switch field {
		case "length", "depth":
			return len(result.Path.Edges)
		case "end_id":
			return pathEnd(*result.Path).ID
		}
	}
	return nil
}

func resultIdentity(result Result) string {
	if result.Edge != nil {
		if result.Direction != "" {
			return "edge:" + result.Direction + ":" + result.Edge.ID
		}
		return "edge:" + result.Edge.ID
	}
	if result.Entity != nil {
		return "entity:" + result.Entity.ID
	}
	if result.Path != nil {
		entityIDs := make([]string, 0, len(result.Path.Entities))
		for _, entity := range result.Path.Entities {
			entityIDs = append(entityIDs, entity.ID)
		}
		encoded, _ := json.Marshal(entityIDs)
		return legacyPathIdentity(*result.Path) + "\x1f" +
			base64.RawURLEncoding.EncodeToString(encoded)
	}
	return "empty"
}

func legacyPathIdentity(path graph.Path) string {
	parts := make([]string, 0, len(path.Edges))
	for _, edge := range path.Edges {
		parts = append(parts, edge.ID)
	}
	return "path:" + strings.Join(parts, ">")
}

func resultMatchesCursor(result Result, after string) bool {
	if resultIdentity(result) == after {
		return true
	}
	return result.Path != nil && legacyPathIdentity(*result.Path) == after
}

func compareAny(left, right any) int {
	lf, lok := asFloat(left)
	rf, rok := asFloat(right)
	if lok && rok {
		if lf < rf {
			return -1
		}
		if lf > rf {
			return 1
		}
		return 0
	}
	return strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
}

func applyProjection(result *Result, fields []string) {
	if len(fields) == 0 || result.Entity == nil {
		return
	}
	projected := map[string]any{}
	entityFields := graph.Fields{}
	for _, field := range fields {
		value := entityValue(*result.Entity, field)
		projected[field] = value
		name, ok := projectionEntityFieldName(field)
		if !ok {
			continue
		}
		if field == "labels" {
			if _, exists := result.Entity.Fields[graph.ReservedLabelsField]; !exists {
				name = "labels"
			}
		}
		if _, ok := result.Entity.Fields[name]; ok {
			entityFields[name] = value
		}
	}
	result.Fields = projected
	result.Entity.Fields = entityFields
	trimEntityFieldSources(result.Entity, entityFields)
	trimEntityFieldWriteModes(result.Entity, entityFields)
}

func projectionEntityFieldName(field string) (string, bool) {
	if field == "labels" {
		return graph.ReservedLabelsField, true
	}
	switch field {
	case "", "id", "kind", "source", "external_id", "confidence", "source_priority", "created_at", "updated_at":
		return "", false
	}
	if strings.HasPrefix(field, "identity.") {
		return "", false
	}
	if strings.HasPrefix(field, "fields.") {
		name := strings.TrimPrefix(field, "fields.")
		return name, name != ""
	}
	return field, true
}

func trimEntityFieldSources(entity *graph.Entity, fields graph.Fields) {
	if entity == nil || len(entity.FieldSources) == 0 {
		return
	}
	if len(fields) == 0 {
		entity.FieldSources = nil
		return
	}
	next := map[string]graph.FieldSource{}
	for field := range fields {
		if source, ok := entity.FieldSources[field]; ok {
			next[field] = source
		}
	}
	if len(next) == 0 {
		entity.FieldSources = nil
		return
	}
	entity.FieldSources = next
}

func trimEntityFieldWriteModes(entity *graph.Entity, fields graph.Fields) {
	if entity == nil || len(entity.FieldWriteModes) == 0 {
		return
	}
	if len(fields) == 0 {
		entity.FieldWriteModes = nil
		return
	}
	next := make(map[string]string, min(len(entity.FieldWriteModes), len(fields)))
	for field := range fields {
		if mode, ok := entity.FieldWriteModes[field]; ok {
			next[field] = mode
		}
	}
	if len(next) == 0 {
		entity.FieldWriteModes = nil
		return
	}
	entity.FieldWriteModes = next
}

func pathEnd(path graph.Path) graph.Entity {
	if len(path.Entities) == 0 {
		return graph.Entity{}
	}
	return path.Entities[len(path.Entities)-1]
}
