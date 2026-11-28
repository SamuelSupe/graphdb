package graph

import (
	"reflect"
	"slices"
	"time"
)

func copyCIType(ciType CIType) CIType {
	ciType.Extends = append([]string(nil), ciType.Extends...)
	if ciType.IdentityKeys != nil {
		keys := make([]IdentityKey, len(ciType.IdentityKeys))
		for i, key := range ciType.IdentityKeys {
			key.Fields = append([]string(nil), key.Fields...)
			keys[i] = key
		}
		ciType.IdentityKeys = keys
	}
	fields := make(map[string]FieldSpec, len(ciType.Fields))
	for name, spec := range ciType.Fields {
		spec.Enum = copyAnySlice(spec.Enum)
		spec.Default = copyAny(spec.Default)
		fields[name] = spec
	}
	if ciType.Fields != nil {
		ciType.Fields = fields
	}
	return ciType
}

func copyRelationType(relationType RelationType) RelationType {
	relationType.FromKinds = append([]string(nil), relationType.FromKinds...)
	relationType.ToKinds = append([]string(nil), relationType.ToKinds...)
	return relationType
}

func copyFields(fields Fields) Fields {
	if fields == nil {
		return Fields{}
	}
	out := make(Fields, len(fields))
	for key, value := range fields {
		out[key] = copyAny(value)
	}
	return out
}

func copyEntity(entity Entity) Entity {
	entity.Fields = copyFields(entity.Fields)
	entity.FieldWriteModes = copyFieldWriteModes(entity.FieldWriteModes)
	entity.FieldSources = copyFieldSources(entity.FieldSources)
	entity.FieldConflicts = append([]FieldConflict(nil), entity.FieldConflicts...)
	for i := range entity.FieldConflicts {
		entity.FieldConflicts[i].ExistingValue = copyAny(entity.FieldConflicts[i].ExistingValue)
		entity.FieldConflicts[i].IncomingValue = copyAny(entity.FieldConflicts[i].IncomingValue)
	}
	if entity.ExistenceSource != nil {
		source := *entity.ExistenceSource
		entity.ExistenceSource = &source
	}
	entity.Identity = copyIdentity(entity.Identity)
	entity.Sources = append([]EntitySource(nil), entity.Sources...)
	entity.MergedFrom = append([]string(nil), entity.MergedFrom...)
	return entity
}

func copyFieldWriteModes(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func CopyEntity(entity Entity) Entity {
	return copyEntity(entity)
}

func copyEdge(edge Edge) Edge {
	edge.Fields = copyFields(edge.Fields)
	edge.FieldSources = copyFieldSources(edge.FieldSources)
	edge.Sources = append([]EdgeSource(nil), edge.Sources...)
	if edge.ExistenceSource != nil {
		source := *edge.ExistenceSource
		edge.ExistenceSource = &source
	}
	return edge
}

func CopyEdge(edge Edge) Edge {
	return copyEdge(edge)
}

func copyIdentity(identity map[string]any) map[string]any {
	if identity == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(identity))
	for key, value := range identity {
		out[key] = copyAny(value)
	}
	return out
}

func copyAny(value any) any {
	switch typed := value.(type) {
	case nil, string, bool, float64, float32, int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8, time.Time:
		return value
	case Fields:
		return copyFields(typed)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, nested := range typed {
			out[key] = copyAny(nested)
		}
		return out
	case []any:
		return copyAnySlice(typed)
	case []string:
		return slices.Clone(typed)
	case []int:
		return slices.Clone(typed)
	case []int64:
		return slices.Clone(typed)
	case []float64:
		return slices.Clone(typed)
	case []bool:
		return slices.Clone(typed)
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice:
			if rv.IsNil() {
				return value
			}
			out := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
			for i := 0; i < rv.Len(); i++ {
				if cloned := copyAny(rv.Index(i).Interface()); cloned != nil {
					out.Index(i).Set(reflect.ValueOf(cloned))
				}
			}
			return out.Interface()
		case reflect.Map:
			if rv.IsNil() {
				return value
			}
			out := reflect.MakeMapWithSize(rv.Type(), rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				clonedVal := copyAny(iter.Value().Interface())
				if clonedVal == nil {
					out.SetMapIndex(iter.Key(), reflect.Zero(rv.Type().Elem()))
				} else {
					out.SetMapIndex(iter.Key(), reflect.ValueOf(clonedVal))
				}
			}
			return out.Interface()
		default:
			return value
		}
	}
}

func copyAnySlice(values []any) []any {
	if values == nil {
		return nil
	}
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = copyAny(value)
	}
	return out
}

func copyFieldSources(sources map[string]FieldSource) map[string]FieldSource {
	if sources == nil {
		return nil
	}
	out := make(map[string]FieldSource, len(sources))
	for field, source := range sources {
		out[field] = source
	}
	return out
}

func copyFieldIndex(index map[string]map[string]*fieldValueIndex) map[string]map[string]*fieldValueIndex {
	out := make(map[string]map[string]*fieldValueIndex, len(index))
	for kind, byField := range index {
		out[kind] = make(map[string]*fieldValueIndex, len(byField))
		for field, byValue := range byField {
			values := NewShardedMap[*ShardedMap[struct{}]]()
			for value, ids := range byValue.All() {
				values.Set(value, ids.Clone())
			}
			out[kind][field] = values
		}
	}
	return out
}

func copyStringMap(values map[string]*ShardedMap[string]) map[string]*ShardedMap[string] {
	out := make(map[string]*ShardedMap[string], len(values))
	for key, inner := range values {
		out[key] = inner.Clone()
	}
	return out
}

func copySet(values map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for value := range values {
		out[value] = struct{}{}
	}
	return out
}
