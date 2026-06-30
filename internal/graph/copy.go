package graph

func copyCIType(ciType CIType) CIType {
	ciType.Extends = append([]string(nil), ciType.Extends...)
	ciType.IdentityKeys = append([]IdentityKey(nil), ciType.IdentityKeys...)
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
	entity.FieldSources = copyFieldSources(entity.FieldSources)
	if entity.ExistenceSource != nil {
		source := *entity.ExistenceSource
		entity.ExistenceSource = &source
	}
	entity.Identity = copyIdentity(entity.Identity)
	entity.Sources = append([]EntitySource(nil), entity.Sources...)
	entity.MergedFrom = append([]string(nil), entity.MergedFrom...)
	return entity
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
	case nil:
		return nil
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
		return append([]string(nil), typed...)
	case []int:
		return append([]int(nil), typed...)
	case []int64:
		return append([]int64(nil), typed...)
	case []float64:
		return append([]float64(nil), typed...)
	case []bool:
		return append([]bool(nil), typed...)
	default:
		return value
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

func copySetMap(values map[string]map[string]struct{}) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{}, len(values))
	for key, set := range values {
		out[key] = copySet(set)
	}
	return out
}

func copyFieldIndex(index map[string]map[string]map[string]map[string]struct{}) map[string]map[string]map[string]map[string]struct{} {
	out := make(map[string]map[string]map[string]map[string]struct{}, len(index))
	for kind, byField := range index {
		out[kind] = make(map[string]map[string]map[string]struct{}, len(byField))
		for field, byValue := range byField {
			out[kind][field] = make(map[string]map[string]struct{}, len(byValue))
			for value, ids := range byValue {
				out[kind][field][value] = copySet(ids)
			}
		}
	}
	return out
}

func copyStringMap(values map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(values))
	for key, inner := range values {
		out[key] = make(map[string]string, len(inner))
		for innerKey, innerValue := range inner {
			out[key][innerKey] = innerValue
		}
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
