package graph

import (
	"fmt"
	"strings"
)

const (
	FieldMergeReplace      = "replace"
	FieldMergeAppendUnique = "append_unique"
)

func normalizeCIType(ciType CIType) (CIType, error) {
	ciType.Name = strings.TrimSpace(ciType.Name)
	if ciType.Name == "" {
		return CIType{}, fmt.Errorf("ci type name is required")
	}
	ciType.Extends = append([]string(nil), ciType.Extends...)
	for i := range ciType.Extends {
		ciType.Extends[i] = strings.TrimSpace(ciType.Extends[i])
		if ciType.Extends[i] == "" {
			return CIType{}, fmt.Errorf("ci type %q has empty extends entry", ciType.Name)
		}
	}
	if ciType.Fields == nil {
		ciType.Fields = map[string]FieldSpec{}
	}
	fields, err := normalizeFieldSpecs(ciType.Name, ciType.Fields)
	if err != nil {
		return CIType{}, err
	}
	ciType.Fields = fields
	ciType.IdentityKeys = append([]IdentityKey(nil), ciType.IdentityKeys...)
	if err := normalizeIdentityKeys(ciType); err != nil {
		return CIType{}, err
	}
	return ciType, nil
}

func normalizeFieldSpecs(ciTypeName string, specs map[string]FieldSpec) (map[string]FieldSpec, error) {
	fields := make(map[string]FieldSpec, len(specs))
	for name, spec := range specs {
		fieldName := strings.TrimSpace(name)
		if fieldName == "" {
			return nil, fmt.Errorf("ci type %q has empty field name", ciTypeName)
		}
		if spec.Type == "" {
			spec.Type = "any"
		}
		if !validFieldType(spec.Type) {
			return nil, fmt.Errorf("ci type %q field %q has unsupported type %q", ciTypeName, fieldName, spec.Type)
		}
		spec.MergeStrategy = strings.TrimSpace(spec.MergeStrategy)
		if spec.MergeStrategy != "" && spec.MergeStrategy != FieldMergeReplace && spec.MergeStrategy != FieldMergeAppendUnique {
			return nil, fmt.Errorf("ci type %q field %q has unsupported merge_strategy %q", ciTypeName, fieldName, spec.MergeStrategy)
		}
		if spec.MergeStrategy == FieldMergeAppendUnique && spec.Type != "array" {
			return nil, fmt.Errorf("ci type %q field %q append_unique merge_strategy requires array type", ciTypeName, fieldName)
		}
		if spec.Default != nil && !valueMatchesType(spec.Default, spec.Type) {
			return nil, fmt.Errorf("ci type %q field %q default does not match type %q", ciTypeName, fieldName, spec.Type)
		}
		for _, value := range spec.Enum {
			if !valueMatchesType(value, spec.Type) {
				return nil, fmt.Errorf("ci type %q field %q enum value does not match type %q", ciTypeName, fieldName, spec.Type)
			}
		}
		spec.Default = copyAny(spec.Default)
		spec.Enum = copyAnySlice(spec.Enum)
		fields[fieldName] = spec
	}
	return fields, nil
}

func normalizeIdentityKeys(ciType CIType) error {
	for i, key := range ciType.IdentityKeys {
		key.Name = strings.TrimSpace(key.Name)
		if key.Name == "" {
			key.Name = fmt.Sprintf("identity_%d", i+1)
		}
		if key.Strategy == "" {
			key.Strategy = "merge"
		}
		if key.Strategy != "merge" && key.Strategy != "reject" {
			return fmt.Errorf("ci type %q identity key %q has unsupported strategy %q", ciType.Name, key.Name, key.Strategy)
		}
		if len(key.Fields) == 0 {
			return fmt.Errorf("ci type %q identity key %q requires fields", ciType.Name, key.Name)
		}
		key.Fields = append([]string(nil), key.Fields...)
		for j := range key.Fields {
			key.Fields[j] = strings.TrimSpace(key.Fields[j])
			if key.Fields[j] == "" {
				return fmt.Errorf("ci type %q identity key %q has empty field", ciType.Name, key.Name)
			}
		}
		ciType.IdentityKeys[i] = key
	}
	return nil
}
