package graph

import "fmt"

// ApplyPropertyDefaults copies fields and fills schema defaults.
func ApplyPropertyDefaults(fields Fields, specs map[string]FieldSpec) Fields {
	out := copyFields(fields)
	for name, spec := range specs {
		if _, exists := out[name]; !exists && spec.Default != nil {
			out[name] = copyAny(spec.Default)
		}
	}
	return out
}

// ValidatePropertyFields validates schemaless properties against an optional schema.
func ValidatePropertyFields(resource string, fields Fields, specs map[string]FieldSpec, strict bool) error {
	for name, spec := range specs {
		value, exists := fields[name]
		if spec.Required && !exists {
			return fmt.Errorf("%s missing required field %q", resource, name)
		}
		if !exists {
			continue
		}
		if !valueMatchesType(value, spec.Type) {
			return fmt.Errorf("%s field %q does not match type %q", resource, name, spec.Type)
		}
		if len(spec.Enum) > 0 && !valueInEnum(value, spec.Enum) {
			return fmt.Errorf("%s field %q value is not in enum", resource, name)
		}
	}
	if strict {
		for name := range fields {
			if _, exists := specs[name]; !exists {
				return fmt.Errorf("%s has undeclared field %q", resource, name)
			}
		}
	}
	return nil
}
