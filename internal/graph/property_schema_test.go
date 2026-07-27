package graph

import "testing"

func TestPropertySchemaDefaultsAndValidation(t *testing.T) {
	specs, err := NormalizePropertyFieldSpecs("depends_on", map[string]FieldSpec{
		"status": {Type: "string", Required: true, Default: "active", Enum: []any{"active", "inactive"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fields := ApplyPropertyDefaults(nil, specs)
	if fields["status"] != "active" {
		t.Fatalf("default fields=%#v", fields)
	}
	if err := ValidatePropertyFields("edge depends_on", fields, specs, true); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePropertyFields("edge depends_on", Fields{"status": "unknown"}, specs, true); err == nil {
		t.Fatal("invalid enum unexpectedly passed")
	}
	if err := ValidatePropertyFields("edge depends_on", Fields{"status": "active", "weight": 1}, specs, true); err == nil {
		t.Fatal("strict undeclared field unexpectedly passed")
	}
}
