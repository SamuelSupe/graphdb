package graph

import "testing"

func TestFailedCommitDoesNotLeakCopiedIndexMutation(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes:  []CIType{{Name: "host", Fields: map[string]FieldSpec{"hostname": {Type: "string", Indexed: true}}}},
			UpsertEntities: []Entity{{ID: "host:a", Kind: "host", Fields: Fields{"hostname": "app-01"}}},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := g.ApplyCommit(Commit{
		ID:      "bad",
		Version: 2,
		Mutations: Mutations{
			UpsertEntities: []Entity{{ID: "host:b", Kind: "host", Fields: Fields{"hostname": "app-02"}}},
			UpsertEdges:    []Edge{{ID: "bad-edge", Type: "runs_on", From: "host:b", To: "missing"}},
		},
	})
	if err == nil {
		t.Fatal("expected invalid edge")
	}
	matches := g.MatchEntities("host", Fields{"hostname": "app-02"})
	if len(matches) != 0 {
		t.Fatalf("failed commit leaked field index matches: %#v", matches)
	}
}

func TestGraphCopiesDoNotShareNestedFieldValues(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{Name: "host"}},
			UpsertEntities: []Entity{{
				ID:   "host:a",
				Kind: "host",
				Fields: Fields{
					"meta": map[string]any{"env": "prod"},
					"tags": []any{"blue"},
				},
				Identity: map[string]any{
					"cloud": map[string]any{"provider": "aws"},
				},
			}},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	first, ok := g.GetEntity("host:a")
	if !ok {
		t.Fatal("missing entity")
	}
	first.Fields["meta"].(map[string]any)["env"] = "staging"
	first.Fields["tags"].([]any)[0] = "red"
	first.Identity["cloud"].(map[string]any)["provider"] = "gcp"

	again, ok := g.GetEntity("host:a")
	if !ok {
		t.Fatal("missing entity on second read")
	}
	if got := again.Fields["meta"].(map[string]any)["env"]; got != "prod" {
		t.Fatalf("nested map mutation leaked through read copy: %v", got)
	}
	if got := again.Fields["tags"].([]any)[0]; got != "blue" {
		t.Fatalf("nested slice mutation leaked through read copy: %v", got)
	}
	if got := again.Identity["cloud"].(map[string]any)["provider"]; got != "aws" {
		t.Fatalf("nested identity mutation leaked through read copy: %v", got)
	}

	cloned := g.Clone()
	cloned.Entities["host:a"].Fields["meta"].(map[string]any)["env"] = "qa"
	afterCloneMutation, _ := g.GetEntity("host:a")
	if got := afterCloneMutation.Fields["meta"].(map[string]any)["env"]; got != "prod" {
		t.Fatalf("nested map mutation leaked through graph clone: %v", got)
	}
}

func TestNeighborCopiesDoNotShareNestedEdgeFieldValues(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{Name: "service"}, {Name: "host"}},
			UpsertRelationTypes: []RelationType{{
				Name:     "runs_on",
				FromKind: "service",
				ToKind:   "host",
				Directed: true,
			}},
			UpsertEntities: []Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "host:a", Kind: "host"},
			},
			UpsertEdges: []Edge{{
				ID:     "edge:collector",
				Type:   "runs_on",
				From:   "service:api",
				To:     "host:a",
				Fields: Fields{"meta": map[string]any{"port": float64(443)}},
			}},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	neighbors := g.Neighbors("service:api", "out", "runs_on")
	if len(neighbors) != 1 {
		t.Fatalf("expected one neighbor, got %d", len(neighbors))
	}
	neighbors[0].Edge.Fields["meta"].(map[string]any)["port"] = float64(80)

	again := g.Neighbors("service:api", "out", "runs_on")
	if len(again) != 1 {
		t.Fatalf("expected one neighbor on second read, got %d", len(again))
	}
	if got := again[0].Edge.Fields["meta"].(map[string]any)["port"]; got != float64(443) {
		t.Fatalf("nested edge field mutation leaked through neighbor copy: %v", got)
	}
}

func TestSchemaDefaultValuesDoNotShareNestedFieldValues(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertCITypes: []CIType{{
				Name: "host",
				Fields: map[string]FieldSpec{
					"meta": {Type: "object", Default: map[string]any{"env": "prod"}},
					"tags": {Type: "array", Default: []any{"blue"}},
				},
				IdentityKeys: []IdentityKey{{Name: "by_meta", Fields: []string{"meta"}}},
			}},
			UpsertEntities: []Entity{
				{ID: "host:a", Kind: "host"},
				{ID: "host:b", Kind: "host"},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	g.Entities["host:a"].Fields["meta"].(map[string]any)["env"] = "staging"
	g.Entities["host:a"].Fields["tags"].([]any)[0] = "red"
	g.Entities["host:a"].Identity["meta"].(map[string]any)["env"] = "identity"

	if got := g.Entities["host:b"].Fields["meta"].(map[string]any)["env"]; got != "prod" {
		t.Fatalf("default object shared between entities: %v", got)
	}
	if got := g.Entities["host:b"].Fields["tags"].([]any)[0]; got != "blue" {
		t.Fatalf("default array shared between entities: %v", got)
	}
	if got := g.CITypes["host"].Fields["meta"].Default.(map[string]any)["env"]; got != "prod" {
		t.Fatalf("default object shared with ci type: %v", got)
	}
	if got := g.Entities["host:a"].Fields["meta"].(map[string]any)["env"]; got != "staging" {
		t.Fatalf("identity mutation changed field value: %v", got)
	}
}

func TestCITypeDefaultsDoNotShareCallerValues(t *testing.T) {
	defaultMeta := map[string]any{"env": "prod"}
	defaultTags := []any{"blue"}
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{
			Name: "host",
			Fields: map[string]FieldSpec{
				"meta": {Type: "object", Default: defaultMeta},
				"tags": {Type: "array", Default: defaultTags},
			},
		}}},
	}); err != nil {
		t.Fatalf("schema: %v", err)
	}

	defaultMeta["env"] = "staging"
	defaultTags[0] = "red"

	if err := g.ApplyCommit(Commit{
		ID:      "entity",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:a", Kind: "host",
		}}},
	}); err != nil {
		t.Fatalf("entity: %v", err)
	}
	if got := g.Entities["host:a"].Fields["meta"].(map[string]any)["env"]; got != "prod" {
		t.Fatalf("ci type default shared caller object: %v", got)
	}
	if got := g.Entities["host:a"].Fields["tags"].([]any)[0]; got != "blue" {
		t.Fatalf("ci type default shared caller array: %v", got)
	}
}
