package query

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestEnhancedFiltersSortProjectionAggregationAndStableCursor(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:   "match",
		Kind: "host",
		Where: []Filter{
			{Field: "cpu", Op: "gte", Value: 8},
			{Field: "region", Op: "in", Value: []any{"us-east-1", "eu-west-1"}},
			{Field: "owner", Op: "exists", Value: true},
			{Field: "hostname", Op: "prefix", Value: "app-"},
		},
		Sort:      []SortSpec{{Field: "cpu", Desc: true}},
		Project:   []string{"id", "hostname", "cpu"},
		Aggregate: []Aggregation{{Op: "count"}, {Op: "avg", Field: "cpu", Name: "avg_cpu"}},
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("enhanced match: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:app-02" {
		t.Fatalf("first page = %#v", response.Results)
	}
	if response.Results[0].Fields["cpu"] != float64(16) {
		t.Fatalf("projection cpu = %#v", response.Results[0].Fields)
	}
	if response.Aggregates["count"] != 2 {
		t.Fatalf("count aggregate = %#v", response.Aggregates["count"])
	}
	if response.NextCursor == "" {
		t.Fatal("expected stable cursor")
	}
	next, err := Execute(g, Request{
		Op:   "match",
		Kind: "host",
		Where: []Filter{
			{Field: "cpu", Op: "gte", Value: 8},
			{Field: "region", Op: "in", Value: []any{"us-east-1", "eu-west-1"}},
			{Field: "owner", Op: "exists", Value: true},
			{Field: "hostname", Op: "prefix", Value: "app-"},
		},
		Sort:      []SortSpec{{Field: "cpu", Desc: true}},
		Project:   []string{"id", "hostname", "cpu"},
		Aggregate: []Aggregation{{Op: "count"}, {Op: "avg", Field: "cpu", Name: "avg_cpu"}},
		Limit:     1,
		Cursor:    response.NextCursor,
	})
	if err != nil {
		t.Fatalf("cursor page 2: %v", err)
	}
	if len(next.Results) != 1 || next.Results[0].Entity.ID != "host:app-01" {
		t.Fatalf("second page = %#v", next.Results)
	}
}

func TestRangeFiltersTreatJSONNumbersAsNumbers(t *testing.T) {
	g := graph.New()
	err := g.ApplyCommit(graph.Commit{
		ID:      "json-number-range",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "host:small", Kind: "host", Fields: graph.Fields{"cpu": json.Number("2")}},
				{ID: "host:large", Kind: "host", Fields: graph.Fields{"cpu": json.Number("10")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	response, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "cpu", Op: "gt", Value: json.Number("2")}},
		Sort:  []SortSpec{{Field: "cpu"}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("range match: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:large" {
		t.Fatalf("results = %#v, want host:large only", response.Results)
	}
}

func TestEqualityFiltersTreatNumericRepresentationsAsNumbers(t *testing.T) {
	g := seedCMDBGraph(t)
	eq, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "cpu", Op: "eq", Value: 8}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("eq match: %v", err)
	}
	if len(eq.Results) != 1 || eq.Results[0].Entity.ID != "host:app-01" {
		t.Fatalf("eq results = %#v, want host:app-01 only", eq.Results)
	}

	in, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "cpu", Op: "in", Value: []any{json.Number("16.0")}}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("in match: %v", err)
	}
	if len(in.Results) != 1 || in.Results[0].Entity.ID != "host:app-02" {
		t.Fatalf("in results = %#v, want host:app-02 only", in.Results)
	}
}

func TestFuzzyFilterSupportsUnicodeText(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "unicode-fuzzy",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "service:db", Kind: "service", Fields: graph.Fields{"name": "数据库服务"}},
				{ID: "service:cache", Kind: "service", Fields: graph.Fields{"name": "缓存服务"}},
				{ID: "service:api", Kind: "service", Fields: graph.Fields{"name": "APIService"}},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	response, err := Execute(g, Request{
		Op:    "match",
		Kind:  "service",
		Where: []Filter{{Field: "name", Op: "fuzzy", Value: "据服"}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("fuzzy match: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "service:db" {
		t.Fatalf("results = %#v, want service:db only", response.Results)
	}
	response, err = Execute(g, Request{
		Op:    "match",
		Kind:  "service",
		Where: []Filter{{Field: "name", Op: "fuzzy", Value: "apisvc"}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("case-insensitive fuzzy match: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "service:api" {
		t.Fatalf("case-insensitive results = %#v, want service:api only", response.Results)
	}
}

func TestEmptyFieldsPrefixIsNotAQueryableSchemalessField(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "empty-fields-prefix",
		Version: 1,
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
		}}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if field, ok := indexedField("fields."); ok || field != "" {
		t.Fatalf("fields. indexed as field=%q ok=%v", field, ok)
	}
	plan := PlanQuery(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "fields.", Op: "eq", Value: "bad"}},
	})
	if plan.Strategy == "field-index" {
		t.Fatalf("fields. should not use field index: %#v", plan)
	}
	response, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "fields.", Op: "exists", Value: true}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(response.Results) != 0 {
		t.Fatalf("fields. matched entities: %#v", response.Results)
	}
}

func TestProjectionDoesNotChangeSortOrAggregateInputs(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:        "match",
		Kind:      "host",
		Where:     []Filter{{Field: "owner", Op: "exists", Value: true}},
		Sort:      []SortSpec{{Field: "cpu", Desc: true}},
		Project:   []string{"hostname"},
		Aggregate: []Aggregation{{Op: "avg", Field: "cpu", Name: "avg_cpu"}},
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:app-02" {
		t.Fatalf("results = %#v, want highest cpu first", response.Results)
	}
	if _, ok := response.Results[0].Fields["cpu"]; ok {
		t.Fatalf("projection leaked cpu: %#v", response.Results[0].Fields)
	}
	if response.Aggregates["avg_cpu"] != float64(12) {
		t.Fatalf("avg_cpu = %#v, want 12", response.Aggregates["avg_cpu"])
	}
}

func TestBoundedIndexedMatchReturnsOwnedEntities(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:   "match",
		Kind: "host",
		Where: []Filter{
			{Field: "region", Op: "eq", Value: "us-east-1"},
			{Field: "cpu", Op: "gte", Value: 8},
		},
		Sort:      []SortSpec{{Field: "cpu", Desc: true}},
		Aggregate: []Aggregation{{Op: "count"}},
		Limit:     1,
		Profile:   true,
	})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if response.Plan == nil || response.Plan.Strategy != "field-index" {
		t.Fatalf("plan = %#v, want field-index", response.Plan)
	}
	if len(response.Results) != 1 || response.Results[0].Entity == nil {
		t.Fatalf("results = %#v", response.Results)
	}
	response.Results[0].Entity.Fields["hostname"] = "mutated"
	original, ok := g.GetEntity(response.Results[0].Entity.ID)
	if !ok {
		t.Fatalf("entity %q missing from graph", response.Results[0].Entity.ID)
	}
	if original.Fields["hostname"] == "mutated" {
		t.Fatal("mutating the response changed the graph entity")
	}
}

func TestProjectionTrimsFieldSources(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:      "match",
		Kind:    "host",
		Where:   []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Project: []string{"hostname"},
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity == nil {
		t.Fatalf("results = %#v", response.Results)
	}
	sources := response.Results[0].Entity.FieldSources
	if len(sources) != 1 {
		t.Fatalf("field_sources = %#v, want only hostname", sources)
	}
	if _, ok := sources["hostname"]; !ok {
		t.Fatalf("field_sources = %#v, missing hostname", sources)
	}
}

func TestProjectionTrimsFieldWriteModes(t *testing.T) {
	entity := graph.Entity{
		ID:              "host:modes",
		Kind:            "host",
		Fields:          graph.Fields{"keep": "value", "drop": "unused"},
		FieldWriteModes: map[string]string{"keep": graph.FieldMergeReplace, "drop": graph.FieldMergeReplace},
	}
	result := Result{Entity: &entity}
	applyProjection(&result, []string{"keep"})
	modes := result.Entity.FieldWriteModes
	if len(modes) != 1 || modes["keep"] != graph.FieldMergeReplace {
		t.Fatalf("field_write_modes = %#v, want only keep", modes)
	}
}

func TestProjectionKeepsMetaFieldsSeparateFromSchemalessFields(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "projection-meta-fields",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID:     "host:meta",
				Kind:   "host",
				Fields: graph.Fields{"id": "field-id", "kind": "field-kind", "hostname": "app-01"},
			}},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	response, err := Execute(g, Request{
		Op:      "match",
		Kind:    "host",
		Project: []string{"id", "kind", "fields.id", "hostname"},
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(response.Results) != 1 {
		t.Fatalf("results = %#v", response.Results)
	}
	result := response.Results[0]
	if result.Fields["id"] != "host:meta" || result.Fields["kind"] != "host" || result.Fields["fields.id"] != "field-id" {
		t.Fatalf("projected fields = %#v", result.Fields)
	}
	if result.Entity.Fields["id"] != "field-id" || result.Entity.Fields["hostname"] != "app-01" {
		t.Fatalf("entity fields = %#v", result.Entity.Fields)
	}
	if _, ok := result.Entity.Fields["kind"]; ok {
		t.Fatalf("entity fields kept meta kind projection: %#v", result.Entity.Fields)
	}
}

func TestExistsFilterUsesFieldPresenceForNullValues(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "exists-null-field",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "host:null-owner", Kind: "host", Fields: graph.Fields{"owner": nil}},
				{ID: "host:missing-owner", Kind: "host", Fields: graph.Fields{"hostname": "missing-owner"}},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	present, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "fields.owner", Op: "exists", Value: true}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("exists true: %v", err)
	}
	if len(present.Results) != 1 || present.Results[0].Entity.ID != "host:null-owner" {
		t.Fatalf("exists true results = %#v, want null-owner only", present.Results)
	}

	missing, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "owner", Op: "exists", Value: false}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("exists false: %v", err)
	}
	if len(missing.Results) != 1 || missing.Results[0].Entity.ID != "host:missing-owner" {
		t.Fatalf("exists false results = %#v, want missing-owner only", missing.Results)
	}
}

func TestMissingFieldsDoNotMatchValueFilters(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "missing-field-filters",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "host:null-owner", Kind: "host", Fields: graph.Fields{"owner": nil}},
				{ID: "host:platform", Kind: "host", Fields: graph.Fields{"owner": "platform"}},
				{ID: "host:missing-owner", Kind: "host", Fields: graph.Fields{"hostname": "missing-owner"}},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eqNull, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "owner", Op: "eq", Value: nil}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("eq null: %v", err)
	}
	if len(eqNull.Results) != 1 || eqNull.Results[0].Entity.ID != "host:null-owner" {
		t.Fatalf("eq null results = %#v, want explicit null only", eqNull.Results)
	}

	for _, filter := range []Filter{
		{Field: "owner", Op: "contains", Value: "nil"},
		{Field: "owner", Op: "fuzzy", Value: "nil"},
		{Field: "owner", Op: "lt", Value: "zzz"},
		{Field: "owner", Op: "neq", Value: "platform"},
	} {
		response, err := Execute(g, Request{Op: "match", Kind: "host", Where: []Filter{filter}, Limit: 10})
		if err != nil {
			t.Fatalf("%s filter: %v", filter.Op, err)
		}
		for _, result := range response.Results {
			if result.Entity.ID == "host:missing-owner" {
				t.Fatalf("%s matched missing field: %#v", filter.Op, response.Results)
			}
		}
	}
}

func TestNeighborProjectionDoesNotChangeSortInput(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "neighbor-projection-sort",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "cpu": 1}},
				{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02", "cpu": 2}},
			},
			UpsertEdges: []graph.Edge{
				{ID: "edge:api-host-1", Type: "runs_on", From: "service:api", To: "host:app-01"},
				{ID: "edge:api-host-2", Type: "runs_on", From: "service:api", To: "host:app-02"},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	response, err := Execute(g, Request{
		Op:           "neighbors",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
		Sort:         []SortSpec{{Field: "cpu", Desc: true}},
		Project:      []string{"hostname"},
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 2 || response.Results[0].Entity.ID != "host:app-02" {
		t.Fatalf("results = %#v, want highest cpu first", response.Results)
	}
	if _, ok := response.Results[0].Fields["cpu"]; ok {
		t.Fatalf("projection leaked cpu: %#v", response.Results[0].Fields)
	}
}

func TestMetadataFilterDoesNotUseFieldIndex(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:    "match",
		Where: []Filter{{Field: "id", Op: "eq", Value: "service:api"}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("metadata filter: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "service:api" {
		t.Fatalf("metadata filter results = %#v", response.Results)
	}
}

func TestNonScalarEqualityFilterDoesNotUseFieldIndex(t *testing.T) {
	g := graph.New()
	err := g.ApplyCommit(graph.Commit{
		ID:      "non-scalar-filter",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"meta": {Type: "object", Indexed: true}}}},
			UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{
				"meta": map[string]any{"env": "prod"},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	request := Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "meta", Op: "eq", Value: map[string]any{"env": "prod"}}},
	}
	plan := PlanQuery(g, request)
	if plan.Strategy == "field-index" {
		t.Fatalf("non-scalar filter used field index: %#v", plan)
	}
	response, err := Execute(g, request)
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:app-01" {
		t.Fatalf("results = %#v", response.Results)
	}
}

func TestLazyReadRequiresEntityPagesForMaterialization(t *testing.T) {
	request := Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
	}
	stats := PlannerStats{
		Version: 1,
		Indexes: []PlannerIndexStat{{
			Kind: "host", Field: "hostname", Status: "ready", EntryCount: 1, DistinctValues: 1,
		}},
	}
	if SupportsLazyRead(request, stats) {
		t.Fatal("lazy read should be disabled when catalog has no entity pages")
	}
	stats.EntityPages = []PlannerEntityPageStat{{Shard: "68", EntityCount: 1}}
	if !SupportsLazyRead(request, stats) {
		t.Fatal("lazy read should be enabled when index and entity pages are available")
	}
}

func TestImpactShortestPathAndPathFilter(t *testing.T) {
	g := seedCMDBGraph(t)
	impact, err := Execute(g, Request{
		Op:    "impact",
		ID:    "service:frontend",
		Depth: 2,
		Path:  PathFilter{EndKind: "database"},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("impact: %v", err)
	}
	if len(impact.Results) != 1 {
		t.Fatalf("impact results = %#v", impact.Results)
	}
	if pathEnd(*impact.Results[0].Path).ID != "db:postgres" {
		t.Fatalf("impact path end = %#v", pathEnd(*impact.Results[0].Path))
	}

	shortest, err := Execute(g, Request{
		Op:           "shortest_path",
		ID:           "service:frontend",
		TargetID:     "db:postgres",
		RelationType: "depends_on",
		Direction:    "out",
		Depth:        4,
		Project:      []string{"name"},
	})
	if err != nil {
		t.Fatalf("shortest_path: %v", err)
	}
	if len(shortest.Results) != 1 || len(shortest.Results[0].Path.Edges) != 2 {
		t.Fatalf("shortest path = %#v", shortest.Results)
	}
	if shortest.Results[0].Fields["name"] != "postgres" {
		t.Fatalf("shortest path projection = %#v", shortest.Results[0].Fields)
	}
}

func TestQueryCostLimit(t *testing.T) {
	g := seedCMDBGraph(t)
	_, err := Execute(g, Request{
		Op:        "traverse",
		ID:        "service:frontend",
		Direction: "out",
		Depth:     4,
		CostLimit: 1,
	})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestInvalidFilterAndAggregateOpsReturnErrInvalid(t *testing.T) {
	g := seedCMDBGraph(t)
	_, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "hostname", Op: "regex", Value: "app-.*"}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("filter err = %v, want ErrInvalid", err)
	}
	_, err = Execute(g, Request{
		Op:        "match",
		Kind:      "host",
		Aggregate: []Aggregation{{Op: "median", Field: "cpu"}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("aggregate err = %v, want ErrInvalid", err)
	}
	_, err = Execute(g, Request{
		Op:        "match",
		Kind:      "host",
		Aggregate: []Aggregation{{Op: "avg"}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("aggregate missing field err = %v, want ErrInvalid", err)
	}
	_, err = Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "owner", Op: "exists", Value: "yes"}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("exists value err = %v, want ErrInvalid", err)
	}
	_, err = Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "region", Op: "in", Value: "us-east-1"}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("in scalar value err = %v, want ErrInvalid", err)
	}
}

func TestInvalidQueryControlsReturnErrInvalid(t *testing.T) {
	g := seedCMDBGraph(t)
	cases := []Request{
		{Op: "match", Kind: "host", Limit: -1},
		{Op: "match", Kind: "host", TimeoutMS: -1},
		{Op: "match", Kind: "host", CostLimit: -1},
		{Op: "traverse", ID: "service:frontend", Depth: -1},
		{Op: "traverse", ID: "service:frontend", Path: PathFilter{MaxPaths: -1}},
		{Op: "traverse", ID: "service:frontend", DirectionStrategy: "reverse"},
		{Op: "shortest_path", ID: "service:frontend", TargetID: "db:primary", Direction: "sideways"},
	}
	for _, request := range cases {
		if _, err := Execute(g, request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("request %#v err = %v, want ErrInvalid", request, err)
		}
	}
}

func TestQueryShapeLimitsRejectAmplification(t *testing.T) {
	expression := &FilterExpr{Field: "region", Op: "eq", Value: "us-east-1"}
	for range maxFilterExpressionDepth {
		expression = &FilterExpr{Op: "not", Children: []FilterExpr{*expression}}
	}
	cases := []Request{
		{
			Op: "match", Kind: "host",
			Where: []Filter{{Field: "region", Op: "in", Value: make([]string, maxInFilterValues+1)}},
		},
		{Op: "match", Kind: "host", Sort: make([]SortSpec, maxQuerySortSpecs+1)},
		{Op: "match", Kind: "host", WhereExpr: expression},
		{
			Op: "match", Kind: "host",
			WhereExpr: &FilterExpr{Field: "region", Op: "eq", Value: "us-east-1", Children: []FilterExpr{{Field: "owner", Op: "eq", Value: "team-a"}}},
		},
		{Op: "match", Kind: "host", CostLimit: maxQueryCostLimit + 1},
	}
	for _, request := range cases {
		if err := ValidateRequest(request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("request %#v err = %v, want ErrInvalid", request, err)
		}
	}
}

func TestExecuteContextPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ExecuteContext(ctx, seedCMDBGraph(t), Request{Op: "match", Kind: "host"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestExplainValidatesTargetRequest(t *testing.T) {
	g := seedCMDBGraph(t)
	_, err := Execute(g, Request{
		Op:       "explain",
		TargetOp: "match",
		Kind:     "host",
		Where:    []Filter{{Field: "hostname", Op: "regex", Value: "app-.*"}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("explain err = %v, want ErrInvalid", err)
	}
	_, err = Execute(g, Request{
		Op:        "explain",
		TargetOp:  "traverse",
		ID:        "service:frontend",
		CostLimit: -1,
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("explain control err = %v, want ErrInvalid", err)
	}
}

func TestExplainAndProfileExposeQueryPlan(t *testing.T) {
	g := seedCMDBGraph(t)
	explain, err := Execute(g, Request{
		Op:       "explain",
		TargetOp: "match",
		Kind:     "host",
		Where:    []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
	})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if explain.Plan == nil || explain.Plan.Strategy != "field-index" || explain.Plan.Index != "field:host.hostname" {
		t.Fatalf("explain plan = %#v", explain.Plan)
	}
	if len(explain.Results) != 0 {
		t.Fatalf("explain should not execute query: %#v", explain.Results)
	}

	profile, err := Execute(g, Request{
		Op:       "profile",
		TargetOp: "match",
		Kind:     "host",
		Where:    []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
	})
	if err != nil {
		t.Fatalf("profile: %v", err)
	}
	if profile.Plan == nil || profile.Plan.EstimatedRows != 1 || profile.Stats.Scanned != 1 || profile.Stats.Returned != 1 || len(profile.Results) != 1 {
		t.Fatalf("profile response = %#v", profile)
	}
}

func seedCMDBGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g := graph.New()
	err := g.ApplyCommit(graph.Commit{
		ID:      "cmdb-query",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{
				{
					Name: "host",
					Fields: map[string]graph.FieldSpec{
						"hostname": {Type: "string", Indexed: true},
						"region":   {Type: "string", Indexed: true},
						"cpu":      {Type: "number", Indexed: true},
						"owner":    {Type: "string"},
					},
				},
				{Name: "service"},
				{Name: "database"},
			},
			UpsertEntities: []graph.Entity{
				{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "us-east-1", "cpu": 8, "owner": "platform"}},
				{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02", "region": "eu-west-1", "cpu": 16, "owner": "platform"}},
				{ID: "host:db-01", Kind: "host", Fields: graph.Fields{"hostname": "db-01", "region": "us-east-1", "cpu": 4}},
				{ID: "service:frontend", Kind: "service", Fields: graph.Fields{"name": "frontend"}},
				{ID: "service:api", Kind: "service", Fields: graph.Fields{"name": "api"}},
				{ID: "db:postgres", Kind: "database", Fields: graph.Fields{"name": "postgres"}},
			},
			UpsertEdges: []graph.Edge{
				{ID: "edge:frontend-api", Type: "depends_on", From: "service:frontend", To: "service:api"},
				{ID: "edge:api-db", Type: "depends_on", From: "service:api", To: "db:postgres"},
				{ID: "edge:frontend-host", Type: "runs_on", From: "service:frontend", To: "host:app-01"},
				{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-02"},
			},
		},
	})
	if err != nil {
		t.Fatalf("seed cmdb graph: %v", err)
	}
	return g
}
