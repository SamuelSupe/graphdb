package query

import (
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestTimestampPredicatesUseInstants(t *testing.T) {
	created := time.Date(2026, 10, 19, 0, 0, 0, 500000000, time.UTC)
	entity := graph.Entity{ID: "host:a", Kind: "host", CreatedAt: created, UpdatedAt: created}
	edge := graph.Edge{CreatedAt: created, UpdatedAt: created}
	for _, field := range []string{"created_at", "updated_at"} {
		for _, test := range []struct {
			op    string
			value any
			want  bool
		}{
			{"gte", "2026-10-19T00:00:00Z", true},
			{"gt", "2026-10-19T01:00:00+01:00", true},
			{"eq", "2026-10-19T01:00:00.5+01:00", true},
			{"neq", "2026-10-19T01:00:00.500000000+01:00", false},
			{"lt", "2026-10-19T00:00:01Z", true},
			{"lte", "2026-10-19T01:00:00.499999999+01:00", false},
			{"in", []any{"2026-10-19T01:00:00.5+01:00"}, true},
		} {
			filter := Filter{Field: field, Op: test.op, Value: test.value}
			if got := entityMatches(&entity, []Filter{filter}); got != test.want {
				t.Errorf("entity %s %s %v = %v", field, test.op, test.value, got)
			}
			if got := edgeMatches(edge, []Filter{filter}); got != test.want {
				t.Errorf("edge %s %s %v = %v", field, test.op, test.value, got)
			}
		}
		if !requestEntityMatches(&Request{Filters: graph.Fields{field: "2026-10-19T01:00:00.5+01:00"}}, &entity) {
			t.Errorf("legacy %s equality did not match equivalent instant", field)
		}
	}
	other := entity
	other.ID = "host:b"
	other.CreatedAt = created.Add(time.Second).In(time.FixedZone("west", -3600))
	if compareResults(Result{Entity: &entity}, Result{Entity: &other}, []SortSpec{{Field: "created_at"}}) >= 0 {
		t.Fatal("timestamps with different zones did not sort chronologically")
	}
}
