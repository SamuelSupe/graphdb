package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

const (
	cypherMaxRows   = 100000
	cypherMaxMemory = 64 << 20
	cypherMaxDepth  = 16
)

// CypherRequest executes one read-only openCypher 9 statement. Parameters are
// values, never query fragments. MinVersion is a freshness floor, not history.
type CypherRequest struct {
	Query      string         `json:"query"`
	Parameters map[string]any `json:"parameters,omitempty"`
	MinVersion int64          `json:"min_version,omitempty"`
	AllowStale bool           `json:"allow_stale,omitempty"`
	TimeoutMS  int            `json:"timeout_ms,omitempty"`
	CostLimit  int            `json:"cost_limit,omitempty"`
	Profile    bool           `json:"profile,omitempty"`
}

// UnmarshalJSON preserves integer parameters before they enter expressions.
func (r *CypherRequest) UnmarshalJSON(data []byte) error {
	type plain CypherRequest
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	return d.Decode((*plain)(r))
}

type CypherResponse struct {
	Version int64          `json:"version"`
	Columns []string       `json:"columns"`
	Rows    [][]any        `json:"rows"`
	Stats   Stats          `json:"stats"`
	Plan    *Plan          `json:"plan,omitempty"`
	Profile []OperatorStat `json:"profile,omitempty"`
}

// CypherError identifies a compile or evaluation failure at its source position.
type CypherError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Line    int    `json:"line"`
	Column  int    `json:"column"`
}

func (e *CypherError) Error() string {
	return fmt.Sprintf("%s at %d:%d: %s", e.Code, e.Line, e.Column, e.Message)
}
func (e *CypherError) Unwrap() error { return ErrInvalid }

type cyPos struct{ line, column int }

func (p cyPos) fail(code, message string) error { return &CypherError{code, message, p.line, p.column} }

type cyExpr struct {
	op, name         string
	value            any
	slot             int
	args             []*cyExpr
	keys             []string
	distinct         bool
	pos              cyPos
	patterns         []cyPattern
	patternSlotStart int
}
type cyNodePattern struct {
	slot   int
	labels []string
	props  *cyExpr
}
type cyRelPattern struct {
	slot      int
	types     []string
	direction string
	min, max  int
	variable  bool
	props     *cyExpr
}
type cyPattern struct {
	slot     int
	nodes    []cyNodePattern
	rels     []cyRelPattern
	reversed bool
	shortest string
}
type cyItem struct {
	name string
	expr *cyExpr
	slot int
}
type cySort struct {
	expr *cyExpr
	desc bool
}
type cyClause struct {
	kind        string
	patterns    []cyPattern
	optional    bool
	newSlots    []int
	where, expr *cyExpr
	slot        int
	items       []cyItem
	sort        []cySort
	skip, limit *cyExpr
	distinct    bool
	aggregates  []*cyExpr
	groupBy     []*cyExpr
}
type cyBranch struct {
	clauses  []cyClause
	unionAll bool
	columns  []string
}

// CypherPlan is immutable and can be reused with the parameters supplied when
// compiling it. Execution receives only graph read APIs and query lookups.
type CypherPlan struct {
	request                   CypherRequest
	branches                  []cyBranch
	slots                     int
	explain, profile, reverse bool
	fields                    []string
	allFields                 bool
	definitions               map[int]*cyExpr
}

func (p *CypherPlan) ReadRequest() Request {
	return Request{Op: "cypher", TimeoutMS: p.request.TimeoutMS, CostLimit: p.request.CostLimit, MinVersion: p.request.MinVersion, AllowStale: p.request.AllowStale, Profile: p.profile}
}
func (p *CypherPlan) RequiresReverseIndex() bool {
	for _, b := range p.branches {
		for _, cl := range b.clauses {
			for _, pat := range cl.patterns {
				if len(pat.rels) > 0 {
					return true
				}
			}
		}
	}
	return p.reverse
}
func (p *CypherPlan) SupportsLazyRead(o ExecuteOptions) bool {
	_, pages := o.EntityLookup.(EntityPageLookup)
	return pages && len(o.PlannerStats.EntityPages) > 0 && (!p.reverse || reverseEdgeIndexAvailable(o.PlannerStats))
}

func (p *CypherPlan) Execute(ctx context.Context, g *graph.Graph, options ExecuteOptions) (CypherResponse, error) {
	return p.execute(ctx, g, options, nil)
}

// ExecuteStream emits rows synchronously so output backpressure and failures
// stop storage reads. Blocking operators still use the bounded query buffers.
func (p *CypherPlan) ExecuteStream(ctx context.Context, g *graph.Graph, options ExecuteOptions, emit func([]any) error) (CypherResponse, error) {
	return p.execute(ctx, g, options, emit)
}
func (p *CypherPlan) Columns() []string { return append([]string(nil), p.branches[0].columns...) }

func cyAggregate(e *cyExpr) bool {
	if e == nil || e.op != "call" {
		return false
	}
	switch e.name {
	case "count", "sum", "avg", "min", "max", "collect":
		return true
	}
	return false
}
func cyHasAggregate(e *cyExpr) bool {
	if e == nil {
		return false
	}
	if cyAggregate(e) {
		return true
	}
	for _, a := range e.args {
		if cyHasAggregate(a) {
			return true
		}
	}
	return false
}
