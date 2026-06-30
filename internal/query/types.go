package query

import (
	"context"
	"errors"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

var ErrInvalid = errors.New("invalid query")
var ErrLimitExceeded = errors.New("query limit exceeded")
var ErrIndexUnavailable = errors.New("persisted index unavailable")

type Request struct {
	Op                string        `json:"op"`
	TargetOp          string        `json:"target_op,omitempty"`
	Kind              string        `json:"kind,omitempty"`
	Filters           graph.Fields  `json:"filters,omitempty"`
	Where             []Filter      `json:"where,omitempty"`
	WhereExpr         *FilterExpr   `json:"where_expr,omitempty"`
	EdgeWhere         []Filter      `json:"edge_where,omitempty"`
	EdgeWhereExpr     *FilterExpr   `json:"edge_where_expr,omitempty"`
	ID                string        `json:"id,omitempty"`
	TargetID          string        `json:"target_id,omitempty"`
	Direction         string        `json:"direction,omitempty"`
	DirectionStrategy string        `json:"direction_strategy,omitempty"`
	RelationType      string        `json:"relation_type,omitempty"`
	RelationTypes     []string      `json:"relation_types,omitempty"`
	Depth             int           `json:"depth,omitempty"`
	Path              PathFilter    `json:"path,omitempty"`
	Sort              []SortSpec    `json:"sort,omitempty"`
	Project           []string      `json:"project,omitempty"`
	Aggregate         []Aggregation `json:"aggregate,omitempty"`
	GroupBy           []string      `json:"group_by,omitempty"`
	Having            []Filter      `json:"having,omitempty"`
	HavingExpr        *FilterExpr   `json:"having_expr,omitempty"`
	Limit             int           `json:"limit,omitempty"`
	Cursor            string        `json:"cursor,omitempty"`
	TimeoutMS         int           `json:"timeout_ms,omitempty"`
	CostLimit         int           `json:"cost_limit,omitempty"`
	Profile           bool          `json:"profile,omitempty"`
	MinVersion        int64         `json:"min_version,omitempty"`
	AllowStale        bool          `json:"allow_stale,omitempty"`
}

type Response struct {
	Version    int64            `json:"version"`
	Results    []Result         `json:"results"`
	NextCursor string           `json:"next_cursor,omitempty"`
	Stats      Stats            `json:"stats"`
	Aggregates map[string]any   `json:"aggregates,omitempty"`
	Groups     []AggregateGroup `json:"groups,omitempty"`
	Plan       *Plan            `json:"plan,omitempty"`
	Profile    []OperatorStat   `json:"profile,omitempty"`
}

type Result struct {
	Entity    *graph.Entity  `json:"entity,omitempty"`
	Edge      *graph.Edge    `json:"edge,omitempty"`
	Direction string         `json:"direction,omitempty"`
	Path      *graph.Path    `json:"path,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
	Score     int            `json:"score,omitempty"`
}

type Filter struct {
	Field string `json:"field"`
	Op    string `json:"op,omitempty"`
	Value any    `json:"value,omitempty"`
}

type FilterExpr struct {
	Op       string       `json:"op,omitempty"`
	Field    string       `json:"field,omitempty"`
	Value    any          `json:"value,omitempty"`
	Children []FilterExpr `json:"children,omitempty"`
}

type SortSpec struct {
	Field string `json:"field"`
	Desc  bool   `json:"desc,omitempty"`
}

type Aggregation struct {
	Name  string `json:"name,omitempty"`
	Op    string `json:"op"`
	Field string `json:"field,omitempty"`
}

type PathFilter struct {
	NodeKinds     []string    `json:"node_kinds,omitempty"`
	EndKind       string      `json:"end_kind,omitempty"`
	EndWhere      []Filter    `json:"end_where,omitempty"`
	EndWhereExpr  *FilterExpr `json:"end_where_expr,omitempty"`
	RelationTypes []string    `json:"relation_types,omitempty"`
	Steps         []PathStep  `json:"steps,omitempty"`
	MaxPaths      int         `json:"max_paths,omitempty"`
}

type PathStep struct {
	RelationTypes []string    `json:"relation_types,omitempty"`
	NodeKinds     []string    `json:"node_kinds,omitempty"`
	Where         []Filter    `json:"where,omitempty"`
	WhereExpr     *FilterExpr `json:"where_expr,omitempty"`
	EdgeWhere     []Filter    `json:"edge_where,omitempty"`
	EdgeWhereExpr *FilterExpr `json:"edge_where_expr,omitempty"`
}

type AggregateGroup struct {
	Key        map[string]any `json:"key"`
	Aggregates map[string]any `json:"aggregates"`
}

type Stats struct {
	Scanned   int  `json:"scanned"`
	Visited   int  `json:"visited"`
	Returned  int  `json:"returned"`
	Cost      int  `json:"cost"`
	TimedOut  bool `json:"timed_out,omitempty"`
	Truncated bool `json:"truncated,omitempty"`
}

type Plan struct {
	Op            string     `json:"op"`
	Strategy      string     `json:"strategy"`
	Index         string     `json:"index,omitempty"`
	IndexField    string     `json:"index_field,omitempty"`
	IndexOp       string     `json:"index_op,omitempty"`
	IndexValues   []any      `json:"index_values,omitempty"`
	StatsSource   string     `json:"stats_source,omitempty"`
	EstimatedRows int        `json:"estimated_rows,omitempty"`
	Steps         []PlanStep `json:"steps"`
	EstimatedCost int        `json:"estimated_cost"`
	Warnings      []string   `json:"warnings,omitempty"`
}

type PlanStep struct {
	Name   string `json:"name"`
	Detail string `json:"detail,omitempty"`
	Cost   int    `json:"cost,omitempty"`
}

type ExecuteOptions struct {
	PlannerStats PlannerStats
	IndexLookup  IndexLookup
	EntityLookup EntityLookup
}

type IndexLookup interface {
	MatchFieldIndex(ctx context.Context, kind string, field string, values []any) ([]string, bool, error)
	OutEdges(ctx context.Context, from string, allowedRelationTypes map[string]struct{}) ([]graph.Edge, bool, error)
}

type FieldIndexScanLookup interface {
	ScanFieldIndex(ctx context.Context, kind string, field string) (map[string][]string, bool, error)
}

type FieldIndexFilterScanLookup interface {
	ScanFieldIndexWithFilters(ctx context.Context, kind string, field string, filters []Filter) (map[string][]string, bool, error)
}

type EntityLookup interface {
	GetEntity(ctx context.Context, id string, fields []string) (graph.Entity, bool, error)
}

type EntityBatchLookup interface {
	GetEntities(ctx context.Context, ids []string, fields []string) (map[string]graph.Entity, bool, error)
}

type EntityPageLookup interface {
	ListEntities(ctx context.Context, kind string, fields []string) ([]graph.Entity, bool, error)
}

type PlannerStats struct {
	Version     int64                   `json:"version,omitempty"`
	Indexes     []PlannerIndexStat      `json:"indexes,omitempty"`
	EdgeShards  []PlannerEdgeStat       `json:"edge_shards,omitempty"`
	EntityPages []PlannerEntityPageStat `json:"entity_pages,omitempty"`
}

type PlannerIndexStat struct {
	Kind           string             `json:"kind"`
	Field          string             `json:"field"`
	Status         string             `json:"status,omitempty"`
	EntryCount     int                `json:"entry_count,omitempty"`
	DistinctValues int                `json:"distinct_values,omitempty"`
	TopValues      []PlannerValueStat `json:"top_values,omitempty"`
}

type PlannerValueStat struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type PlannerEdgeStat struct {
	RelationType string `json:"relation_type"`
	Shard        string `json:"shard"`
	EdgeCount    int    `json:"edge_count"`
}

type PlannerEntityPageStat struct {
	Shard       string `json:"shard"`
	EntityCount int    `json:"entity_count"`
}

type OperatorStat struct {
	Name       string  `json:"name"`
	Detail     string  `json:"detail,omitempty"`
	Rows       int     `json:"rows,omitempty"`
	Cost       int     `json:"cost,omitempty"`
	DurationMS float64 `json:"duration_ms"`
}
