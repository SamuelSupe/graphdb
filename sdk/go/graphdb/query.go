package graphdb

import "context"

type QueryRequest struct {
	Op                string        `json:"op"`
	TargetOp          string        `json:"target_op,omitempty"`
	Kind              string        `json:"kind,omitempty"`
	Filters           Fields        `json:"filters,omitempty"`
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
	Direction     string      `json:"direction,omitempty"`
	RelationTypes []string    `json:"relation_types,omitempty"`
	NodeKinds     []string    `json:"node_kinds,omitempty"`
	Where         []Filter    `json:"where,omitempty"`
	WhereExpr     *FilterExpr `json:"where_expr,omitempty"`
	EdgeWhere     []Filter    `json:"edge_where,omitempty"`
	EdgeWhereExpr *FilterExpr `json:"edge_where_expr,omitempty"`
}

type QueryResponse struct {
	Version    int64            `json:"version"`
	Results    []QueryResult    `json:"results"`
	NextCursor string           `json:"next_cursor,omitempty"`
	Stats      QueryStats       `json:"stats"`
	Aggregates map[string]any   `json:"aggregates,omitempty"`
	Groups     []AggregateGroup `json:"groups,omitempty"`
	Plan       map[string]any   `json:"plan,omitempty"`
	Profile    []map[string]any `json:"profile,omitempty"`
}

type QueryResult struct {
	Entity    *Entity        `json:"entity,omitempty"`
	Edge      *Edge          `json:"edge,omitempty"`
	Direction string         `json:"direction,omitempty"`
	Path      map[string]any `json:"path,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
	Score     int            `json:"score,omitempty"`
}

type QueryStats struct {
	Scanned   int  `json:"scanned"`
	Visited   int  `json:"visited"`
	Returned  int  `json:"returned"`
	Cost      int  `json:"cost"`
	TimedOut  bool `json:"timed_out,omitempty"`
	Truncated bool `json:"truncated,omitempty"`
}

type AggregateGroup struct {
	Key        map[string]any `json:"key"`
	Aggregates map[string]any `json:"aggregates"`
}

type GraphQLRequest struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName,omitempty"`
	Variables     map[string]any `json:"variables,omitempty"`
}

type GraphQLError struct {
	Message    string           `json:"message"`
	Path       []any            `json:"path,omitempty"`
	Locations  []map[string]int `json:"locations,omitempty"`
	Extensions map[string]any   `json:"extensions,omitempty"`
}

type GraphQLResponse struct {
	Data   map[string]any `json:"data,omitempty"`
	Errors []GraphQLError `json:"errors,omitempty"`
}

func (c *Client) Query(ctx context.Context, request QueryRequest) (out QueryResponse, err error) {
	err = c.doJSON(ctx, "POST", "/v1/query", "", nil, request, &out)
	return out, err
}

func (c *Client) QueryStream(ctx context.Context, request QueryRequest) (*Stream, error) {
	return c.streamJSON(ctx, "POST", "/v1/query/stream", "", nil, request)
}

func (c *Client) GraphQL(ctx context.Context, request GraphQLRequest) (out GraphQLResponse, err error) {
	err = c.doJSON(ctx, "POST", "/v1/query/graphql", "", nil, request, &out)
	return out, err
}

// GQL executes the legacy FIND/MATCH text DSL. It is not GraphQL.
// Deprecated: use Query for the JSON DSL or GraphQL for GraphQL documents.
func (c *Client) GQL(ctx context.Context, text string) (out QueryResponse, err error) {
	err = c.doText(ctx, "POST", "/v1/query/gql", "", nil, text, &out)
	return out, err
}

// GQLStream streams the legacy FIND/MATCH text DSL.
// Deprecated: use QueryStream. GraphQL responses are not NDJSON streams.
func (c *Client) GQLStream(ctx context.Context, text string) (*Stream, error) {
	return c.streamText(ctx, "POST", "/v1/query/gql/stream", "", nil, text)
}

type SavedQuery struct {
	TenantID    string       `json:"tenant_id,omitempty"`
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Request     QueryRequest `json:"request"`
	CreatedAt   string       `json:"created_at,omitempty"`
	UpdatedAt   string       `json:"updated_at,omitempty"`
}

func (c *Client) SaveQuery(ctx context.Context, saved SavedQuery) (out SavedQuery, err error) {
	err = c.doJSON(ctx, "POST", "/v1/query/templates", "", nil, saved, &out)
	return out, err
}

func (c *Client) ListQueries(ctx context.Context) (out map[string][]SavedQuery, err error) {
	err = c.doJSON(ctx, "GET", "/v1/query/templates", "", nil, nil, &out)
	return out, err
}

func (c *Client) RunSavedQuery(ctx context.Context, name string) (out QueryResponse, err error) {
	err = c.doJSON(ctx, "POST", "/v1/query/templates/"+pathEscape(name)+"/run", "", nil, nil, &out)
	return out, err
}

func (c *Client) ListRunningQueries(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, "GET", "/v1/queries/running", "", nil, nil, &out)
	return out, err
}

func (c *Client) KillRunningQuery(ctx context.Context, id string) error {
	return c.doJSON(ctx, "DELETE", "/v1/queries/running/"+pathEscape(id), "", nil, nil, nil)
}
