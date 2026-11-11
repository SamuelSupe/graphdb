package graphdb

import (
	"bytes"
	"context"
	"encoding/json"
)

// CypherRequest executes the 3.0 development read-only openCypher profile.
type CypherRequest struct {
	Query      string         `json:"query"`
	Parameters map[string]any `json:"parameters,omitempty"`
	MinVersion int64          `json:"min_version,omitempty"`
	AllowStale bool           `json:"allow_stale,omitempty"`
	TimeoutMS  int            `json:"timeout_ms,omitempty"`
	CostLimit  int            `json:"cost_limit,omitempty"`
	Profile    bool           `json:"profile,omitempty"`
}
type CypherResponse struct {
	Version int64            `json:"version"`
	Columns []string         `json:"columns"`
	Rows    [][]any          `json:"rows"`
	Stats   QueryStats       `json:"stats"`
	Plan    map[string]any   `json:"plan,omitempty"`
	Profile []map[string]any `json:"profile,omitempty"`
}

// UnmarshalJSON retains exact JSON numbers in dynamically typed result cells.
func (r *CypherResponse) UnmarshalJSON(data []byte) error {
	type plain CypherResponse
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	return d.Decode((*plain)(r))
}
func (c *Client) Cypher(ctx context.Context, request CypherRequest) (out CypherResponse, err error) {
	err = c.doJSON(ctx, "POST", "/v1/query/cypher", "", nil, request, &out)
	return
}

// CypherStream reports failed terminal frames through Stream.Err and requires
// a successful terminal frame, including for queries that return no rows.
// Each successful Next replaces its destination with the current frame.
func (c *Client) CypherStream(ctx context.Context, request CypherRequest) (*Stream, error) {
	s, err := c.streamJSON(ctx, "POST", "/v1/query/cypher/stream", "", nil, request)
	if err == nil {
		s.requireDone = true
		s.requireSuccess = true
	}
	return s, err
}
