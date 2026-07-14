package graphdb

import (
	"context"
	"net/url"
	"strconv"
)

type ReadOptions struct {
	MinVersion int64
	AllowStale bool
}

func (o ReadOptions) values() url.Values {
	values := url.Values{}
	setInt64(values, "min_version", o.MinVersion)
	setBool(values, "allow_stale", o.AllowStale)
	return values
}

func (c *Client) GetEntity(ctx context.Context, id string, options *ReadOptions) (out Entity, err error) {
	values := url.Values{}
	if options != nil {
		values = options.values()
	}
	var response struct {
		Entity Entity `json:"entity"`
	}
	err = c.doJSON(ctx, "GET", "/v1/entities/"+pathEscape(id), "", values, nil, &response)
	return response.Entity, err
}

type EntityListOptions struct {
	Kind       string
	Source     string
	Shard      string
	Limit      int
	Cursor     string
	MinVersion int64
	AllowStale bool
}

type EntityListResult struct {
	Version    int64    `json:"version"`
	Entities   []Entity `json:"entities"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

func (c *Client) ListEntities(ctx context.Context, options EntityListOptions) (out EntityListResult, err error) {
	values := queryValues("kind", options.Kind, "source", options.Source, "shard", options.Shard, "cursor", options.Cursor)
	setInt(values, "limit", options.Limit)
	setInt64(values, "min_version", options.MinVersion)
	setBool(values, "allow_stale", options.AllowStale)
	err = c.doJSON(ctx, "GET", "/v1/entities", "", values, nil, &out)
	return out, err
}

func (c *Client) StreamEntities(ctx context.Context, options EntityListOptions) (*Stream, error) {
	values := queryValues("kind", options.Kind, "source", options.Source, "shard", options.Shard, "cursor", options.Cursor)
	setInt(values, "limit", options.Limit)
	setInt64(values, "min_version", options.MinVersion)
	setBool(values, "allow_stale", options.AllowStale)
	return c.streamGet(ctx, "/v1/entities/stream", values)
}

type EdgeListOptions struct {
	Type       string
	From       string
	FromShard  string
	Source     string
	Limit      int
	Cursor     string
	MinVersion int64
	AllowStale bool
}

type EdgeListResult struct {
	Version    int64  `json:"version"`
	Edges      []Edge `json:"edges"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func (c *Client) ListEdges(ctx context.Context, options EdgeListOptions) (out EdgeListResult, err error) {
	values := queryValues("type", options.Type, "from", options.From, "from_shard", options.FromShard, "source", options.Source, "cursor", options.Cursor)
	setInt(values, "limit", options.Limit)
	setInt64(values, "min_version", options.MinVersion)
	setBool(values, "allow_stale", options.AllowStale)
	err = c.doJSON(ctx, "GET", "/v1/edges", "", values, nil, &out)
	return out, err
}

func (c *Client) StreamEdges(ctx context.Context, options EdgeListOptions) (*Stream, error) {
	values := queryValues("type", options.Type, "from", options.From, "from_shard", options.FromShard, "source", options.Source, "cursor", options.Cursor)
	setInt(values, "limit", options.Limit)
	setInt64(values, "min_version", options.MinVersion)
	setBool(values, "allow_stale", options.AllowStale)
	return c.streamGet(ctx, "/v1/edges/stream", values)
}

func (c *Client) ExportSnapshot(ctx context.Context, options ReadOptions) (out map[string]any, err error) {
	err = c.doJSON(ctx, "GET", "/v1/export/snapshot", "", options.values(), nil, &out)
	return out, err
}

func (c *Client) StreamSnapshot(ctx context.Context, options ReadOptions, inline bool) (*Stream, error) {
	values := options.values()
	if inline {
		values.Set("inline", strconv.FormatBool(inline))
	}
	return c.streamGet(ctx, "/v1/export/snapshot/stream", values)
}

func (c *Client) ListCITypes(ctx context.Context, options *ReadOptions) (out map[string]any, err error) {
	values := url.Values{}
	if options != nil {
		values = options.values()
	}
	err = c.doJSON(ctx, "GET", "/v1/ci-types", "", values, nil, &out)
	return out, err
}

func (c *Client) ListRelationTypes(ctx context.Context, options *ReadOptions) (out map[string]any, err error) {
	values := url.Values{}
	if options != nil {
		values = options.values()
	}
	err = c.doJSON(ctx, "GET", "/v1/relation-types", "", values, nil, &out)
	return out, err
}

func (c *Client) streamGet(ctx context.Context, path string, values url.Values) (*Stream, error) {
	resp, err := c.do(ctx, "GET", path, "", values, nil, "")
	if err != nil {
		return nil, err
	}
	return newStream(resp.Body), nil
}
