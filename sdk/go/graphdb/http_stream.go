package graphdb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strings"
)

func (c *Client) streamJSON(ctx context.Context, method string, path string, tenantID string, query url.Values, body any) (*Stream, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, method, path, tenantID, query, bytes.NewReader(data), "application/json")
	if err != nil {
		return nil, err
	}
	return newStream(resp.Body), nil
}

func (c *Client) streamText(ctx context.Context, method string, path string, tenantID string, query url.Values, text string) (*Stream, error) {
	resp, err := c.do(ctx, method, path, tenantID, query, strings.NewReader(text), "text/plain")
	if err != nil {
		return nil, err
	}
	return newStream(resp.Body), nil
}
