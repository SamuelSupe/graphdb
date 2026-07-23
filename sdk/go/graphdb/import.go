package graphdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type ImportOptions struct {
	Format      string
	Source      string
	CollectorID string
	BatchSize   int
	OnError     string
}

func (c *Client) StartImport(ctx context.Context, source io.Reader, options ImportOptions) (out Task, err error) {
	format := strings.ToLower(strings.TrimSpace(options.Format))
	contentType := ""
	switch format {
	case "jsonl", "ndjson":
		format, contentType = "jsonl", "application/x-ndjson"
	case "csv":
		contentType = "text/csv"
	default:
		return Task{}, fmt.Errorf("import format must be jsonl or csv")
	}
	values := url.Values{}
	values.Set("format", format)
	setQueryValue(values, "source", options.Source)
	setQueryValue(values, "collector_id", options.CollectorID)
	setQueryValue(values, "on_error", options.OnError)
	setInt(values, "batch_size", options.BatchSize)
	resp, err := c.do(ctx, http.MethodPost, "/v1/imports", "", values, source, contentType)
	if err != nil {
		return Task{}, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}

func setQueryValue(values url.Values, key string, value string) {
	if value = strings.TrimSpace(value); value != "" {
		values.Set(key, value)
	}
}
