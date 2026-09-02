package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type apiClient struct {
	baseURL string
	tenant  string
	http    *http.Client
}

type apiResponse struct {
	status  int
	body    []byte
	json    map[string]any
	headers http.Header
}

func newClient(baseURL string, tenant string) *apiClient {
	return &apiClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		tenant:  tenant,
		http:    &http.Client{Timeout: 20 * time.Second},
	}
}

func (c *apiClient) do(ctx context.Context, method string, path string, body any, want ...int) (apiResponse, error) {
	return c.doWithHeaders(ctx, method, path, body, nil, want...)
}

func (c *apiClient) doWithHeaders(ctx context.Context, method string, path string, body any, headers map[string]string, want ...int) (apiResponse, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return apiResponse{}, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return apiResponse{}, err
	}
	if c.tenant != "" {
		req.Header.Set("X-Tenant-ID", c.tenant)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return apiResponse{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if !statusAllowed(resp.StatusCode, want) {
		return apiResponse{status: resp.StatusCode, body: data}, fmt.Errorf("%s %s returned %d: %s", method, path, resp.StatusCode, string(data))
	}
	out := apiResponse{status: resp.StatusCode, body: data, headers: resp.Header.Clone()}
	_ = json.Unmarshal(data, &out.json)
	return out, nil
}

func (c *apiClient) ndjson(ctx context.Context, path string, body any) ([]map[string]any, error) {
	resp, err := c.do(ctx, http.MethodPost, path, body, http.StatusOK)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(bytes.TrimSpace(resp.body), []byte("\n"))
	items := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var item map[string]any
		if err := json.Unmarshal(line, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func entityPath(id string) string {
	return "/v1/entities/" + url.PathEscape(id)
}

func statusAllowed(status int, want []int) bool {
	for _, value := range want {
		if status == value {
			return true
		}
	}
	return false
}
