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

	"gitlab.jiagouyun.com/guance/graphdb/internal/httpapi"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type apiClient struct {
	baseURL                string
	tenant                 string
	allowWriteBackpressure bool
	http                   *http.Client
}

type apiResponse struct {
	status  int
	body    []byte
	headers http.Header
	json    map[string]any
}

type ingestOutcome struct {
	version       int64
	applied       int64
	backpressured bool
}

func newClient(baseURL string, tenant string, timeout time.Duration) *apiClient {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1024
	transport.MaxIdleConnsPerHost = 512
	return &apiClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		tenant:  tenant,
		http:    &http.Client{Timeout: timeout, Transport: transport},
	}
}

func (c *apiClient) health(ctx context.Context, metrics *registry) error {
	_, err := c.do(ctx, metrics, "health", http.MethodGet, "/v1/health", nil, http.StatusOK)
	return err
}

func (c *apiClient) commitSchema(ctx context.Context, metrics *registry) (int64, error) {
	body := httpapi.CommitRequest{Mutations: schemaMutations()}
	resp, err := c.do(ctx, metrics, "commit-schema", http.MethodPost, "/v1/commits", body, http.StatusOK)
	return responseVersion(resp), err
}

func (c *apiClient) ingest(ctx context.Context, metrics *registry, body storage.IngestRequest) (ingestOutcome, error) {
	want := []int{http.StatusOK, http.StatusMultiStatus, http.StatusAccepted}
	if c.allowWriteBackpressure {
		want = append(want, http.StatusTooManyRequests)
	}
	started := time.Now()
	resp, err := c.do(ctx, metrics, "ingest", http.MethodPost, "/v1/ingest/batches", body, want...)
	if err != nil {
		return ingestOutcome{}, err
	}
	if resp.status == http.StatusTooManyRequests {
		return ingestOutcome{backpressured: true}, nil
	}
	if resp.status != http.StatusAccepted {
		return ingestOutcome{version: responseVersion(resp), applied: responseApplied(resp)}, nil
	}
	statusPath := stringValue(resp.json["status_url"])
	if statusPath == "" {
		statusPath = resp.headers.Get("Location")
	}
	if statusPath == "" {
		return ingestOutcome{}, fmt.Errorf("WAL ingest response omitted status_url and Location: %s", string(resp.body))
	}
	outcome, err := c.waitIngestCommitted(ctx, metrics, statusPath)
	metrics.add("ingest-committed", time.Since(started), http.StatusOK, err)
	return outcome, err
}

func (c *apiClient) waitIngestCommitted(ctx context.Context, metrics *registry, statusPath string) (ingestOutcome, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		resp, err := c.do(ctx, metrics, "ingest-status", http.MethodGet, statusPath, nil, http.StatusOK)
		if err != nil {
			return ingestOutcome{}, err
		}
		switch stringValue(resp.json["state"]) {
		case storage.IngestStateCommitted:
			return ingestOutcome{version: responseVersion(resp), applied: responseApplied(resp)}, nil
		case storage.IngestStateFailed:
			return ingestOutcome{}, fmt.Errorf("WAL ingest failed: %s", string(resp.body))
		}
		select {
		case <-ctx.Done():
			return ingestOutcome{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *apiClient) query(ctx context.Context, metrics *registry, name string, body query.Request) error {
	_, err := c.do(ctx, metrics, name, http.MethodPost, "/v1/query", body, http.StatusOK)
	return err
}

func (c *apiClient) queryStream(ctx context.Context, metrics *registry, body query.Request) error {
	_, err := c.do(ctx, metrics, "query-stream", http.MethodPost, "/v1/query/stream", body, http.StatusOK)
	return err
}

func (c *apiClient) queryMinVersion(ctx context.Context, metrics *registry, name string, body query.Request) error {
	resp, err := c.do(ctx, metrics, name, http.MethodPost, "/v1/query", body, http.StatusOK, http.StatusServiceUnavailable)
	if err != nil {
		return err
	}
	return validateReaderVersionResponse(resp)
}

func (c *apiClient) queryStreamMinVersion(ctx context.Context, metrics *registry, body query.Request) error {
	resp, err := c.do(ctx, metrics, "query-stream-min-version", http.MethodPost, "/v1/query/stream", body, http.StatusOK, http.StatusServiceUnavailable)
	if err != nil {
		return err
	}
	return validateReaderVersionResponse(resp)
}

func (c *apiClient) entityMinVersion(ctx context.Context, metrics *registry, id string, version int64) error {
	path := fmt.Sprintf("/v1/entities/%s?min_version=%d", url.PathEscape(id), version)
	resp, err := c.do(ctx, metrics, "entity-min-version", http.MethodGet, path, nil, http.StatusOK, http.StatusServiceUnavailable)
	if err != nil {
		return err
	}
	return validateReaderVersionResponse(resp)
}

func (c *apiClient) listEntitiesAllowStale(ctx context.Context, metrics *registry) error {
	_, err := c.doWithHeaders(ctx, metrics, "entities-allow-stale", http.MethodGet, "/v1/entities?kind=host&limit=20", nil, map[string]string{
		"X-GraphDB-Allow-Stale": "true",
	}, http.StatusOK)
	return err
}

func (c *apiClient) saveQuery(ctx context.Context, metrics *registry) error {
	body := storage.SavedQuery{Name: "loadtest-hosts", Request: matchQuery("region-0")}
	_, err := c.do(ctx, metrics, "save-query", http.MethodPost, "/v1/query/templates", body, http.StatusOK)
	return err
}

func (c *apiClient) runSavedQuery(ctx context.Context, metrics *registry) error {
	_, err := c.do(ctx, metrics, "run-saved-query", http.MethodPost, "/v1/query/templates/loadtest-hosts/run", nil, http.StatusOK)
	return err
}

func (c *apiClient) rebuildIndexes(ctx context.Context, metrics *registry, timeout time.Duration) error {
	if timeout <= 0 {
		_, err := c.do(ctx, metrics, "rebuild-indexes", http.MethodPost, "/v1/indexes/rebuild", nil, http.StatusOK)
		return err
	}
	child, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := c.do(child, metrics, "rebuild-indexes", http.MethodPost, "/v1/indexes/rebuild", nil, http.StatusOK)
	return err
}

func (c *apiClient) indexHealth(ctx context.Context, metrics *registry) error {
	_, err := c.do(ctx, metrics, "index-health", http.MethodGet, "/v1/indexes/health", nil, http.StatusOK)
	return err
}

func (c *apiClient) collectorStatus(ctx context.Context, metrics *registry) error {
	_, err := c.do(ctx, metrics, "collector-status", http.MethodGet, "/v1/ingest/collectors/loadtest/"+collectorName(0), nil, http.StatusOK)
	return err
}

func (c *apiClient) do(ctx context.Context, metrics *registry, name string, method string, path string, body any, want ...int) (apiResponse, error) {
	return c.doWithHeaders(ctx, metrics, name, method, path, body, nil, want...)
}

func (c *apiClient) doWithHeaders(ctx context.Context, metrics *registry, name string, method string, path string, body any, headers map[string]string, want ...int) (apiResponse, error) {
	start := time.Now()
	resp, err := c.request(ctx, method, path, body, headers)
	if err == nil && !statusAllowed(resp.status, want) {
		err = fmt.Errorf("%s returned %d, want %v: %s", path, resp.status, want, string(resp.body))
	}
	metrics.add(name, time.Since(start), resp.status, err)
	return resp, err
}

func (c *apiClient) request(ctx context.Context, method string, path string, body any, headers map[string]string) (apiResponse, error) {
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
	req.Header.Set("X-Tenant-ID", c.tenant)
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
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return apiResponse{status: resp.StatusCode, headers: resp.Header.Clone()}, err
	}
	out := apiResponse{status: resp.StatusCode, body: data, headers: resp.Header.Clone()}
	_ = json.Unmarshal(data, &out.json)
	return out, nil
}

func statusAllowed(status int, want []int) bool {
	for _, value := range want {
		if status == value {
			return true
		}
	}
	return false
}

func validateReaderVersionResponse(resp apiResponse) error {
	if resp.status != http.StatusServiceUnavailable {
		return nil
	}
	if stringValue(resp.json["code"]) != "reader_not_fresh" {
		return fmt.Errorf("503 code = %q, want reader_not_fresh: %s", stringValue(resp.json["code"]), string(resp.body))
	}
	if !boolValue(resp.json["retryable"]) {
		return fmt.Errorf("reader_not_fresh retryable = false: %s", string(resp.body))
	}
	if resp.headers.Get("Retry-After") == "" {
		return fmt.Errorf("reader_not_fresh missing Retry-After: %s", string(resp.body))
	}
	return nil
}

func responseVersion(resp apiResponse) int64 {
	if result, ok := resp.json["result"].(map[string]any); ok {
		if version := int64Value(result["readable_version"]); version > 0 {
			return version
		}
		if version := int64Value(result["version"]); version > 0 {
			return version
		}
	}
	if version := int64Value(resp.json["readable_version"]); version > 0 {
		return version
	}
	return int64Value(resp.json["version"])
}

func responseApplied(resp apiResponse) int64 {
	if result, ok := resp.json["result"].(map[string]any); ok {
		return int64Value(result["applied"])
	}
	return int64Value(resp.json["applied"])
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}
