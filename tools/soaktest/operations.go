package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"graphdb/internal/httpapi"
	"graphdb/internal/query"
	"graphdb/internal/storage"
)

func (c *apiClient) health(ctx context.Context, metrics *registry) error {
	_, err := c.do(ctx, metrics, "health", http.MethodGet, "/v1/health", nil, http.StatusOK)
	return err
}

func (c *apiClient) createTenant(ctx context.Context, metrics *registry) error {
	body := map[string]any{"tenant_id": c.tenant, "name": c.tenant}
	_, err := c.do(ctx, metrics, "create-tenant", http.MethodPost, "/v1/tenants", body, http.StatusOK, http.StatusConflict)
	return err
}

func (c *apiClient) commitSchema(ctx context.Context, metrics *registry) (int64, error) {
	body := httpapi.CommitRequest{Mutations: schemaMutations()}
	resp, err := c.do(ctx, metrics, "commit-schema", http.MethodPost, "/v1/commits", body, http.StatusOK)
	return responseVersion(resp), err
}

func (c *apiClient) ingest(ctx context.Context, metrics *registry, body storage.IngestRequest) (int64, int, error) {
	want := []int{http.StatusOK}
	if c.allowBackpressure {
		want = append(want, http.StatusTooManyRequests)
	}
	resp, err := c.do(ctx, metrics, "ingest", http.MethodPost, "/v1/ingest/batches", body, want...)
	return responseVersion(resp), resp.status, err
}

func (c *apiClient) query(ctx context.Context, metrics *registry, name string, body query.Request) (apiResponse, error) {
	return c.do(ctx, metrics, name, http.MethodPost, "/v1/query", body, http.StatusOK)
}

func (c *apiClient) queryWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration, body query.Request) (apiResponse, error) {
	return c.doWithTimeout(ctx, metrics, timeout, name, http.MethodPost, "/v1/query", body, http.StatusOK)
}

func (c *apiClient) queryMinVersionWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration, body query.Request) error {
	resp, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodPost, "/v1/query", body, http.StatusOK, http.StatusServiceUnavailable)
	if err != nil {
		return err
	}
	return validateReaderVersionResponse(resp)
}

func (c *apiClient) queryStream(ctx context.Context, metrics *registry, body query.Request) error {
	return c.queryStreamNamedWithTimeout(ctx, metrics, "query-stream", c.timeout, body)
}

func (c *apiClient) queryStreamWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration, body query.Request) error {
	return c.queryStreamNamedWithTimeout(ctx, metrics, "query-stream", timeout, body)
}

func (c *apiClient) queryStreamNamedWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration, body query.Request) error {
	resp, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodPost, "/v1/query/stream", body, http.StatusOK)
	if err != nil {
		return err
	}
	if !bytes.Contains(resp.body, []byte(`"done":true`)) {
		return fmt.Errorf("stream query did not finish")
	}
	return nil
}

func (c *apiClient) queryStreamMinVersionWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration, body query.Request) error {
	resp, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodPost, "/v1/query/stream", body, http.StatusOK, http.StatusServiceUnavailable)
	if err != nil {
		return err
	}
	if err := validateReaderVersionResponse(resp); err != nil {
		return err
	}
	if resp.status == http.StatusOK && !bytes.Contains(resp.body, []byte(`"done":true`)) {
		return fmt.Errorf("stream query did not finish")
	}
	return nil
}

func (c *apiClient) saveQuery(ctx context.Context, metrics *registry, name string, request query.Request) error {
	body := storage.SavedQuery{Name: name, Request: request}
	_, err := c.do(ctx, metrics, "save-query", http.MethodPost, "/v1/query/templates", body, http.StatusOK)
	return err
}

func (c *apiClient) runSavedQuery(ctx context.Context, metrics *registry, name string) error {
	return c.runSavedQueryWithTimeout(ctx, metrics, c.timeout, name)
}

func (c *apiClient) runSavedQueryWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration, name string) error {
	return c.runSavedQueryNamedWithTimeout(ctx, metrics, "run-saved-query", timeout, name)
}

func (c *apiClient) runSavedQueryNamedWithTimeout(ctx context.Context, metrics *registry, metricName string, timeout time.Duration, name string) error {
	path := "/v1/query/templates/" + url.PathEscape(name) + "/run"
	_, err := c.doWithTimeout(ctx, metrics, timeout, metricName, http.MethodPost, path, nil, http.StatusOK)
	return err
}

func (c *apiClient) rebuildIndexes(ctx context.Context, metrics *registry) error {
	return c.rebuildIndexesWithTimeout(ctx, metrics, c.timeout)
}

func (c *apiClient) rebuildIndexesWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration) error {
	return c.runTaskWithTimeout(ctx, metrics, "rebuild-indexes", timeout, "index_rebuild", nil)
}

func (c *apiClient) indexCatalog(ctx context.Context, metrics *registry) (apiResponse, error) {
	return c.do(ctx, metrics, "index-catalog", http.MethodGet, "/v1/indexes", nil, http.StatusOK, http.StatusNotFound)
}

func (c *apiClient) indexHealth(ctx context.Context, metrics *registry) (apiResponse, error) {
	return c.do(ctx, metrics, "index-health", http.MethodGet, "/v1/indexes/health", nil, http.StatusOK)
}

func (c *apiClient) compact(ctx context.Context, metrics *registry) error {
	return c.compactWithTimeout(ctx, metrics, c.timeout)
}

func (c *apiClient) compactWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration) error {
	return c.runTaskWithTimeout(ctx, metrics, "compact", timeout, "compact", nil)
}

func (c *apiClient) gc(ctx context.Context, metrics *registry) error {
	return c.gcWithTimeout(ctx, metrics, c.timeout)
}

func (c *apiClient) gcWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration) error {
	body := map[string]any{
		"keep_snapshots":             2,
		"deadletter_max_age_seconds": 3600,
		"task_max_age_seconds":       3600,
		"cleanup_index_orphans":      true,
	}
	return c.runTaskWithTimeout(ctx, metrics, "gc", timeout, "gc", body)
}

func (c *apiClient) tenantUsage(ctx context.Context, metrics *registry) (apiResponse, error) {
	return c.do(ctx, metrics, "tenant-usage", http.MethodGet, "/v1/tenant-usage", nil, http.StatusOK)
}

func (c *apiClient) readerFreshness(ctx context.Context, metrics *registry) (apiResponse, error) {
	return c.do(ctx, metrics, "reader-freshness", http.MethodGet, "/v1/control/reader-freshness", nil, http.StatusOK)
}

func (c *apiClient) fleetReadiness(ctx context.Context, metrics *registry) (apiResponse, error) {
	values := url.Values{}
	values.Set("min_ready", "1")
	values.Set("max_staleness_ms", fmt.Sprintf("%d", c.readerMaxStaleness.Milliseconds()))
	path := "/v1/control/reader-fleet-readiness?" + values.Encode()
	resp, err := c.do(ctx, metrics, "reader-fleet", http.MethodGet, path, nil, http.StatusOK)
	if err != nil {
		return resp, err
	}
	if c.failOnUnreadyReader && !boolValue(resp.json["ready"]) {
		return resp, fmt.Errorf("reader fleet is not ready")
	}
	return resp, nil
}

func (c *apiClient) scanEntities(ctx context.Context, metrics *registry) error {
	return c.scanEntitiesWithTimeout(ctx, metrics, c.timeout)
}

func (c *apiClient) scanEntitiesWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration) error {
	return c.scanEntitiesNamedWithTimeout(ctx, metrics, "scan-entities", timeout)
}

func (c *apiClient) scanEntitiesNamedWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration) error {
	_, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodGet, "/v1/entities?kind=host&limit=100", nil, http.StatusOK)
	return err
}

func (c *apiClient) scanEntitiesMinVersionWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration, version int64) error {
	path := fmt.Sprintf("/v1/entities?kind=host&limit=100&min_version=%d", version)
	resp, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodGet, path, nil, http.StatusOK, http.StatusServiceUnavailable)
	if err != nil {
		return err
	}
	return validateReaderVersionResponse(resp)
}

func (c *apiClient) scanEntitiesAllowStaleWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration) error {
	_, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodGet, "/v1/entities?kind=host&limit=100&allow_stale=true", nil, http.StatusOK)
	return err
}

func (c *apiClient) scanEdges(ctx context.Context, metrics *registry) error {
	return c.scanEdgesWithTimeout(ctx, metrics, c.timeout)
}

func (c *apiClient) scanEdgesWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration) error {
	return c.scanEdgesNamedWithTimeout(ctx, metrics, "scan-edges", timeout)
}

func (c *apiClient) scanEdgesNamedWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration) error {
	_, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodGet, "/v1/edges?type=runs_on&limit=100", nil, http.StatusOK)
	return err
}

func (c *apiClient) scanEdgesAllowStaleWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration) error {
	_, err := c.doWithTimeout(ctx, metrics, timeout, name, http.MethodGet, "/v1/edges?type=runs_on&limit=100&allow_stale=true", nil, http.StatusOK)
	return err
}

func (c *apiClient) exportSnapshotStreamWithTimeout(ctx context.Context, metrics *registry, name string, timeout time.Duration) error {
	return c.doDiscardWithTimeout(ctx, metrics, timeout, name, http.MethodGet, "/v1/export/snapshot/stream", nil, http.StatusOK)
}

func responseVersion(resp apiResponse) int64 {
	if version := int64Value(resp.json["readable_version"]); version > 0 {
		return version
	}
	return int64Value(resp.json["version"])
}
