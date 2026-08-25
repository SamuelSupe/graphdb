package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type apiClient struct {
	baseURL             string
	tenant              string
	timeout             time.Duration
	readerMaxStaleness  time.Duration
	allowBackpressure   bool
	failOnUnreadyReader bool
	http                *http.Client
}

type apiResponse struct {
	status  int
	body    []byte
	headers http.Header
	json    map[string]any
}

func newClient(baseURL string, tenant string, cfg config) *apiClient {
	timeout := cfg.httpTimeout
	return &apiClient{
		baseURL:             strings.TrimRight(baseURL, "/"),
		tenant:              tenant,
		timeout:             timeout,
		readerMaxStaleness:  cfg.readerMaxStaleness,
		allowBackpressure:   cfg.allowBackpressure,
		failOnUnreadyReader: cfg.failOnUnreadyReader,
		http:                &http.Client{},
	}
}

func (c *apiClient) do(ctx context.Context, metrics *registry, name string, method string, path string, body any, want ...int) (apiResponse, error) {
	return c.doWithTimeout(ctx, metrics, c.timeout, name, method, path, body, want...)
}

func (c *apiClient) doWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration, name string, method string, path string, body any, want ...int) (apiResponse, error) {
	start := time.Now()
	resp, err := c.request(ctx, method, path, body, timeout)
	if err == nil && !statusAllowed(resp.status, want) {
		err = fmt.Errorf("%s returned %d, want %v: %s", path, resp.status, want, string(resp.body))
	}
	if metrics != nil {
		metrics.add(name, time.Since(start), resp.status, metricErr(ctx, err))
	}
	return resp, err
}

func (c *apiClient) doDiscardWithTimeout(ctx context.Context, metrics *registry, timeout time.Duration, name string, method string, path string, body any, want ...int) error {
	start := time.Now()
	status, err := c.requestDiscard(ctx, method, path, body, timeout, want...)
	if metrics != nil {
		metrics.add(name, time.Since(start), status, metricErr(ctx, err))
	}
	return err
}

func (c *apiClient) request(ctx context.Context, method string, path string, body any, timeout time.Duration) (apiResponse, error) {
	if timeout <= 0 {
		timeout = c.timeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return apiResponse{}, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, c.baseURL+path, reader)
	if err != nil {
		return apiResponse{}, err
	}
	req.Header.Set("X-Tenant-ID", c.tenant)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if method == http.MethodPost && path == "/v1/ingest/batches" {
		req.Header.Set("Prefer", "wait=committed")
	}
	httpResp, err := c.http.Do(req)
	if err != nil {
		return apiResponse{}, err
	}
	defer httpResp.Body.Close()
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return apiResponse{status: httpResp.StatusCode, headers: httpResp.Header.Clone()}, err
	}
	resp := apiResponse{status: httpResp.StatusCode, body: data, headers: httpResp.Header.Clone()}
	_ = json.Unmarshal(data, &resp.json)
	return resp, nil
}

func (c *apiClient) requestDiscard(ctx context.Context, method string, path string, body any, timeout time.Duration, want ...int) (int, error) {
	if timeout <= 0 {
		timeout = c.timeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-Tenant-ID", c.tenant)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	httpResp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer httpResp.Body.Close()
	if !statusAllowed(httpResp.StatusCode, want) {
		data, _ := io.ReadAll(io.LimitReader(httpResp.Body, 4096))
		return httpResp.StatusCode, fmt.Errorf("%s returned %d, want %v: %s", path, httpResp.StatusCode, want, string(data))
	}
	if _, err := io.Copy(io.Discard, httpResp.Body); err != nil {
		return httpResp.StatusCode, err
	}
	return httpResp.StatusCode, nil
}

func statusAllowed(status int, want []int) bool {
	for _, value := range want {
		if status == value {
			return true
		}
	}
	return false
}

func metricErr(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return nil
	}
	return err
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

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}
