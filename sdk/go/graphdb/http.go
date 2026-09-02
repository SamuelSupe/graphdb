package graphdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func (c *Client) doJSON(ctx context.Context, method string, path string, tenantID string, query url.Values, body any, out any) error {
	_, _, err := c.doJSONWithHeaders(ctx, method, path, tenantID, query, body, nil, out)
	return err
}

func (c *Client) doJSONWithHeaders(
	ctx context.Context,
	method string,
	path string,
	tenantID string,
	query url.Values,
	body any,
	requestHeaders http.Header,
	out any,
) (int, http.Header, error) {
	var reader io.Reader
	contentType := ""
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(data)
		contentType = "application/json"
	}
	resp, err := c.doWithHeaders(ctx, method, path, tenantID, query, reader, contentType, requestHeaders)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, resp.Header.Clone(), nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return resp.StatusCode, resp.Header.Clone(), err
	}
	return resp.StatusCode, resp.Header.Clone(), nil
}

func (c *Client) doText(ctx context.Context, method string, path string, tenantID string, query url.Values, text string, out any) error {
	resp, err := c.do(ctx, method, path, tenantID, query, strings.NewReader(text), "text/plain")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) do(ctx context.Context, method string, path string, tenantID string, query url.Values, body io.Reader, contentType string) (*http.Response, error) {
	return c.doWithHeaders(ctx, method, path, tenantID, query, body, contentType, nil)
}

func (c *Client) doWithHeaders(
	ctx context.Context,
	method string,
	path string,
	tenantID string,
	query url.Values,
	body io.Reader,
	contentType string,
	requestHeaders http.Header,
) (*http.Response, error) {
	if c == nil {
		return nil, errors.New("nil GGraphDB client")
	}
	requestURL, err := c.url(path, query)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, err
	}
	c.setHeaders(req, tenantID, contentType)
	for key, values := range requestHeaders {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, parseAPIError(resp)
	}
	return resp, nil
}

func (c *Client) setHeaders(req *http.Request, tenantID string, contentType string) {
	for key, values := range c.headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	if tenantID == "" {
		tenantID = c.tenantID
	}
	if tenantID != "" {
		req.Header.Set("X-Tenant-ID", tenantID)
	}
}

func (c *Client) url(path string, query url.Values) (string, error) {
	next := *c.baseURL
	basePath := strings.TrimRight(next.EscapedPath(), "/")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	escapedPath := basePath + path
	decodedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return "", err
	}
	next.Path = decodedPath
	next.RawPath = escapedPath
	next.RawQuery = query.Encode()
	return next.String(), nil
}
