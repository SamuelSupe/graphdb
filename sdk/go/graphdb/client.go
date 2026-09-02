package graphdb

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const (
	SDKVersion       = "1.3.1"
	defaultUserAgent = "graphdb-go-sdk/" + SDKVersion
)

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	tenantID   string
	headers    http.Header
	userAgent  string
}

type Option func(*Client)

func NewClient(baseURL string, options ...Option) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("base URL must include scheme and host")
	}
	client := &Client{
		baseURL:    parsed,
		httpClient: http.DefaultClient,
		headers:    http.Header{},
		userAgent:  defaultUserAgent,
	}
	for _, option := range options {
		option(client)
	}
	return client, nil
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(client *Client) {
		if httpClient != nil {
			client.httpClient = httpClient
		}
	}
}

func WithTenant(tenantID string) Option {
	return func(client *Client) {
		client.tenantID = strings.TrimSpace(tenantID)
	}
}

func WithBearerToken(token string) Option {
	return func(client *Client) {
		if token = strings.TrimSpace(token); token != "" {
			client.headers.Set("Authorization", "Bearer "+token)
		}
	}
}

func WithHeader(key string, values ...string) Option {
	return func(client *Client) {
		for _, value := range values {
			client.headers.Add(key, value)
		}
	}
}

func WithUserAgent(userAgent string) Option {
	return func(client *Client) {
		if strings.TrimSpace(userAgent) != "" {
			client.userAgent = strings.TrimSpace(userAgent)
		}
	}
}

func (c *Client) ForTenant(tenantID string) *Client {
	clone := *c
	clone.tenantID = strings.TrimSpace(tenantID)
	clone.headers = c.headers.Clone()
	return &clone
}

func (c *Client) TenantID() string {
	return c.tenantID
}

func (c *Client) Health(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, http.MethodGet, "/v1/health", "", nil, nil, &out)
	return out, err
}
