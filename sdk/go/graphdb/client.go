package graphdb

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	SDKVersion       = "2.7.2"
	defaultUserAgent = "graphdb-go-sdk/" + SDKVersion
)

type Client struct {
	baseURL        *url.URL
	httpClient     *http.Client
	ownsHTTPClient bool
	tenantID       string
	headers        http.Header
	userAgent      string
}

type Option func(*Client)

func NewClient(baseURL string, options ...Option) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("base URL must include http or https scheme and host")
	}
	client := &Client{
		baseURL:   parsed,
		headers:   http.Header{},
		userAgent: defaultUserAgent,
	}
	for _, option := range options {
		option(client)
	}
	if client.httpClient == nil {
		var transport *http.Transport
		if base, ok := http.DefaultTransport.(*http.Transport); ok && base != nil {
			transport = base.Clone()
		} else {
			transport = &http.Transport{}
		}
		transport.MaxIdleConns = 128
		transport.MaxIdleConnsPerHost = 64
		transport.IdleConnTimeout = 90 * time.Second
		client.httpClient = &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		client.ownsHTTPClient = true
	}
	return client, nil
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(client *Client) {
		if httpClient != nil {
			copied := *httpClient
			if copied.CheckRedirect == nil {
				copied.CheckRedirect = func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				}
			}
			client.httpClient = &copied
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
	clone.ownsHTTPClient = false
	clone.tenantID = strings.TrimSpace(tenantID)
	clone.headers = c.headers.Clone()
	return &clone
}

func (c *Client) CloseIdleConnections() {
	if c != nil && c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}

func (c *Client) Close() error {
	if c != nil && c.ownsHTTPClient {
		c.CloseIdleConnections()
	}
	return nil
}

func (c *Client) TenantID() string {
	return c.tenantID
}

func (c *Client) Health(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, http.MethodGet, "/v1/health", "", nil, nil, &out)
	return out, err
}
