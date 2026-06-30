package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDoWithTimeoutOverridesDefaultRequestTimeout(t *testing.T) {
	client := &apiClient{
		baseURL: "http://graphdb-soak-test",
		tenant:  "tenant-a",
		timeout: 5 * time.Millisecond,
		http: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			select {
			case <-time.After(25 * time.Millisecond):
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Request:    req,
			}, nil
		})},
	}

	if _, err := client.do(context.Background(), nil, "short", http.MethodGet, "/", nil, http.StatusOK); err == nil {
		t.Fatal("expected default request timeout to fail")
	}
	if _, err := client.doWithTimeout(context.Background(), nil, 100*time.Millisecond, "long", http.MethodGet, "/", nil, http.StatusOK); err != nil {
		t.Fatalf("expected extended request timeout to succeed: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
