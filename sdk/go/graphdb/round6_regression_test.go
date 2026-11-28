package graphdb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRound6WithHTTPClientPreservesNoRedirectDefault(t *testing.T) {
	var secretHits atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secretHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer redirectTarget.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	defer server.Close()

	callerClient := &http.Client{Timeout: 5 * time.Second}
	client, err := NewClient(server.URL, WithBearerToken("secret-token"), WithHTTPClient(callerClient))
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if callerClient.CheckRedirect != nil {
		t.Fatalf("WithHTTPClient must not mutate caller's http.Client")
	}
	_, err = client.Health(context.Background())
	if err == nil {
		t.Fatalf("expected 302 redirect not to be followed and return an error")
	}
	if secretHits.Load() != 0 {
		t.Fatalf("redirect target should not have been contacted, got %d hits", secretHits.Load())
	}
}

func TestRound6HTTPResponsesDrainTrailingBytesForKeepAlive(t *testing.T) {
	var remoteAddrs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteAddrs = append(remoteAddrs, r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Trailing newline/spaces after JSON object would leave unread bytes if not drained.
		_, _ = w.Write([]byte("{\"status\":\"ok\"}\n   \n"))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	var out map[string]any
	if err := client.doText(context.Background(), http.MethodPost, "/v1/query/gql", "", nil, "MATCH host", &out); err != nil {
		t.Fatalf("doText failed: %v", err)
	}
	if _, err := client.StartImport(context.Background(), strings.NewReader("id,kind\nh1,host\n"), ImportOptions{Format: "csv"}); err != nil {
		t.Fatalf("StartImport failed: %v", err)
	}
	if _, err := client.Health(context.Background()); err != nil {
		t.Fatalf("Health failed: %v", err)
	}
	if len(remoteAddrs) != 3 {
		t.Fatalf("expected 3 requests, got %d", len(remoteAddrs))
	}
	if remoteAddrs[0] != remoteAddrs[1] || remoteAddrs[1] != remoteAddrs[2] {
		t.Fatalf("expected keep-alive connection reuse across requests, got remote addrs %v", remoteAddrs)
	}
}
