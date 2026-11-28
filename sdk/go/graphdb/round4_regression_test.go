package graphdb

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRound4GoSDKPlatformCallStripsDefaultTenantHeaderAndStreamError(t *testing.T) {
	var seenTenantHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenTenantHeader = r.Header.Get("X-Tenant-ID")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tenants": []any{}})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithHeader("X-Tenant-ID", "leaked-tenant"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTenants(context.Background(), false); err != nil {
		t.Fatalf("ListTenants failed: %v", err)
	}
	if seenTenantHeader != "" {
		t.Fatalf("platform call leaked X-Tenant-ID header: %q", seenTenantHeader)
	}

	stream := newStream(io.NopCloser(strings.NewReader("{\"id\":\"host:1\"}\n{\"done\":true,\"error\":\"timed out\",\"code\":\"request_timeout\"}\n")))
	defer stream.Close()
	var items []map[string]any
	for {
		var item map[string]any
		if !stream.Next(&item) {
			break
		}
		items = append(items, item)
	}
	if len(items) != 1 || items[0]["id"] != "host:1" {
		t.Fatalf("stream yielded terminal error frame as data item: %+v", items)
	}
	if stream.Err() == nil {
		t.Fatal("expected stream.Err() on terminal error frame")
	}
}
