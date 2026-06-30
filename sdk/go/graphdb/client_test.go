package graphdb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCommitSetsTenantHeaderAndParsesResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/commits" || r.Method != http.MethodPost {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-Tenant-ID"); got != "tenant-a" {
			t.Fatalf("tenant header = %q", got)
		}
		var request CommitRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.IdempotencyKey != "idem-1" || len(request.Mutations.UpsertEntities) != 1 {
			t.Fatalf("commit request = %#v", request)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": "tenant-a", "version": 3, "readable_version": 3})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Commit(context.Background(), Mutations{
		UpsertEntities: []Entity{{ID: "host:1", Kind: "host"}},
	}, &CommitOptions{IdempotencyKey: "idem-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 3 || result.ReadableVersion != 3 {
		t.Fatalf("result = %#v", result)
	}
}

func TestAPIErrorParsesEnvelopeAndRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":"write_backpressure","message":"slow down","retryable":true}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(context.Background(), QueryRequest{Op: "match", Kind: "host"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %T %v, want APIError", err, err)
	}
	if apiErr.Code != "write_backpressure" || !apiErr.Retryable || apiErr.RetryAfter != 2*time.Second {
		t.Fatalf("api error = %#v", apiErr)
	}
}

func TestGQLUsesTextPlain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "text/plain" {
			t.Fatalf("content type = %q", got)
		}
		var body struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			t.Fatalf("body was JSON: %#v", body)
		}
		_ = json.NewEncoder(w).Encode(QueryResponse{Version: 9})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.GQL(context.Background(), `FIND host LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 9 {
		t.Fatalf("result = %#v", result)
	}
}

func TestStreamDecodesNDJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"stream\":true}\n{\"done\":true}\n"))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.GQLStream(context.Background(), `FIND host LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	count := 0
	for {
		var item map[string]any
		if !stream.Next(&item) {
			break
		}
		count++
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("stream item count = %d", count)
	}
}
