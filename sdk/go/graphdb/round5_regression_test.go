package graphdb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRound5GoSDKBlocksRedirectsAndRejects3xx(t *testing.T) {
	sinkHit := false
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sinkHit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL+"/steal", http.StatusFound)
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL, WithHeader("X-API-Key", "secret"), WithTenant("t1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(context.Background(), QueryRequest{Op: "match", Kind: "host"})
	if err == nil {
		t.Fatal("expected error on 302 redirect")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
		t.Fatalf("expected *APIError with status 302, got %T %v", err, err)
	}
	if sinkHit {
		t.Fatal("Go SDK followed cross-host HTTP redirect")
	}
}

func TestRound5GoSDKAsyncIngestFailureReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/ingest/batches":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(IngestAcceptance{
				StatusURL: "/v1/ingest/batches/s/c/b1",
				State:     "accepted",
			})
		case "/v1/ingest/batches/s/c/b1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(IngestBatchStatus{
				State: "failed",
				Result: &IngestResult{
					BatchID:   "b1",
					Failed:    1,
					ErrorCode: "version_conflict",
					Failures:  []IngestFailure{{Error: "expected version 1, got 2"}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client, err := NewClient(srv.URL, WithTenant("t1"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Ingest(context.Background(), IngestRequest{Source: "s", CollectorID: "c", BatchID: "b1"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict || apiErr.Code != "version_conflict" {
		t.Fatalf("expected *APIError(409, version_conflict), got %v", err)
	}
}
