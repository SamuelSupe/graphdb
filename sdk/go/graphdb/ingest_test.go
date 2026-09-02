package graphdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmitIngestAcceptsWALAndWaitsForOwnerStatus(t *testing.T) {
	const statusPath = "/v1/ingest/writers/writer-a/aws/collector-a/batch-1"
	var statusRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Tenant-ID"); got != "tenant-a" {
			t.Errorf("tenant header = %q, want tenant-a", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/ingest/batches":
			if got := r.Header.Get("Prefer"); got != "" {
				t.Errorf("SubmitIngest Prefer = %q, want empty", got)
			}
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode ingest request: %v", err)
				return
			}
			if got, ok := payload["expected_version"].(float64); !ok || int64(got) != 7 {
				t.Errorf("expected_version = %#v, want 7", payload["expected_version"])
			}
			if got := payload["failure_mode"]; got != "atomic" {
				t.Errorf("failure_mode = %#v, want atomic", got)
			}
			preconditions, ok := payload["preconditions"].([]any)
			if !ok || len(preconditions) != 1 {
				t.Errorf("preconditions = %#v, want one item", payload["preconditions"])
			} else if condition, ok := preconditions[0].(map[string]any); !ok ||
				condition["resource_type"] != "entity" || condition["id"] != "host:1" ||
				condition["field"] != "state" || condition["op"] != "eq" || condition["value"] != "ready" {
				t.Errorf("precondition = %#v", preconditions[0])
			}
			w.Header().Set("Location", statusPath)
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"writer_id":"writer-a","batch_id":"batch-1","state":"accepted","durability":"durable","accepted_at":"2026-09-02T10:00:00Z","estimated_flush_at":"2026-09-02T10:00:01Z","status_url":"`+statusPath+`"}`)
		case r.Method == http.MethodGet && r.URL.Path == statusPath:
			call := statusRequests.Add(1)
			if call == 1 {
				_, _ = io.WriteString(w, `{"tenant_id":"tenant-a","writer_id":"writer-a","source":"aws","collector_id":"collector-a","batch_id":"batch-1","state":"accepted","durability":"durable","recovery_pending":true}`)
				return
			}
			_, _ = io.WriteString(w, `{"tenant_id":"tenant-a","writer_id":"writer-a","source":"aws","collector_id":"collector-a","batch_id":"batch-1","state":"committed","durability":"durable","result":{"batch_id":"batch-1","version":12,"applied":1,"failed":0}}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	expectedVersion := int64(7)
	submission, err := client.SubmitIngest(context.Background(), IngestRequest{
		Source: "aws", CollectorID: "collector-a", BatchID: "batch-1", Items: []IngestItem{{
			ExternalID: "i-1", Entity: &Entity{ID: "host:1", Kind: "host"},
		}}, ExpectedVersion: &expectedVersion, FailureMode: "atomic",
		Preconditions: []IngestPrecondition{{ResourceType: "entity", ID: "host:1", Field: "state", Op: "eq", Value: "ready"}},
	})
	if err != nil {
		t.Fatalf("SubmitIngest: %v", err)
	}
	if submission.StatusCode != http.StatusAccepted || submission.Result != nil || submission.Accepted == nil {
		t.Fatalf("submission = %#v, want accepted 202", submission)
	}
	if submission.Accepted.WriterID != "writer-a" || submission.Accepted.StatusURL != statusPath || submission.Accepted.State != "accepted" {
		t.Fatalf("acceptance = %#v", submission.Accepted)
	}
	if submission.Accepted.AcceptedAt.IsZero() || submission.Accepted.EstimatedFlushAt.IsZero() {
		t.Fatalf("acceptance timestamps were not decoded: %#v", submission.Accepted)
	}

	status, err := client.GetIngestStatus(context.Background(), submission.Accepted.StatusURL)
	if err != nil {
		t.Fatalf("GetIngestStatus: %v", err)
	}
	if status.State != "accepted" || !status.RecoveryPending {
		t.Fatalf("active status = %#v", status)
	}
	terminal, err := client.WaitIngest(context.Background(), submission.Accepted.StatusURL, &IngestWaitOptions{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("WaitIngest: %v", err)
	}
	if terminal.State != "committed" || terminal.Result == nil || terminal.Result.Version != 12 || terminal.Result.Applied != 1 {
		t.Fatalf("terminal status = %#v", terminal)
	}
	if got := statusRequests.Load(); got != 2 {
		t.Fatalf("status requests = %d, want GetIngestStatus plus one WaitIngest poll", got)
	}
}

func TestIngestBlocksOnWALAndPreservesDirectPartialResults(t *testing.T) {
	const statusPath = "/v1/ingest/writers/writer-a/agent/collector-a/wal-batch"
	var postRequests atomic.Int32
	var statusRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/ingest/batches" {
			call := postRequests.Add(1)
			if got := r.Header.Get("Prefer"); got != "wait=committed" {
				t.Errorf("Ingest Prefer = %q, want wait=committed", got)
			}
			switch call {
			case 1:
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, `{"batch_id":"direct-200","version":3,"applied":1,"failed":0}`)
			case 2:
				w.WriteHeader(http.StatusMultiStatus)
				_, _ = io.WriteString(w, `{"batch_id":"direct-207","version":4,"applied":1,"failed":1,"error_code":"precondition_failed","failures":[{"index":1,"error":"state changed"}]}`)
			case 3:
				w.Header().Set("Location", statusPath)
				w.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(w, `{"writer_id":"writer-a","batch_id":"wal-batch","state":"accepted","durability":"durable","status_url":"`+statusPath+`","accepted_at":"2026-09-02T10:00:00Z","estimated_flush_at":"2026-09-02T10:00:01Z"}`)
			default:
				t.Errorf("unexpected POST count %d", call)
			}
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == statusPath {
			statusRequests.Add(1)
			_, _ = io.WriteString(w, `{"tenant_id":"tenant-a","writer_id":"writer-a","source":"agent","collector_id":"collector-a","batch_id":"wal-batch","state":"committed","durability":"durable","result":{"batch_id":"wal-batch","version":5,"applied":1,"failed":0}}`)
			return
		}
		http.Error(w, "unexpected request", http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	request := IngestRequest{Source: "agent", CollectorID: "collector-a", Items: []IngestItem{{ExternalID: "host-1", Entity: &Entity{ID: "host:1", Kind: "host"}}}}
	result, err := client.Ingest(context.Background(), request)
	if err != nil || result.Version != 3 || result.Failed != 0 {
		t.Fatalf("direct 200 result = %#v err=%v", result, err)
	}

	result, err = client.Ingest(context.Background(), request)
	if err != nil || result.Version != 4 || result.Failed != 1 || result.ErrorCode != "precondition_failed" {
		t.Fatalf("direct 207 result = %#v err=%v", result, err)
	}

	result, err = client.Ingest(context.Background(), request)
	if err != nil || result.Version != 5 || result.Failed != 0 {
		t.Fatalf("WAL result = %#v err=%v", result, err)
	}
	if postRequests.Load() != 3 || statusRequests.Load() != 1 {
		t.Fatalf("POST/GET counts = %d/%d, want 3/1", postRequests.Load(), statusRequests.Load())
	}
}

func TestIngestReturnsStructuredAPIErrorForTerminalHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Prefer"); got != "wait=committed" {
			t.Errorf("Prefer = %q, want wait=committed", got)
		}
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = io.WriteString(w, `{"error":"state changed","code":"precondition_failed","message":"state changed","retryable":false}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Ingest(context.Background(), IngestRequest{Source: "agent", CollectorID: "collector-a"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %T %v, want APIError", err, err)
	}
	if apiErr.StatusCode != http.StatusPreconditionFailed || apiErr.Code != "precondition_failed" || apiErr.Message != "state changed" {
		t.Fatalf("api error = %#v", apiErr)
	}
}

func TestIngestStatusRejectsUntrustedURLs(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1", WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	for _, statusURL := range []string{
		"https://other.example/v1/ingest/batches/agent/collector/batch",
		"/v1/query/graphql",
		"/v1/ingest/batches/agent/collector/batch?tenant=other",
	} {
		if _, err := client.GetIngestStatus(context.Background(), statusURL); err == nil || !strings.Contains(err.Error(), "invalid ingest status URL") {
			t.Errorf("GetIngestStatus(%q) error = %v, want URL validation error", statusURL, err)
		}
	}
}
