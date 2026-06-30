package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRunTaskWithTimeoutPollsTaskCompletion(t *testing.T) {
	var calls []string
	client := &apiClient{
		baseURL: "http://graphdb-soak-test",
		tenant:  "tenant-a",
		timeout: time.Second,
		http: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls = append(calls, req.Method+" "+req.URL.Path)
			if req.Method == http.MethodPost && req.URL.Path == "/v1/tasks" {
				data, _ := io.ReadAll(req.Body)
				if !strings.Contains(string(data), `"type":"compact"`) {
					t.Fatalf("task body = %s", string(data))
				}
				return jsonResponse(http.StatusAccepted, `{"id":"task-1","status":"queued"}`), nil
			}
			if req.Method == http.MethodGet && req.URL.Path == "/v1/tasks/task-1" {
				return jsonResponse(http.StatusOK, `{"id":"task-1","status":"succeeded"}`), nil
			}
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}
	metrics := newRegistry()

	if err := client.runTaskWithTimeout(context.Background(), metrics, "compact", time.Second, "compact", nil); err != nil {
		t.Fatalf("run task: %v", err)
	}
	if got, want := strings.Join(calls, ","), "POST /v1/tasks,GET /v1/tasks/task-1"; got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
	snapshots := metrics.snapshots()
	if len(snapshots) != 1 || snapshots[0].name != "compact" || snapshots[0].count != 1 || snapshots[0].errors != 0 || snapshots[0].statuses[http.StatusAccepted] != 1 {
		t.Fatalf("metrics = %#v", snapshots)
	}
}

func TestRunTaskWithTimeoutReturnsTaskFailure(t *testing.T) {
	client := &apiClient{
		baseURL: "http://graphdb-soak-test",
		tenant:  "tenant-a",
		timeout: time.Second,
		http: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && req.URL.Path == "/v1/tasks" {
				return jsonResponse(http.StatusAccepted, `{"id":"task-1","status":"queued"}`), nil
			}
			return jsonResponse(http.StatusOK, `{"id":"task-1","status":"failed","error":"boom"}`), nil
		})},
	}

	err := client.runTaskWithTimeout(context.Background(), nil, "compact", time.Second, "compact", nil)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want task failure", err)
	}
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
