package graphdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCypherExactNumbersAndStreamCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Tenant-ID") != "tenant-a" {
			t.Error("missing tenant")
		}
		switch r.URL.Path {
		case "/v1/query/cypher":
			io.WriteString(w, `{"version":1,"columns":["n"],"rows":[[9007199254740993]],"stats":{}}`)
		case "/v1/query/cypher/stream":
			var request CypherRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			io.WriteString(w, "{\"stream\":true}\n{\"row\":[9007199254740993]}\n")
			if request.Query == "failed" {
				io.WriteString(w, "{\"done\":true,\"success\":false,\"error\":{\"code\":\"query_limit_exceeded\",\"message\":\"budget\"}}\n")
			} else if request.Query == "ok" {
				io.WriteString(w, "{\"done\":true,\"success\":true}\n")
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Cypher(context.Background(), CypherRequest{Query: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := result.Rows[0][0].(json.Number); !ok || n.String() != "9007199254740993" {
		t.Fatalf("precision=%#v", result.Rows)
	}
	for _, q := range []string{"ok", "failed", "truncated"} {
		t.Run(q, func(t *testing.T) {
			stream, err := client.CypherStream(context.Background(), CypherRequest{Query: q})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			var item map[string]any
			rows := 0
			for stream.Next(&item) {
				if row, ok := item["row"].([]any); ok {
					rows++
					if n, ok := row[0].(json.Number); !ok || n.String() != "9007199254740993" {
						t.Fatalf("stream precision=%#v", item)
					}
				}
			}
			if rows != 1 {
				t.Errorf("received %d rows, want 1", rows)
			}
			switch q {
			case "ok":
				if stream.Err() != nil {
					t.Fatal(stream.Err())
				}
			case "truncated":
				if !errors.Is(stream.Err(), io.ErrUnexpectedEOF) {
					t.Fatal(stream.Err())
				}
			case "failed":
				var apiErr *APIError
				if !errors.As(stream.Err(), &apiErr) || apiErr.Code != "query_limit_exceeded" {
					t.Fatal(stream.Err())
				}
			}
		})
	}
}
