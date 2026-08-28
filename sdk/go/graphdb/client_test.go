package graphdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
		if got := r.Header.Get("User-Agent"); got != "graphdb-go-sdk/1.2.0" {
			t.Fatalf("user agent = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization = %q", got)
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

	client, err := NewClient(server.URL, WithTenant("tenant-a"), WithBearerToken("test-token"))
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

func TestGetEntityEscapesPathSegmentExactlyOnce(t *testing.T) {
	ids := []string{
		"urn:host:default:我来添加一个主机看看",
		"urn:kubernetes:default:namespace/name 100%",
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			expectedPath := "/proxy%20root/v1/entities/" + url.PathEscape(id)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.EscapedPath(); got != expectedPath {
					t.Errorf("escaped path = %q, want %q", got, expectedPath)
				}
				if got := r.URL.Path; got != "/proxy root/v1/entities/"+id {
					t.Errorf("decoded path = %q, want %q", got, "/proxy root/v1/entities/"+id)
				}
				if got := r.URL.Query().Get("min_version"); got != "42" {
					t.Errorf("min_version = %q, want 42", got)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"entity": Entity{ID: id, Kind: "host"}})
			}))
			defer server.Close()

			client, err := NewClient(server.URL+"/proxy%20root/", WithTenant("tenant-a"))
			if err != nil {
				t.Fatal(err)
			}
			entity, err := client.GetEntity(context.Background(), id, &ReadOptions{MinVersion: 42})
			if err != nil {
				t.Fatal(err)
			}
			if entity.ID != id {
				t.Fatalf("entity ID = %q, want %q", entity.ID, id)
			}
		})
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

func TestGraphQLUsesStandardTransport(t *testing.T) {
	var got GraphQLRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/query/graphql" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = io.WriteString(w, `{"data":{"graph":{"version":3}}}`)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.GraphQL(context.Background(), GraphQLRequest{
		Query:         `query Version($request: QueryRequest!) { graph(request: $request) { version } }`,
		OperationName: "Version",
		Variables:     map[string]any{"request": map[string]any{"op": "match"}},
	})
	if err != nil {
		t.Fatalf("GraphQL: %v", err)
	}
	if got.OperationName != "Version" || response.Data["graph"] == nil {
		t.Fatalf("request=%#v response=%#v", got, response)
	}
}

func TestVersion11SchemaAndImportAPIs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/imports":
			data, _ := io.ReadAll(r.Body)
			if r.Header.Get("Content-Type") != "application/x-ndjson" || string(data) != "{\"entity\":{}}\n" {
				t.Fatalf("import content-type=%q body=%q", r.Header.Get("Content-Type"), data)
			}
			if r.URL.Query().Get("format") != "jsonl" || r.URL.Query().Get("batch_size") != "25" {
				t.Fatalf("import query = %v", r.URL.Query())
			}
			_ = json.NewEncoder(w).Encode(Task{ID: "task-import", Type: "bulk_import"})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/relation-schemas/cites":
			_ = json.NewEncoder(w).Encode(RelationSchemaCatalog{Revision: 2, RelationSchemas: []RelationSchema{{RelationType: "cites"}}})
		default:
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, WithTenant("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	task, err := client.StartImport(context.Background(), strings.NewReader("{\"entity\":{}}\n"), ImportOptions{Format: "jsonl", BatchSize: 25})
	if err != nil || task.ID != "task-import" {
		t.Fatalf("task=%#v err=%v", task, err)
	}
	catalog, err := client.PutRelationSchema(context.Background(), RelationSchema{RelationType: "cites", Strict: true})
	if err != nil || catalog.Revision != 2 {
		t.Fatalf("catalog=%#v err=%v", catalog, err)
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
