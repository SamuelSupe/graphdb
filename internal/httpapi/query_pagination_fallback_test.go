package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPMatchCursorKeepsOrderWhenLazyCatalogDisappears(t *testing.T) {
	objects, writer := paginationFallbackStore(t)
	if _, err := writer.RebuildIndexes(
		context.Background(), "tenant-a",
	); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	request := query.Request{Op: "match", Kind: "host", Limit: 7}
	handler := paginationFallbackHandler(objects)
	first := executePaginationRequest(t, handler, request)
	request.Cursor = first.NextCursor
	want := executePaginationRequest(t, handler, request)

	deleteIndexCatalogs(t, objects)
	got := executePaginationRequest(
		t, paginationFallbackHandler(objects), request,
	)
	if wantIDs, gotIDs := httpQueryEntityIDs(want), httpQueryEntityIDs(got); !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf(
			"materialized fallback ids = %v, want lazy continuation %v",
			gotIDs, wantIDs,
		)
	}
}

func TestHTTPMatchIdentityCursorFallsBackAfterLazyCatalogAppears(t *testing.T) {
	objects, writer := paginationFallbackStore(t)
	request := query.Request{Op: "match", Kind: "host", Limit: 7}
	handler := paginationFallbackHandler(objects)
	first := executePaginationRequest(t, handler, request)
	request.Cursor = first.NextCursor
	want := executePaginationRequest(t, handler, request)

	if _, err := writer.RebuildIndexes(
		context.Background(), "tenant-a",
	); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	got := executePaginationRequest(
		t, paginationFallbackHandler(objects), request,
	)
	if wantIDs, gotIDs := httpQueryEntityIDs(want), httpQueryEntityIDs(got); !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf(
			"lazy retry fallback ids = %v, want identity continuation %v",
			gotIDs, wantIDs,
		)
	}
}

func paginationFallbackStore(
	t *testing.T,
) (storage.ObjectStore, *storage.TenantStore) {
	t.Helper()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	entities := make([]graph.Entity, 128)
	for i := range entities {
		entities[i] = graph.Entity{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
		}
	}
	if _, err := writer.Commit(
		context.Background(),
		"tenant-a",
		graph.Mutations{
			UpsertCITypes:  []graph.CIType{{Name: "host"}},
			UpsertEntities: entities,
		},
		storage.CommitOptions{},
	); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	return objects, writer
}

func paginationFallbackHandler(objects storage.ObjectStore) http.Handler {
	reader := storage.NewTenantStore(objects, "test")
	return (&Server{Store: reader, Mode: "reader"}).Handler()
}

func executePaginationRequest(
	t *testing.T,
	handler http.Handler,
	request query.Request,
) query.Response {
	t.Helper()
	response := serveJSON(
		handler, http.MethodPost, "/v1/query", "tenant-a", request,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"query status = %d body=%s",
			response.Code, response.Body.String(),
		)
	}
	var decoded query.Response
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if decoded.NextCursor == "" {
		t.Fatal("query did not return a next cursor")
	}
	return decoded
}

func deleteIndexCatalogs(
	t *testing.T,
	objects storage.ObjectStore,
) {
	t.Helper()
	listed, err := objects.List(context.Background(), "test/tenants/tenant-a/indexes/")
	if err != nil {
		t.Fatalf("list index objects: %v", err)
	}
	deleted := 0
	for _, object := range listed {
		if !strings.Contains(object.Key, "catalog") {
			continue
		}
		if err := objects.Delete(context.Background(), object.Key); err != nil {
			t.Fatalf("delete catalog %q: %v", object.Key, err)
		}
		deleted++
	}
	if deleted == 0 {
		t.Fatal("no index catalog objects were deleted")
	}
}

func httpQueryEntityIDs(response query.Response) []string {
	ids := make([]string, 0, len(response.Results))
	for _, result := range response.Results {
		if result.Entity != nil {
			ids = append(ids, result.Entity.ID)
		}
	}
	return ids
}
