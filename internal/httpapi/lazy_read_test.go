package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPQueryUsesLazyEntityReadWithoutLoadingGraph(t *testing.T) {
	handler := lazyReadHandler(t)
	body := query.Request{
		Op:      "match",
		Kind:    "host",
		Where:   []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Project: []string{"hostname"},
	}
	rr := serveJSON(handler, "POST", "/v1/query", "tenant-a", body)
	if rr.Code != 200 {
		t.Fatalf("query = %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"host:app-01"`) || strings.Contains(rr.Body.String(), `us-east-1`) {
		t.Fatalf("lazy projected body=%s", rr.Body.String())
	}
}

func TestHTTPQueryUsesLazyEntityPageScanForFuzzyMatch(t *testing.T) {
	handler := lazyReadHandler(t)
	body := query.Request{
		Op:      "match",
		Kind:    "service",
		Where:   []query.Filter{{Field: "name", Op: "fuzzy", Value: "api"}},
		Project: []string{"name"},
		Limit:   10,
	}
	rr := serveJSON(handler, "POST", "/v1/query", "tenant-a", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("query = %d body=%s", rr.Code, rr.Body.String())
	}
	text := rr.Body.String()
	if !strings.Contains(text, `"service:api"`) || strings.Contains(text, `"host:app-01"`) {
		t.Fatalf("lazy fuzzy body=%s", text)
	}
}

func TestHTTPGetEntityUsesPersistedByIDWithoutLoadingGraph(t *testing.T) {
	handler := lazyReadHandler(t)
	rr := serveJSON(handler, "GET", "/v1/entities/host:app-01", "tenant-a", nil)
	if rr.Code != 200 {
		t.Fatalf("get entity = %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"host:app-01"`) || !strings.Contains(rr.Body.String(), `us-east-1`) {
		t.Fatalf("entity body=%s", rr.Body.String())
	}
}

func TestHTTPQueryStreamUsesLazyPaginationWithoutLoadingGraph(t *testing.T) {
	handler := lazyReadHandler(t)
	body := query.Request{
		Op:      "match",
		Kind:    "host",
		Where:   []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Project: []string{"hostname"},
		Limit:   1,
	}
	rr := serveJSON(handler, "POST", "/v1/query/stream", "tenant-a", body)
	if rr.Code != 200 {
		t.Fatalf("stream = %d body=%s", rr.Code, rr.Body.String())
	}
	text := rr.Body.String()
	if !strings.Contains(text, `"stream":true`) || !strings.Contains(text, `"host:app-01"`) || !strings.Contains(text, `"done":true`) {
		t.Fatalf("stream body=%s", text)
	}
}

func TestHTTPQueryStreamLazyFlushesNDJSON(t *testing.T) {
	handler := lazyReadHandler(t)
	body := query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit: 1,
	}
	rr := serveJSON(handler, "POST", "/v1/query/stream", "tenant-a", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("stream = %d body=%s", rr.Code, rr.Body.String())
	}
	if !rr.Flushed {
		t.Fatalf("stream did not flush NDJSON response: body=%s", rr.Body.String())
	}
}

func TestHTTPQueryUsesStaleIndexCatalogAsLazySnapshot(t *testing.T) {
	handler := staleLazyReadHandler(t)
	body := query.Request{
		Op:      "match",
		Kind:    "host",
		Where:   []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Project: []string{"hostname"},
	}
	rr := serveJSON(handler, "POST", "/v1/query", "tenant-a", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("query = %d body=%s", rr.Code, rr.Body.String())
	}
	text := rr.Body.String()
	if !strings.Contains(text, `"version":1`) || !strings.Contains(text, `"host:app-01"`) || strings.Contains(text, `"host:app-02"`) {
		t.Fatalf("stale catalog lazy query body=%s", text)
	}
}

func TestHTTPQueryStreamUsesStaleIndexCatalogAsLazySnapshot(t *testing.T) {
	handler := staleLazyReadHandler(t)
	body := query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit: 1,
	}
	rr := serveJSON(handler, "POST", "/v1/query/stream", "tenant-a", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("stream = %d body=%s", rr.Code, rr.Body.String())
	}
	text := rr.Body.String()
	if !strings.Contains(text, `"version":1`) || !strings.Contains(text, `"stream":true`) || !strings.Contains(text, `"done":true`) {
		t.Fatalf("stale catalog lazy stream body=%s", text)
	}
}

func TestHTTPImpactOutUsesLazyEdgeShardWithoutLoadingGraph(t *testing.T) {
	handler := lazyReadHandler(t)
	body := query.Request{
		Op:           "impact",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
		Depth:        1,
		Limit:        10,
	}
	rr := serveJSON(handler, "POST", "/v1/query", "tenant-a", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("impact = %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"host:app-01"`) {
		t.Fatalf("lazy impact body=%s", rr.Body.String())
	}
}

func TestHTTPNeighborsUsesAuthoritativeEmptyEdgeIndexes(t *testing.T) {
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	if _, err := writer.Commit(
		context.Background(),
		"tenant-a",
		graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host",
		}}},
		storage.CommitOptions{},
	); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := writer.RebuildIndexes(
		context.Background(), "tenant-a",
	); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	reader := storage.NewTenantStore(
		&denyGraphLoadStore{base: objects}, "test",
	)
	handler := (&Server{Store: reader, Mode: "reader"}).Handler()

	for _, direction := range []string{"out", "in", "both"} {
		rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{
			Op:        "neighbors",
			ID:        "host:app-01",
			Direction: direction,
			Limit:     10,
		})
		if rr.Code != http.StatusOK ||
			!strings.Contains(rr.Body.String(), `"results":[]`) {
			t.Fatalf(
				"%s neighbors = %d body=%s",
				direction,
				rr.Code,
				rr.Body.String(),
			)
		}
	}
}

func TestHTTPQueryStreamLazyUsesAdmission(t *testing.T) {
	admission := NewQueryAdmission(1, 1, time.Millisecond)
	release, err := admission.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("acquire admission: %v", err)
	}
	defer release()
	handler := lazyReadHandlerWithAdmission(t, admission)
	body := query.Request{
		Op:      "match",
		Kind:    "host",
		Where:   []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit:   1,
		Profile: true,
	}
	rr := serveJSON(handler, "POST", "/v1/query/stream", "tenant-a", body)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("stream status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPLazyTraverseKeepsFullPathEntitiesWhenProjected(t *testing.T) {
	handler := lazyReadHandler(t)
	body := query.Request{
		Op:           "traverse",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
		Depth:        1,
		Project:      []string{"hostname"},
		Limit:        10,
	}
	rr := serveJSON(handler, "POST", "/v1/query", "tenant-a", body)
	if rr.Code != 200 {
		t.Fatalf("traverse = %d body=%s", rr.Code, rr.Body.String())
	}
	text := rr.Body.String()
	if !strings.Contains(text, `"host:app-01"`) || !strings.Contains(text, `us-east-1`) {
		t.Fatalf("lazy traverse path lost full entity fields: %s", text)
	}
}

func TestHTTPQueryStreamLazyPreflightErrorUsesHTTPStatus(t *testing.T) {
	handler := lazyReadHandler(t)
	body := query.Request{
		Op:     "match",
		Kind:   "host",
		Where:  []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit:  1,
		Cursor: "not-a-valid-cursor",
	}
	rr := serveJSON(handler, "POST", "/v1/query/stream", "tenant-a", body)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("stream = %d body=%s", rr.Code, rr.Body.String())
	}
	text := rr.Body.String()
	if !strings.Contains(text, "invalid cursor") || !strings.Contains(text, `"code":"invalid_query"`) || strings.Contains(text, `"stream":true`) {
		t.Fatalf("stream preflight error body=%s", text)
	}
}

func TestHTTPQueryStreamLazyRejectsCursorAfterOutsideResultSet(t *testing.T) {
	handler := lazyReadHandler(t)
	cursor := encodeTestCursor(t, map[string]any{"version": float64(1), "after": "entity:host:missing"})
	body := query.Request{
		Op:     "match",
		Kind:   "host",
		Where:  []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit:  1,
		Cursor: cursor,
	}
	rr := serveJSON(handler, "POST", "/v1/query/stream", "tenant-a", body)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("stream = %d body=%s", rr.Code, rr.Body.String())
	}
	text := rr.Body.String()
	if !strings.Contains(text, "was not found in result set") || !strings.Contains(text, `"code":"invalid_query"`) || strings.Contains(text, `"stream":true`) {
		t.Fatalf("stream missing cursor body=%s", text)
	}
}

func encodeTestCursor(t *testing.T, cursor map[string]any) string {
	t.Helper()
	data, err := json.Marshal(cursor)
	if err != nil {
		t.Fatalf("marshal cursor: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func lazyReadHandler(t *testing.T) http.Handler {
	return lazyReadHandlerWithAdmission(t, nil)
}

func lazyReadHandlerWithAdmission(t *testing.T, admission *QueryAdmission) http.Handler {
	return lazyReadHandlerWithOptions(t, admission, false)
}

func staleLazyReadHandler(t *testing.T) http.Handler {
	return lazyReadHandlerWithOptions(t, nil, true)
}

func lazyReadHandlerWithOptions(t *testing.T, admission *QueryAdmission, staleCatalog bool) http.Handler {
	t.Helper()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	if _, err := writer.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}, "region": {Type: "string"}},
		}},
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host",
			Directed: true, ImpactDirection: "forward",
		}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service", Fields: graph.Fields{"name": "api"}},
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "us-east-1"}},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := writer.RebuildIndexes(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if staleCatalog {
		if _, err := writer.Commit(context.Background(), "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02", "region": "us-west-2"}},
			},
		}, storage.CommitOptions{}); err != nil {
			t.Fatalf("stale commit: %v", err)
		}
	}
	reader := storage.NewTenantStore(&denyGraphLoadStore{base: objects}, "test")
	return (&Server{Store: reader, Mode: "reader", Admission: admission}).Handler()
}

type denyGraphLoadStore struct {
	base storage.ObjectStore
}

func (s *denyGraphLoadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if deniedGraphLoadKey(key) {
		return nil, errDeniedGraphLoad
	}
	return s.base.Get(ctx, key)
}

func (s *denyGraphLoadStore) GetWithMeta(ctx context.Context, key string) ([]byte, storage.ObjectMeta, error) {
	if deniedGraphLoadKey(key) {
		return nil, storage.ObjectMeta{Key: key}, errDeniedGraphLoad
	}
	return s.base.GetWithMeta(ctx, key)
}

func (s *denyGraphLoadStore) Put(ctx context.Context, key string, data []byte) error {
	return s.base.Put(ctx, key, data)
}

func (s *denyGraphLoadStore) PutConditional(ctx context.Context, key string, data []byte, condition storage.PutCondition) (storage.ObjectMeta, error) {
	return s.base.PutConditional(ctx, key, data, condition)
}

func (s *denyGraphLoadStore) Delete(ctx context.Context, key string) error {
	return s.base.Delete(ctx, key)
}

func (s *denyGraphLoadStore) DeleteConditional(ctx context.Context, key string, condition storage.PutCondition) error {
	return s.base.DeleteConditional(ctx, key, condition)
}

func (s *denyGraphLoadStore) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return s.base.List(ctx, prefix)
}

func deniedGraphLoadKey(key string) bool {
	return strings.Contains(key, "/commits/") || strings.Contains(key, "/snapshots/")
}

var errDeniedGraphLoad = &graphLoadDeniedError{}

type graphLoadDeniedError struct{}

func (e *graphLoadDeniedError) Error() string {
	return "graph load object access denied in lazy read test"
}
