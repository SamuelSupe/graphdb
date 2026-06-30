package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"graphdb/internal/graph"
	"graphdb/internal/storage"
)

func TestHTTPListEntitiesEdgesAndExportSnapshot(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()

	first := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host&limit=1", "tenant-a", nil)
	if first.Code != http.StatusOK {
		t.Fatalf("entities first page = %d body=%s", first.Code, first.Body.String())
	}
	var firstPage storage.EntityScanResult
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Entities) != 1 || firstPage.NextCursor == "" || !firstPage.IndexedRead {
		t.Fatalf("first page = %#v", firstPage)
	}
	second := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host&limit=1&cursor="+firstPage.NextCursor, "tenant-a", nil)
	if second.Code != http.StatusOK {
		t.Fatalf("entities second page = %d body=%s", second.Code, second.Body.String())
	}
	var secondPage storage.EntityScanResult
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(secondPage.Entities) != 1 || secondPage.NextCursor != "" {
		t.Fatalf("second page = %#v", secondPage)
	}

	manual := serveJSON(handler, http.MethodGet, "/v1/entities?source=manual", "tenant-a", nil)
	if manual.Code != http.StatusOK || !strings.Contains(manual.Body.String(), `"id":"host:b"`) || strings.Contains(manual.Body.String(), `"id":"host:a"`) {
		t.Fatalf("manual entities = %d body=%s", manual.Code, manual.Body.String())
	}
	edges := serveJSON(handler, http.MethodGet, "/v1/edges?type=runs_on&from=service:api", "tenant-a", nil)
	if edges.Code != http.StatusOK || !strings.Contains(edges.Body.String(), `"from":"service:api"`) {
		t.Fatalf("edges = %d body=%s", edges.Code, edges.Body.String())
	}
	snapshot := serveJSON(handler, http.MethodGet, "/v1/export/snapshot", "tenant-a", nil)
	if snapshot.Code != http.StatusOK || !strings.Contains(snapshot.Body.String(), `"snapshot"`) || !strings.Contains(snapshot.Body.String(), `"entities"`) {
		t.Fatalf("snapshot = %d body=%s", snapshot.Code, snapshot.Body.String())
	}
}

func TestHTTPListEntitiesCursorStaysOnPinnedVersionAfterManifestAdvance(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()

	first := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host&limit=1", "tenant-a", nil)
	if first.Code != http.StatusOK {
		t.Fatalf("entities first page = %d body=%s", first.Code, first.Body.String())
	}
	var firstPage storage.EntityScanResult
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.Version != catalog.Version || firstPage.NextCursor == "" {
		t.Fatalf("first page = %#v, want cursor pinned to catalog version %d", firstPage, catalog.Version)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host", Fields: graph.Fields{"hostname": "app-c"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}

	second := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host&limit=1&cursor="+firstPage.NextCursor, "tenant-a", nil)
	if second.Code != http.StatusOK {
		t.Fatalf("entities second page = %d body=%s", second.Code, second.Body.String())
	}
	var secondPage storage.EntityScanResult
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if secondPage.Version != catalog.Version ||
		len(secondPage.Entities) != 1 ||
		secondPage.NextCursor != "" ||
		strings.Contains(second.Body.String(), `"id":"host:c"`) {
		t.Fatalf("second page = %#v body=%s, want pinned version %d without new entity", secondPage, second.Body.String(), catalog.Version)
	}
}

func TestHTTPListEdgesCursorStaysOnPinnedVersionAfterManifestAdvance(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-2",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:b",
			Source:     "agent",
			ExternalID: "edge-2",
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed second edge: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()

	first := serveJSON(handler, http.MethodGet, "/v1/edges?type=runs_on&limit=1", "tenant-a", nil)
	if first.Code != http.StatusOK {
		t.Fatalf("edges first page = %d body=%s", first.Code, first.Body.String())
	}
	var firstPage storage.EdgeScanResult
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.Version != catalog.Version || firstPage.NextCursor == "" {
		t.Fatalf("first edge page = %#v, want cursor pinned to catalog version %d", firstPage, catalog.Version)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host", Fields: graph.Fields{"hostname": "app-c"}}},
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-3",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:c",
			Source:     "agent",
			ExternalID: "edge-3",
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}

	second := serveJSON(handler, http.MethodGet, "/v1/edges?type=runs_on&limit=1&cursor="+firstPage.NextCursor, "tenant-a", nil)
	if second.Code != http.StatusOK {
		t.Fatalf("edges second page = %d body=%s", second.Code, second.Body.String())
	}
	var secondPage storage.EdgeScanResult
	if err := json.Unmarshal(second.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if secondPage.Version != catalog.Version ||
		len(secondPage.Edges) != 1 ||
		secondPage.NextCursor != "" ||
		strings.Contains(second.Body.String(), `"to":"host:c"`) {
		t.Fatalf("second edge page = %#v body=%s, want pinned version %d without new edge", secondPage, second.Body.String(), catalog.Version)
	}
}

func TestHTTPStreamEntitiesCursorStaysOnPinnedVersionAfterManifestAdvance(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()

	first := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host&limit=1", "tenant-a", nil)
	if first.Code != http.StatusOK {
		t.Fatalf("entities first page = %d body=%s", first.Code, first.Body.String())
	}
	var firstPage storage.EntityScanResult
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.Version != catalog.Version || firstPage.NextCursor == "" {
		t.Fatalf("first page = %#v, want cursor pinned to catalog version %d", firstPage, catalog.Version)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host", Fields: graph.Fields{"hostname": "app-c"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}

	stream := serveJSON(handler, http.MethodGet, "/v1/entities/stream?kind=host&cursor="+firstPage.NextCursor, "tenant-a", nil)
	body := stream.Body.String()
	if stream.Code != http.StatusOK ||
		!strings.Contains(body, `"done":true`) ||
		strings.Contains(body, "cursor version") ||
		strings.Contains(body, `"id":"host:c"`) {
		t.Fatalf("entity stream continuation = %d body=%s", stream.Code, body)
	}
}

func TestHTTPStreamEdgesCursorStaysOnPinnedVersionAfterManifestAdvance(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-2",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:b",
			Source:     "agent",
			ExternalID: "edge-2",
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed second edge: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()

	first := serveJSON(handler, http.MethodGet, "/v1/edges?type=runs_on&limit=1", "tenant-a", nil)
	if first.Code != http.StatusOK {
		t.Fatalf("edges first page = %d body=%s", first.Code, first.Body.String())
	}
	var firstPage storage.EdgeScanResult
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.Version != catalog.Version || firstPage.NextCursor == "" {
		t.Fatalf("first edge page = %#v, want cursor pinned to catalog version %d", firstPage, catalog.Version)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host", Fields: graph.Fields{"hostname": "app-c"}}},
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-3",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:c",
			Source:     "agent",
			ExternalID: "edge-3",
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}

	stream := serveJSON(handler, http.MethodGet, "/v1/edges/stream?type=runs_on&cursor="+firstPage.NextCursor, "tenant-a", nil)
	body := stream.Body.String()
	if stream.Code != http.StatusOK ||
		!strings.Contains(body, `"done":true`) ||
		strings.Contains(body, "cursor version") ||
		strings.Contains(body, `"to":"host:c"`) {
		t.Fatalf("edge stream continuation = %d body=%s", stream.Code, body)
	}
}

func TestHTTPScanMinVersionFallsBackWhenCatalogIsStale(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host", Fields: graph.Fields{"hostname": "app-c"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()

	rr := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host&min_version=2", "tenant-a", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("entities = %d body=%s", rr.Code, rr.Body.String())
	}
	var page storage.EntityScanResult
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if page.Version != 2 || len(page.Entities) != 3 || !strings.Contains(rr.Body.String(), `"id":"host:c"`) {
		t.Fatalf("page = %#v", page)
	}
}

func TestHTTPScanReadAdmissionReturns429(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	admission := NewQueryAdmission(1, 1, time.Millisecond)
	release, err := admission.Acquire(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("hold admission: %v", err)
	}
	defer release()
	handler := (&Server{Store: store, Mode: "reader", ReadAdmission: admission}).Handler()

	rr := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host", "tenant-a", nil)
	if rr.Code != http.StatusTooManyRequests ||
		!strings.Contains(rr.Body.String(), `"code":"too_many_requests"`) ||
		!strings.Contains(rr.Body.String(), "read admission queue timeout") {
		t.Fatalf("read admission = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPScanStreamsNDJSON(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()

	entities := serveJSON(handler, http.MethodGet, "/v1/entities/stream?kind=host", "tenant-a", nil)
	if entities.Code != http.StatusOK || !strings.Contains(entities.Body.String(), `"resource":"entity"`) || !strings.Contains(entities.Body.String(), `"entity"`) || !strings.Contains(entities.Body.String(), `"done":true`) {
		t.Fatalf("entity stream = %d body=%s", entities.Code, entities.Body.String())
	}
	edges := serveJSON(handler, http.MethodGet, "/v1/edges/stream?type=runs_on", "tenant-a", nil)
	if edges.Code != http.StatusOK || !strings.Contains(edges.Body.String(), `"resource":"edge"`) || !strings.Contains(edges.Body.String(), `"edge"`) || !strings.Contains(edges.Body.String(), `"done":true`) {
		t.Fatalf("edge stream = %d body=%s", edges.Code, edges.Body.String())
	}
	snapshot := serveJSON(handler, http.MethodGet, "/v1/export/snapshot/stream?inline=true", "tenant-a", nil)
	if snapshot.Code != http.StatusOK || !strings.Contains(snapshot.Body.String(), `"stream":"snapshot"`) || !strings.Contains(snapshot.Body.String(), `"entity"`) || !strings.Contains(snapshot.Body.String(), `"edge"`) {
		t.Fatalf("snapshot stream = %d body=%s", snapshot.Code, snapshot.Body.String())
	}
}

func TestHTTPStreamEntitiesPinsCatalogAcrossManifestAdvance(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	entities := make([]graph.Entity, 0, scanStreamPageSize+1)
	for i := 0; i < scanStreamPageSize+1; i++ {
		entities = append(entities, graph.Entity{
			ID:     fmt.Sprintf("host:stream-%03d", i),
			Kind:   "host",
			Fields: graph.Fields{"hostname": fmt.Sprintf("stream-%03d", i)},
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	req := httptest.NewRequest(http.MethodGet, "/v1/entities/stream?kind=host", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := &commitOnStreamWriteRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		commit: func() error {
			_, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
				ID:     "host:stream-new",
				Kind:   "host",
				Fields: graph.Fields{"hostname": "stream-new"},
			}}}, storage.CommitOptions{})
			return err
		},
	}

	handler.ServeHTTP(rr, req)
	if rr.commitErr != nil {
		t.Fatalf("advance commit: %v", rr.commitErr)
	}
	if !rr.committed {
		t.Fatal("test did not advance manifest during stream")
	}
	body := rr.Body.String()
	if rr.Code != http.StatusOK ||
		strings.Contains(body, "cursor version") ||
		strings.Contains(body, `"code":"bad_request"`) ||
		!strings.Contains(body, `"done":true`) ||
		strings.Contains(body, "host:stream-new") {
		t.Fatalf("stream status=%d body=%s", rr.Code, body)
	}
}

type commitOnStreamWriteRecorder struct {
	*httptest.ResponseRecorder
	committed bool
	commit    func() error
	commitErr error
}

func (r *commitOnStreamWriteRecorder) Write(data []byte) (int, error) {
	if !r.committed && strings.Contains(string(data), `"resource":"entity"`) {
		r.committed = true
		r.commitErr = r.commit()
	}
	return r.ResponseRecorder.Write(data)
}

func TestHTTPStreamSnapshotUsesShardedCatalogRefs(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	seedHTTPScanTenant(t, ctx, store)
	manifest, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if err := store.Objects.Delete(ctx, manifest.SnapshotKey); err != nil {
		t.Fatalf("delete snapshot object: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()

	snapshot := serveJSON(handler, http.MethodGet, "/v1/export/snapshot/stream", "tenant-a", nil)
	if snapshot.Code != http.StatusOK ||
		!strings.Contains(snapshot.Body.String(), `"stream":"snapshot"`) ||
		!strings.Contains(snapshot.Body.String(), `"mode":"refs"`) ||
		!strings.Contains(snapshot.Body.String(), `"sharded":true`) ||
		!strings.Contains(snapshot.Body.String(), `"entity_page"`) ||
		!strings.Contains(snapshot.Body.String(), `"edge_shard"`) {
		t.Fatalf("snapshot stream = %d body=%s", snapshot.Code, snapshot.Body.String())
	}
	inline := serveJSON(handler, http.MethodGet, "/v1/export/snapshot/stream?inline=true", "tenant-a", nil)
	if inline.Code != http.StatusOK ||
		!strings.Contains(inline.Body.String(), `"indexed_read":true`) ||
		!strings.Contains(inline.Body.String(), `"entity"`) ||
		!strings.Contains(inline.Body.String(), `"edge"`) {
		t.Fatalf("inline snapshot stream = %d body=%s", inline.Code, inline.Body.String())
	}
}

func seedHTTPScanTenant(t *testing.T, ctx context.Context, store *storage.TenantStore) {
	t.Helper()
	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name:     "runs_on",
			FromKind: "service",
			ToKind:   "host",
			Directed: true,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host", Source: "aws", ExternalID: "i-a", Fields: graph.Fields{"hostname": "app-a"}},
			{ID: "host:b", Kind: "host", Source: "manual", ExternalID: "host-b", Fields: graph.Fields{"hostname": "app-b"}},
			{ID: "service:api", Kind: "service", Source: "agent", ExternalID: "svc-api", Fields: graph.Fields{"name": "api"}},
		},
		UpsertEdges: []graph.Edge{{
			ID:         "collector-edge-1",
			Type:       "runs_on",
			From:       "service:api",
			To:         "host:a",
			Source:     "agent",
			ExternalID: "edge-1",
		}},
	}, storage.CommitOptions{})
	if err != nil {
		t.Fatalf("seed commit: %v", err)
	}
}
