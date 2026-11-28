package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound5ReaderModeProtectsRunningQueryDeleteFromCrossOrigin(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "graphdb")
	srv := &Server{Store: store, Mode: "reader"}
	handler := srv.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/v1/queries/running/q-123", nil)
	req.Header.Set("Origin", "https://evil.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site DELETE /v1/queries/running/ in reader mode returned %d, want 403", rec.Code)
	}
}

func TestRound5MissingEntityQueryDoesNotPoisonTenantIndexCache(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewFileStore(t.TempDir())
	defer fs.Close()
	store := storage.NewTenantStore(fs, "graphdb")
	if _, err := store.CreateTenant(ctx, "t1", storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, "t1", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "t1"); err != nil {
		t.Fatal(err)
	}

	srv := &Server{Store: store, Mode: "all"}
	handler := srv.Handler()

	body := []byte(`{"op":"match","where":[{"field":"id","op":"eq","value":"does-not-exist"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "t1")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("missing-entity query status = %d, body=%s", rec.Code, rec.Body.String())
	}

	manifest, err := store.CurrentManifest(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	_, _, ok := store.SelectQueryRead(ctx, "t1", query.Request{Op: "match", Kind: "host"}, 0, manifest.Version, false)
	if !ok {
		t.Fatalf("missing-entity lookup poisoned tenant-wide lazy index cache (now=%v)", time.Now())
	}
}
