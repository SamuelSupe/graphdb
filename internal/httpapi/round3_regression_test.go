package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound3ErrorClassificationIgnoresUserMessageSubstrings(t *testing.T) {
	check := func(err error, wantStatus int, wantCode ErrorCode, wantRetryable bool) {
		t.Helper()
		w := httptest.NewRecorder()
		writeStorageError(w, err)
		var payload ErrorResponse
		if decErr := json.Unmarshal(w.Body.Bytes(), &payload); decErr != nil {
			t.Fatalf("decode error response: %v body=%s", decErr, w.Body.String())
		}
		if w.Code != wantStatus || payload.Code != wantCode || payload.Retryable != wantRetryable {
			t.Fatalf("err=%v got status=%d payload=%+v want status=%d code=%s retryable=%v", err, w.Code, payload, wantStatus, wantCode, wantRetryable)
		}
	}

	check(fmt.Errorf("%w: object store unavailable", storage.ErrInvalidArgument), http.StatusBadRequest, ErrorCodeBadRequest, false)

	diskErr := errors.New("read /var/lib/graphdb/tenant deleted/file: input/output error")
	w := httptest.NewRecorder()
	writeStorageError(w, diskErr)
	var payload ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusInternalServerError || payload.Code == ErrorCodeTenantDeleted {
		t.Fatalf("disk error with substring misclassified: status=%d payload=%+v", w.Code, payload)
	}

	check(fmt.Errorf("%w: task is already completed", storage.ErrTaskConflict), http.StatusConflict, ErrorCodeTaskConflict, false)
}

func TestRound3ListTenantsEnforcesTenantScope(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	defer store.ShutdownTasks(ctx)
	for _, id := range []string{"tenant-a", "tenant-b"} {
		if _, err := store.CreateTenant(ctx, id, storage.TenantCreateOptions{Name: id}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()

	// Scoped list without paging must only return the authenticated tenant.
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("scoped GET /v1/tenants status=%d body=%s", w.Code, w.Body.String())
	}
	var list struct {
		Tenants []storage.TenantInfo `json:"tenants"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Tenants) != 1 || list.Tenants[0].TenantID != "tenant-a" {
		t.Fatalf("scoped GET /v1/tenants leaked tenants: %+v", list.Tenants)
	}

	// Scoped paged list must also only return the authenticated tenant.
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants?limit=10", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("scoped paged GET /v1/tenants status=%d body=%s", w.Code, w.Body.String())
	}
	var page storage.TenantInfoPage
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Tenants) != 1 || page.Total != 1 || page.Tenants[0].TenantID != "tenant-a" {
		t.Fatalf("scoped paged GET /v1/tenants leaked tenants: %+v", page)
	}
}

func TestRound3AdminRoutesExposeListQueryTemplates(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	admin := (&Server{Store: store, Mode: "all"}).AdminHandler(false)
	req := httptest.NewRequest(http.MethodGet, "/v1/query/templates", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	w := httptest.NewRecorder()
	admin.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("admin GET /v1/query/templates status=%d body=%s", w.Code, w.Body.String())
	}
}
