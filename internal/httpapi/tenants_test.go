package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"graphdb/internal/graph"
	"graphdb/internal/storage"
)

func TestHTTPTenantLifecycleCreateDisableEnableDeletePurge(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	create := serveJSON(handler, http.MethodPost, "/v1/tenants", "", tenantRequest{
		TenantID: "tenant-a", Name: "Tenant A", Labels: map[string]string{"env": "test"},
	})
	if create.Code != http.StatusOK || !strings.Contains(create.Body.String(), `"status":"active"`) {
		t.Fatalf("create tenant = %d body=%s", create.Code, create.Body.String())
	}
	commit := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit active = %d body=%s", commit.Code, commit.Body.String())
	}
	disabled := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/disable", "", nil)
	if disabled.Code != http.StatusOK || !strings.Contains(disabled.Body.String(), `"status":"disabled"`) {
		t.Fatalf("disable tenant = %d body=%s", disabled.Code, disabled.Body.String())
	}
	blocked := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}})
	var body ErrorResponse
	if err := json.Unmarshal(blocked.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode disabled response: %v body=%s", err, blocked.Body.String())
	}
	if blocked.Code != http.StatusForbidden || body.Code != ErrorCodeTenantDisabled {
		t.Fatalf("disabled commit = %d body=%#v raw=%s", blocked.Code, body, blocked.Body.String())
	}
	enabled := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/enable", "", nil)
	if enabled.Code != http.StatusOK || !strings.Contains(enabled.Body.String(), `"status":"active"`) {
		t.Fatalf("enable tenant = %d body=%s", enabled.Code, enabled.Body.String())
	}
	deleted := serveJSON(handler, http.MethodDelete, "/v1/tenants/tenant-a", "", nil)
	if deleted.Code != http.StatusOK || !strings.Contains(deleted.Body.String(), `"status":"deleted"`) {
		t.Fatalf("delete tenant = %d body=%s", deleted.Code, deleted.Body.String())
	}
	readDeleted := serveJSON(handler, http.MethodGet, "/v1/entities/host:a", "tenant-a", nil)
	if err := json.Unmarshal(readDeleted.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode deleted response: %v body=%s", err, readDeleted.Body.String())
	}
	if readDeleted.Code != http.StatusGone || body.Code != ErrorCodeTenantDeleted {
		t.Fatalf("read deleted = %d body=%#v raw=%s", readDeleted.Code, body, readDeleted.Body.String())
	}
	purged := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/purge", "", nil)
	if purged.Code != http.StatusOK || !strings.Contains(purged.Body.String(), `"deleted":`) {
		t.Fatalf("purge tenant = %d body=%s", purged.Code, purged.Body.String())
	}
}

func TestHTTPTenantClone(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	if rr := serveJSON(handler, http.MethodPost, "/v1/tenants", "", tenantRequest{TenantID: "tenant-a", Name: "source"}); rr.Code != http.StatusOK {
		t.Fatalf("create source = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}}); rr.Code != http.StatusOK {
		t.Fatalf("commit source = %d body=%s", rr.Code, rr.Body.String())
	}
	clone := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/clone", "", tenantCloneRequest{TargetTenantID: "tenant-b"})
	if clone.Code != http.StatusAccepted || !strings.Contains(clone.Body.String(), `"tenant_id":"tenant-b"`) || !strings.Contains(clone.Body.String(), `"cloned_from":"tenant-a"`) {
		t.Fatalf("clone tenant = %d body=%s", clone.Code, clone.Body.String())
	}
	readClone := serveJSON(handler, http.MethodGet, "/v1/entities/host:a", "tenant-b", nil)
	if readClone.Code != http.StatusOK || !strings.Contains(readClone.Body.String(), `"host:a"`) {
		t.Fatalf("read clone = %d body=%s", readClone.Code, readClone.Body.String())
	}
}

func TestHTTPTenantListUsesManagedRegistryByDefault(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	if _, err := store.Commit(context.Background(), "legacy-tenant", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:legacy", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit legacy tenant: %v", err)
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/tenants", "", tenantRequest{TenantID: "managed-tenant"}); rr.Code != http.StatusOK {
		t.Fatalf("create managed tenant = %d body=%s", rr.Code, rr.Body.String())
	}
	list := serveJSON(handler, http.MethodGet, "/v1/tenants", "", nil)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"managed-tenant"`) || !strings.Contains(list.Body.String(), `"legacy-tenant"`) {
		t.Fatalf("managed list = %d body=%s", list.Code, list.Body.String())
	}
	legacy := serveJSON(handler, http.MethodGet, "/v1/tenants?include_legacy=true", "", nil)
	if legacy.Code != http.StatusOK || !strings.Contains(legacy.Body.String(), `"managed-tenant"`) || !strings.Contains(legacy.Body.String(), `"legacy-tenant"`) {
		t.Fatalf("legacy list = %d body=%s", legacy.Code, legacy.Body.String())
	}
}
