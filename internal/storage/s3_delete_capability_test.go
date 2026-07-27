package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestS3ConditionalDeleteFailsClosedWhenProviderIgnoresIfMatch(t *testing.T) {
	var mu sync.Mutex
	objects := map[string]string{}
	sequence := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch request.Method {
		case http.MethodPut:
			current, exists := objects[request.URL.Path]
			if request.Header.Get("If-None-Match") == "*" && exists ||
				request.Header.Get("If-Match") != "" &&
					request.Header.Get("If-Match") != quoteETag(current) {
				writer.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			sequence++
			etag := fmt.Sprintf("etag-%d", sequence)
			objects[request.URL.Path] = etag
			writer.Header().Set("ETag", quoteETag(etag))
			writer.WriteHeader(http.StatusOK)
		case http.MethodHead:
			etag, exists := objects[request.URL.Path]
			if !exists {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.Header().Set("ETag", quoteETag(etag))
			writer.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			// Model an S3-compatible provider that accepts but ignores If-Match.
			delete(objects, request.URL.Path)
			writer.WriteHeader(http.StatusNoContent)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store, err := NewS3StoreWithOptions(
		server.URL, "bucket", "us-east-1", "access", "secret",
		S3Options{PathStyle: true},
	)
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	key := "objects/current.json"
	oldMeta, err := store.PutConditional(
		context.Background(), key, []byte("old"), PutCondition{},
	)
	if err != nil {
		t.Fatalf("put old: %v", err)
	}
	newMeta, err := store.PutConditional(
		context.Background(), key, []byte("new"), PutCondition{},
	)
	if err != nil {
		t.Fatalf("put new: %v", err)
	}

	if err := store.DeleteConditional(
		context.Background(), key, PutCondition{IfMatch: oldMeta.ETag},
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale conditional delete err = %v, want ErrConflict", err)
	}
	if _, err := store.Head(context.Background(), key); err != nil {
		t.Fatalf("stale conditional delete removed target: %v", err)
	}
	if err := store.DeleteConditional(
		context.Background(), key, PutCondition{IfMatch: newMeta.ETag},
	); !errors.Is(err, ErrConditionalDeleteUnsupported) {
		t.Fatalf("matching conditional delete err = %v, want unsupported", err)
	}
	if _, err := store.Head(context.Background(), key); err != nil {
		t.Fatalf("unsupported conditional delete removed target: %v", err)
	}

	tenantStore := NewTenantStore(store, "graphdb")
	err = tenantStore.PutCoordinationMarker(
		context.Background(), CoordinationPostgres, "namespace-a",
	)
	if !errors.Is(err, ErrConditionalDeleteUnsupported) {
		t.Fatalf("PostgreSQL marker err = %v, want unsupported", err)
	}
	if _, err := store.Head(
		context.Background(), tenantStore.coordinationMarkerKey(),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unsupported provider published coordination marker: %v", err)
	}
}
