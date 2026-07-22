package storage

import (
	"context"
	"errors"
	"testing"
)

type memoryNativeClient struct {
	objects         *MemoryStore
	createOnlyCalls []bool
}

func newMemoryNativeClient() *memoryNativeClient {
	return &memoryNativeClient{objects: NewMemoryStore()}
}

func (c *memoryNativeClient) Get(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	return c.objects.GetWithMeta(ctx, key)
}

func (c *memoryNativeClient) Head(ctx context.Context, key string) (ObjectMeta, error) {
	return c.objects.Head(ctx, key)
}

func (c *memoryNativeClient) Put(ctx context.Context, key string, data []byte, createOnly bool) (ObjectMeta, error) {
	c.createOnlyCalls = append(c.createOnlyCalls, createOnly)
	condition := PutCondition{IfNoneMatch: createOnly}
	return c.objects.PutConditional(ctx, key, data, condition)
}

func (c *memoryNativeClient) Delete(ctx context.Context, key string) error {
	return c.objects.Delete(ctx, key)
}

func (c *memoryNativeClient) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return c.objects.List(ctx, prefix)
}

func TestNativeObjectStoreUsesCreateOnlyAndRejectsRemoteCAS(t *testing.T) {
	ctx := context.Background()
	client := newMemoryNativeClient()
	store := newNativeObjectStore(client)

	meta, err := store.PutConditional(ctx, "objects/current", []byte("first"), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("create-only put: %v", err)
	}
	if !meta.Exists || meta.ETag == "" {
		t.Fatalf("create-only meta = %#v", meta)
	}
	if len(client.createOnlyCalls) != 1 || !client.createOnlyCalls[0] {
		t.Fatalf("create-only calls = %#v, want one true call", client.createOnlyCalls)
	}

	if _, err := store.PutConditional(ctx, "objects/current", []byte("second"), PutCondition{IfMatch: meta.ETag}); !errors.Is(err, ErrConditionalWriteUnsupported) {
		t.Fatalf("If-Match err = %v, want ErrConditionalWriteUnsupported", err)
	}
	if len(client.createOnlyCalls) != 1 {
		t.Fatalf("If-Match reached native client: %#v", client.createOnlyCalls)
	}
}

func TestSingleWriterObjectStoreEmulatesETagCAS(t *testing.T) {
	ctx := context.Background()
	client := newMemoryNativeClient()
	store := NewSingleWriterObjectStore(newNativeObjectStore(client))

	created, err := store.PutConditional(ctx, "objects/current", []byte("first"), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	updated, err := store.PutConditional(ctx, "objects/current", []byte("second"), PutCondition{IfMatch: created.ETag})
	if err != nil {
		t.Fatalf("update with current ETag: %v", err)
	}
	if updated.ETag == "" || updated.ETag == created.ETag {
		t.Fatalf("updated meta = %#v, want a new ETag", updated)
	}
	if _, err := store.PutConditional(ctx, "objects/current", []byte("stale"), PutCondition{IfMatch: created.ETag}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update err = %v, want ErrConflict", err)
	}
	if err := store.DeleteConditional(ctx, "objects/current", PutCondition{IfMatch: created.ETag}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete err = %v, want ErrConflict", err)
	}
	if err := store.DeleteConditional(ctx, "objects/current", PutCondition{IfMatch: updated.ETag}); err != nil {
		t.Fatalf("delete with current ETag: %v", err)
	}
	if _, err := store.Get(ctx, "objects/current"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete err = %v, want ErrNotFound", err)
	}
}

func TestTencentCOSBucketURLAddsBucketToServiceEndpoint(t *testing.T) {
	endpoint, err := tencentCOSBucketURL(nativeStoreOptions{
		endpoint: "https://cos.ap-guangzhou.myqcloud.com",
		bucket:   "graphdb-1250000000",
	})
	if err != nil {
		t.Fatalf("tencentCOSBucketURL: %v", err)
	}
	if endpoint.Host != "graphdb-1250000000.cos.ap-guangzhou.myqcloud.com" {
		t.Fatalf("bucket endpoint host = %q", endpoint.Host)
	}
}

var _ nativeObjectClient = (*memoryNativeClient)(nil)
