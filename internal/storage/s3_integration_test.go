package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"graphdb/internal/graph"
)

func TestS3StoreIntegration(t *testing.T) {
	if os.Getenv("GRAPHDB_MINIO_INTEGRATION") != "1" {
		t.Skip("set GRAPHDB_MINIO_INTEGRATION=1 to run against S3/MinIO")
	}
	s3, err := NewS3Store(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_BUCKET"),
		envOr("S3_REGION", "us-east-1"),
		envOr("S3_ACCESS_KEY_ID", os.Getenv("AWS_ACCESS_KEY_ID")),
		envOr("S3_SECRET_ACCESS_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY")),
	)
	if err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(s3, "graphdb-integration-test")
	ctx := context.Background()
	tenant := fmt.Sprintf("it-tenant-%d", time.Now().UnixNano())
	_, err = store.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:alice", Kind: "person"}},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	loaded, manifest, err := store.Load(ctx, tenant)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version == 0 {
		t.Fatal("manifest was not published")
	}
	if _, ok := loaded.GetEntity("person:alice"); !ok {
		t.Fatal("entity missing after S3 reload")
	}
	key := "graphdb-integration-test/conditional-delete/" + tenant + ".json"
	if err := s3.Put(ctx, key, []byte("old")); err != nil {
		t.Fatalf("put conditional delete seed: %v", err)
	}
	_, oldMeta, err := s3.GetWithMeta(ctx, key)
	if err != nil {
		t.Fatalf("get old meta: %v", err)
	}
	if err := s3.Put(ctx, key, []byte("new")); err != nil {
		t.Fatalf("put conditional delete replacement: %v", err)
	}
	if err := s3.DeleteConditional(ctx, key, PutCondition{IfMatch: oldMeta.ETag}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale conditional delete err = %v, want ErrConflict", err)
	}
	data, err := s3.Get(ctx, key)
	if err != nil || string(data) != "new" {
		t.Fatalf("stale conditional delete changed object, data=%q err=%v", data, err)
	}
	_, newMeta, err := s3.GetWithMeta(ctx, key)
	if err != nil {
		t.Fatalf("get new meta: %v", err)
	}
	if err := s3.DeleteConditional(ctx, key, PutCondition{IfMatch: newMeta.ETag}); !errors.Is(err, ErrConflict) || !errors.Is(err, ErrConditionalDeleteUnsupported) {
		t.Fatalf("matching conditional delete err = %v, want conservative unsupported conflict", err)
	}
	if err := s3.Delete(ctx, key); err != nil {
		t.Fatalf("cleanup delete: %v", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
