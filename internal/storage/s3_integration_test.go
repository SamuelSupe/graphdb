package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestS3StoreIntegration(t *testing.T) {
	if os.Getenv("GRAPHDB_MINIO_INTEGRATION") != "1" {
		t.Skip("set GRAPHDB_MINIO_INTEGRATION=1 to run against S3/MinIO")
	}
	pathStyle, err := envBool("S3_PATH_STYLE")
	if err != nil {
		t.Fatal(err)
	}
	s3, err := NewS3StoreWithOptions(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_BUCKET"),
		envOr("S3_REGION", "us-east-1"),
		envOr("S3_ACCESS_KEY_ID", os.Getenv("AWS_ACCESS_KEY_ID")),
		envOr("S3_SECRET_ACCESS_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY")),
		S3Options{PathStyle: pathStyle},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := s3.Probe(context.Background()); err != nil {
		t.Fatalf("probe bucket: %v", err)
	}
	store := NewTenantStore(s3, "graphdb-integration-test")
	ctx := context.Background()
	tenant := fmt.Sprintf("it-tenant-%d", time.Now().UnixNano())
	mutations := graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:alice", Kind: "person"}},
	}
	first, err := store.CommitWithReport(ctx, tenant, mutations, CommitOptions{IdempotencyKey: "commit-1"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	replay, err := store.CommitWithReport(ctx, tenant, mutations, CommitOptions{IdempotencyKey: "commit-1"})
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay.Version != first.Version || !replay.Skipped || !replay.IdempotentReplay {
		t.Fatalf("idempotent replay = %#v, first=%#v", replay, first)
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
	catalog, err := store.RebuildIndexes(ctx, tenant)
	if err != nil {
		t.Fatalf("rebuild indexes with escaped entity ID: %v", err)
	}
	lookup := &PersistedIndexLookup{
		Store: store, TenantID: tenant,
		Version: catalog.Version, Catalog: catalog,
	}
	if entity, ok, err := lookup.GetEntity(ctx, "person:alice", nil); err != nil || !ok || entity.ID != "person:alice" {
		t.Fatalf("indexed entity lookup entity=%#v ok=%v err=%v", entity, ok, err)
	}
	if _, err := store.SetTenantStatus(ctx, tenant, TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if _, err := store.PurgeTenant(ctx, tenant, false); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, err := store.Commit(ctx, tenant, mutations, CommitOptions{}); !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("commit after purge err = %v, want ErrTenantDeleted", err)
	}
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatalf("explicit tenant recreate after purge: %v", err)
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
	deleteErr := s3.DeleteConditional(ctx, key, PutCondition{IfMatch: newMeta.ETag})
	if errors.Is(deleteErr, ErrConditionalDeleteUnsupported) {
		data, err := s3.Get(ctx, key)
		if err != nil || string(data) != "new" {
			t.Fatalf("unsupported conditional delete changed object, data=%q err=%v", data, err)
		}
		if err := s3.Delete(ctx, key); err != nil {
			t.Fatalf("clean up provider without conditional delete: %v", err)
		}
	} else if deleteErr != nil {
		t.Fatalf("matching conditional delete: %v", deleteErr)
	}
	if _, err := s3.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conditionally deleted object err = %v, want ErrNotFound", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return false, nil
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "y", "on":
		return true, nil
	case "0", "false", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean", key)
	}
}
