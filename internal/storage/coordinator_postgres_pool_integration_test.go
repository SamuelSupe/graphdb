package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewPostgresCoordinatorLazyDefersInitialConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	coordinator, err := NewPostgresCoordinatorLazy(
		ctx,
		"postgres://graphdb:graphdb@127.0.0.1:1/graphdb?connect_timeout=1",
		"graphdb_test_lazy",
		"lazy-start",
	)
	if err != nil {
		t.Fatalf("lazy coordinator construction = %v, want no initial connection attempt", err)
	}
	t.Cleanup(coordinator.Close)
	if coordinator.Backend() != CoordinationPostgres || coordinator.Namespace() != "lazy-start" {
		t.Fatalf("lazy coordinator metadata = backend %q namespace %q", coordinator.Backend(), coordinator.Namespace())
	}
}

func TestPostgresCoordinatorCASMissWorksWithSingleConnection(t *testing.T) {
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL config: %v", err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create single-connection pool: %v", err)
	}
	schema := fmt.Sprintf("graphdb_test_%d", time.Now().UnixNano())
	coordinator := &PostgresCoordinator{
		pool: pool, schema: schema, namespace: "single-connection-cas",
	}
	if err := coordinator.Migrate(ctx); err != nil {
		coordinator.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = coordinator.pool.Exec(
			context.Background(),
			`DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`,
		)
		coordinator.Close()
	})
	head := CoordinationHead{
		TenantID: "tenant-a", Generation: 1, Status: TenantStatusActive,
		Revision: 1, GraphVersion: 1,
		ManifestKey: "manifests/one.parquet", ManifestHash: "manifest-one",
	}
	if err := coordinator.BootstrapHead(ctx, head, true); err != nil {
		t.Fatalf("bootstrap head: %v", err)
	}

	t.Run("write context", func(t *testing.T) {
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		current, published, err := coordinator.PublishWriteContext(
			callCtx,
			WriteContextPublishRequest{
				TenantID:           head.TenantID,
				ExpectedRevision:   head.Revision + 1,
				ExpectedGeneration: head.Generation,
				ExpectedContext:    head.WriteContextRevision,
				WriteContextKey:    "contexts/stale.json",
				WriteContextHash:   "stale-context",
			},
		)
		if err != nil {
			t.Fatalf("publish stale write context: %v", err)
		}
		if published || current.Revision != head.Revision {
			t.Fatalf(
				"published=%v current=%#v, want current head",
				published, current,
			)
		}
	})

	t.Run("activate tenant", func(t *testing.T) {
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		current, published, err := coordinator.ActivateTenantHead(
			callCtx,
			HeadPublishRequest{
				TenantID:           head.TenantID,
				ExpectedGeneration: head.Generation,
				GraphVersion:       2,
				ManifestKey:        "manifests/two.parquet",
				ManifestHash:       "manifest-two",
			},
		)
		if err != nil {
			t.Fatalf("activate active tenant: %v", err)
		}
		if published || current.Revision != head.Revision {
			t.Fatalf(
				"published=%v current=%#v, want current head",
				published, current,
			)
		}
	})
}
