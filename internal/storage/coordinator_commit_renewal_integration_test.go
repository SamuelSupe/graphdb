package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorCommitReservationRenewsWhileWriting(t *testing.T) {
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("graphdb_test_%d", time.Now().UnixNano())
	coordinator, err := NewPostgresCoordinator(ctx, dsn, schema, "commit-renewal")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Migrate(ctx); err != nil {
		coordinator.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = coordinator.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		coordinator.Close()
	})

	objects := &blockingCommitPutStore{
		ObjectStore: NewMemoryStore(),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	defer objects.unblock()
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	store.CoordinatorPendingTTL = 500 * time.Millisecond
	mutations := graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}
	options := CommitOptions{IdempotencyKey: "slow-commit"}
	result := make(chan error, 1)
	go func() {
		_, err := store.Commit(ctx, "tenant-a", mutations, options)
		result <- err
	}()

	select {
	case <-objects.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("commit did not reach the blocked object write")
	}
	time.Sleep(1200 * time.Millisecond)
	requestJSON, err := json.Marshal(directCommitRequest(mutations, options))
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.ReserveCommit(
		ctx,
		"tenant-a",
		options.IdempotencyKey,
		objectContentHash(requestJSON),
		"contending-owner",
		store.CoordinatorPendingTTL,
	)
	if !errors.Is(err, ErrIdempotencyInProgress) {
		t.Fatalf("reservation after multiple TTLs = %v, want ErrIdempotencyInProgress", err)
	}

	objects.unblock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("slow commit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("slow commit did not finish")
	}
	replay, err := store.CommitWithReport(ctx, "tenant-a", mutations, options)
	if err != nil {
		t.Fatalf("replay slow commit: %v", err)
	}
	if !replay.IdempotentReplay || replay.Version != 1 {
		t.Fatalf("replay = %#v, want committed version 1", replay)
	}
}

type blockingCommitPutStore struct {
	ObjectStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingCommitPutStore) Put(
	ctx context.Context,
	key string,
	data []byte,
) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *blockingCommitPutStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	block := false
	if strings.Contains(key, "/commits/") {
		s.once.Do(func() {
			block = true
			close(s.entered)
		})
	}
	if block {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ObjectMeta{}, ctx.Err()
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *blockingCommitPutStore) unblock() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}
