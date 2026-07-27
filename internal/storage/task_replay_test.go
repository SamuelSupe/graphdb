package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestReplayTaskCursorFollowsDeadLetterObjectOrder(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	createdAt := time.Date(2026, 7, 24, 3, 0, 0, 0, time.UTC)

	putReplayTaskDeadLetter(
		t, ctx, base, store, "z-older", "host:z", createdAt,
	)
	putReplayTaskDeadLetter(
		t, ctx, base, store, "a-newer", "host:a",
		createdAt.Add(time.Minute),
	)

	firstTask := Task{
		ID:        "replay-first",
		TenantID:  "tenant-a",
		Type:      TaskTypeReplayDeadLetter,
		Status:    TaskStatusRunning,
		Params:    map[string]any{"source": "agent", "limit": 1},
		StartedAt: createdAt,
		UpdatedAt: createdAt,
	}
	first, err := store.replayDeadLettersTask(ctx, firstTask)
	if err != nil {
		t.Fatalf("first replay task: %v", err)
	}
	firstKey := store.deadLetterKey("tenant-a", "agent", "a-newer")
	if first.Resolved != 1 || first.Checkpoint.NextCursor != firstKey {
		t.Fatalf("first replay report = %#v, want cursor %q", first, firstKey)
	}

	secondTask := Task{
		ID:       "replay-second",
		TenantID: "tenant-a",
		Type:     TaskTypeReplayDeadLetter,
		Status:   TaskStatusRunning,
		Params: map[string]any{
			"source": "agent",
			"limit":  1,
			"cursor": first.Checkpoint.NextCursor,
		},
		StartedAt: createdAt.Add(2 * time.Minute),
		UpdatedAt: createdAt.Add(2 * time.Minute),
	}
	second, err := store.replayDeadLettersTask(ctx, secondTask)
	if err != nil {
		t.Fatalf("second replay task: %v", err)
	}
	secondKey := store.deadLetterKey("tenant-a", "agent", "z-older")
	if second.Resolved != 1 || second.Checkpoint.NextCursor != secondKey {
		t.Fatalf(
			"second replay report = %#v, want cursor %q",
			second, secondKey,
		)
	}

	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load replayed graph: %v", err)
	}
	if _, ok := loaded.Entities["host:a"]; !ok {
		t.Fatal("first replayed entity is missing")
	}
	if _, ok := loaded.Entities["host:z"]; !ok {
		t.Fatal("second replayed entity is missing")
	}
}

func TestReplayTaskRetryDoesNotSkipFailedDeadLetter(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &failReplayAttemptStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	createdAt := time.Date(2026, 7, 26, 3, 0, 0, 0, time.UTC)
	putReplayTaskDeadLetter(
		t, ctx, base, store, "retry-me", "host:retry", createdAt,
	)

	firstTask := Task{
		ID:        "replay-failed",
		TenantID:  "tenant-a",
		Type:      TaskTypeReplayDeadLetter,
		Status:    TaskStatusRunning,
		Params:    map[string]any{"source": "agent", "limit": 1},
		StartedAt: createdAt,
		UpdatedAt: createdAt,
	}
	first, err := store.replayDeadLettersTask(ctx, firstTask)
	if err == nil {
		t.Fatal("first replay unexpectedly succeeded")
	}
	if first.Scanned != 1 || first.Failed != 1 {
		t.Fatalf("first replay report = %#v, want one failed record", first)
	}
	if first.Checkpoint.NextCursor != "" || first.Checkpoint.LastKey != "" {
		t.Fatalf(
			"failed replay advanced checkpoint to %#v",
			first.Checkpoint,
		)
	}

	failedTask, err := store.GetTask(ctx, "tenant-a", firstTask.ID)
	if err != nil {
		t.Fatalf("load failed replay task: %v", err)
	}
	retryParams := retryTaskParams(Task{
		Type:       TaskTypeReplayDeadLetter,
		Params:     firstTask.Params,
		Checkpoint: failedTask.Checkpoint,
	})
	if cursor := stringTaskParam(retryParams, "cursor"); cursor != "" {
		t.Fatalf("retry cursor = %q, want failed record to be retried", cursor)
	}

	secondTask := Task{
		ID:        "replay-retry",
		TenantID:  "tenant-a",
		Type:      TaskTypeReplayDeadLetter,
		Status:    TaskStatusRunning,
		Params:    retryParams,
		StartedAt: createdAt.Add(time.Minute),
		UpdatedAt: createdAt.Add(time.Minute),
	}
	second, err := store.replayDeadLettersTask(ctx, secondTask)
	if err != nil {
		t.Fatalf("retry replay task: %v", err)
	}
	if second.Resolved != 1 {
		t.Fatalf("retry replay report = %#v, want one resolved record", second)
	}

	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].Status != "resolved" {
		t.Fatalf("deadletters after retry = %#v, want resolved record", letters)
	}
}

func putReplayTaskDeadLetter(
	t *testing.T,
	ctx context.Context,
	base *MemoryStore,
	store *TenantStore,
	id string,
	entityID string,
	createdAt time.Time,
) {
	t.Helper()
	letter := DeadLetter{
		ID:       id,
		TenantID: "tenant-a",
		Source:   "agent",
		BatchID:  id,
		Request: IngestRequest{
			Source:      "agent",
			CollectorID: "collector-a",
			BatchID:     id,
			Items: []IngestItem{{
				ExternalID: entityID,
				Entity: &graph.Entity{
					ID:   entityID,
					Kind: "host",
				},
			}},
		},
		LastResult: IngestResult{
			BatchID: id,
			Failed:  1,
		},
		Status:    "pending",
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}
	data, err := marshalParquetDeadLetter(ctx, letter)
	if err != nil {
		t.Fatalf("marshal deadletter %q: %v", id, err)
	}
	if err := base.Put(
		ctx, store.deadLetterKey("tenant-a", "agent", id), data,
	); err != nil {
		t.Fatalf("put deadletter %q: %v", id, err)
	}
}

type failReplayAttemptStore struct {
	ObjectStore

	mu              sync.Mutex
	commitFailed    bool
	collectorFailed bool
}

func (s *failReplayAttemptStore) Put(ctx context.Context, key string, data []byte) error {
	if s.shouldFailReplayPut(key) {
		return errors.New("injected replay put failure")
	}
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *failReplayAttemptStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	if s.shouldFailReplayPut(key) {
		return ObjectMeta{}, errors.New("injected replay put failure")
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *failReplayAttemptStore) shouldFailReplayPut(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.Contains(key, "/commits/") && !s.commitFailed {
		s.commitFailed = true
		return true
	}
	if strings.Contains(key, "/ingest/agent/collectors/") && !s.collectorFailed {
		s.collectorFailed = true
		return true
	}
	return false
}
