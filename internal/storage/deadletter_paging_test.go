package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDeadLetterReplaySelectionIsPagedAndBounded(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	paged := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(paged, "test")
	createdAt := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)

	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("batch-%02d", 8-i)
		status := "pending"
		if i == 1 {
			status = "resolved"
		}
		letter := DeadLetter{
			ID:       id,
			TenantID: "tenant-a",
			Source:   "agent",
			BatchID:  id,
			Request: IngestRequest{
				Source:  "agent",
				BatchID: id,
			},
			LastResult: IngestResult{
				BatchID: id,
				Failed:  1,
			},
			Status:    status,
			CreatedAt: createdAt.Add(time.Duration(i) * time.Minute),
			UpdatedAt: createdAt.Add(time.Duration(i) * time.Minute),
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

	letters, err := store.deadLettersForReplay(
		ctx, "tenant-a", "agent", 3,
	)
	if err != nil {
		t.Fatalf("select deadletters: %v", err)
	}
	if len(letters) != 3 {
		t.Fatalf("selected %d deadletters, want 3", len(letters))
	}
	got := []string{letters[0].ID, letters[1].ID, letters[2].ID}
	want := []string{"batch-08", "batch-06", "batch-05"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selected ids = %v, want %v", got, want)
		}
	}
	if paged.listCalls != 0 || paged.pageCalls == 0 {
		t.Fatalf(
			"list calls=%d page calls=%d, want no unbounded list",
			paged.listCalls, paged.pageCalls,
		)
	}
}

func TestListDeadLettersUsesPagedObjectScan(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	paged := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(paged, "test")
	putPendingDeadLetterForTest(
		t, ctx, store, "tenant-a", "agent", "batch-1",
	)

	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list deadletters: %v", err)
	}
	if len(letters) != 1 || letters[0].ID != "batch-1" {
		t.Fatalf("deadletters = %#v", letters)
	}
	if paged.listCalls != 0 || paged.pageCalls == 0 {
		t.Fatalf(
			"list calls=%d page calls=%d, want no unbounded list",
			paged.listCalls, paged.pageCalls,
		)
	}
}

func TestDeadLetterLocalScanListsOnceAndHonorsCursor(t *testing.T) {
	ctx := context.Background()
	objects := &listOnlyCountingStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	for i := 0; i < 1005; i++ {
		key := store.deadLetterKey("tenant-a", "agent", fmt.Sprintf("%04d", i))
		objects.items = append(objects.items, ObjectInfo{Key: key})
		if err := objects.Put(ctx, key, []byte("damaged record")); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	if err := store.walkDeadLettersByKey(ctx, "tenant-a", "agent", "", func(DeadLetter) (bool, error) {
		count++
		return true, nil
	}); err != nil || count != 1005 || objects.calls != 1 {
		t.Fatalf("count=%d lists=%d err=%v", count, objects.calls, err)
	}
	count = 0
	after := store.deadLetterKey("tenant-a", "agent", "1001")
	if err := store.walkDeadLettersByKey(ctx, "tenant-a", "agent", after, func(item DeadLetter) (bool, error) {
		if item.ID <= "1001" {
			t.Fatalf("cursor returned old ID %s", item.ID)
		}
		count++
		return count < 2, nil
	}); err != nil || count != 2 || objects.calls != 2 {
		t.Fatalf("cursor count=%d lists=%d err=%v", count, objects.calls, err)
	}
}
