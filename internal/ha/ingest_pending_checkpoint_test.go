package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Count object accesses across a cold application restart, including any
// attempt to enumerate receipts that have already reached a terminal state.
type queueRecoveryReads struct {
	storage.ObjectStore
	history            string
	gets, historyLists int
}

func (s *queueRecoveryReads) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, s.history) {
		s.gets++
	}
	return s.ObjectStore.Get(ctx, key)
}
func (s *queueRecoveryReads) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if prefix == s.history {
		s.historyLists++
	}
	return s.ObjectStore.List(ctx, prefix)
}

func TestPendingRecoveryAcrossLegacyCheckpointSpillAndRollback(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	app := &Application{Files: files, ShardID: "home", Store: storage.NewTenantStore(files, "graphdb")}
	reads := &queueRecoveryReads{ObjectStore: app.Store.Objects, history: app.ingestPrefix()}
	app.Store.Objects = reads
	key := func(i int) string { return app.ingestKey("t", "s", "c", fmt.Sprint(i), 1) }
	record := func(i int, state string) acceptedRequest {
		return acceptedRequest{Partitioned: true, Tenant: "t", Generation: 1, Index: uint64(i + 1), State: state, Request: storage.IngestRequest{Source: "s", CollectorID: "c", BatchID: fmt.Sprint(i)}}
	}
	const history = 100
	// Simulate the original representation, with no membership hint.
	for i := range history + 1 {
		state := "committed"
		if i == history {
			state = "accepted"
		}
		data, err := json.Marshal(record(i, state))
		if err != nil {
			t.Fatal(err)
		}
		if err := files.Put(ctx, key(i), data); err != nil {
			t.Fatal(err)
		}
	}
	cold := func(size, gets, lists int) {
		t.Helper()
		app = &Application{Files: files, ShardID: "home", Store: app.Store}
		reads.gets, reads.historyLists = 0, 0
		q, err := app.pendingSnapshot(ctx)
		if err != nil || len(q) != size || reads.gets != gets || reads.historyLists != lists {
			t.Fatalf("recovery: pending %d, reads %d, history lists %d, err %v", len(q), reads.gets, reads.historyLists, err)
		}
	}
	cold(1, history+1, 1)
	if _, err := app.Apply(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	cold(1, 1, 0)
	apply := func(index uint64, change func(context.Context) error, abort bool) error {
		_, err := files.ApplyReplicated(ctx, index, "queue", time.Unix(int64(index), 0), func(tx context.Context) ([]byte, error) {
			if err := change(tx); err != nil {
				return nil, err
			}
			if err := app.checkpointPending(tx); err != nil {
				return nil, err
			}
			if abort {
				return nil, context.Canceled
			}
			return nil, nil
		})
		return err
	}
	const extra = inlinePendingLimit
	add := func(tx context.Context) error {
		for i := history + 1; i <= history+extra; i++ {
			if err := app.saveAccepted(tx, key(i), record(i, "accepted")); err != nil {
				return err
			}
		}
		return nil
	}
	if err := apply(2, add, true); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cold(1, 1, 0)
	if err := apply(2, add, false); err != nil {
		t.Fatal(err)
	}
	cold(extra+1, extra+1, 0)
	// Completing the oldest acceptance and adding a new one in the same journal
	// leaves the next accepted LSN at the head, including after a cold restart.
	if err := apply(3, func(tx context.Context) error {
		if err := app.saveAccepted(tx, key(history), record(history, "committed")); err != nil {
			return err
		}
		return app.saveAccepted(tx, key(history+extra+1), record(history+extra+1, "accepted"))
	}, false); err != nil {
		t.Fatal(err)
	}
	cold(extra+1, extra+1, 0)
	first, err := app.firstPartitionIngest(ctx, "t", 1)
	if err != nil || first != key(history+1) {
		t.Fatalf("queue head: %s %v", first, err)
	}
	old, err := app.accepted(ctx, key(0))
	if err != nil || old.Index != 1 || old.State != "committed" {
		t.Fatalf("original receipt: %+v %v", old, err)
	}
	// An older writer publishes only canonical receipts and drops the new hint.
	// Surviving membership objects must not resurrect its completed batch.
	_, err = files.ApplyReplicated(ctx, 4, "legacy", time.Unix(4, 0), func(tx context.Context) ([]byte, error) {
		if err := files.SetReplicationIngestQueue(tx, nil); err != nil {
			return nil, err
		}
		data, err := json.Marshal(record(history+1, "committed"))
		if err != nil {
			return nil, err
		}
		return nil, files.Put(tx, key(history+1), data)
	})
	if err != nil {
		t.Fatal(err)
	}
	cold(extra, history+extra+2, 1)
	if err := apply(5, func(tx context.Context) error {
		return app.saveAccepted(tx, key(history+extra+2), record(history+extra+2, "accepted"))
	}, false); err != nil {
		t.Fatal(err)
	}
	cold(extra+1, extra+1, 0)
	t.Run("restore spilled queue onto fresh volume", func(t *testing.T) {
		const budget = 16 << 20
		app.MaxSnapshotBytes = budget
		snapshot, err := app.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		target, err := storage.OpenFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { target.Close() })
		counted := &queueRecoveryReads{ObjectStore: target, history: app.ingestPrefix()}
		restored := &Application{Files: target, ShardID: app.ShardID, MaxSnapshotBytes: budget, Store: storage.NewTenantStore(counted, app.Store.Prefix)}
		if err := restored.Restore(ctx, 5, snapshot); err != nil {
			t.Fatal(err)
		}
		q, err := restored.pendingSnapshot(ctx)
		if err != nil || len(q) != extra+1 || counted.gets != extra+1 || counted.historyLists != 0 {
			t.Fatalf("restored queue: size %d, reads %d, history lists %d, error %v", len(q), counted.gets, counted.historyLists, err)
		}
		first, err := restored.firstPartitionIngest(ctx, "t", 1)
		if err != nil || first != key(history+2) {
			t.Fatalf("restored queue head: %s %v", first, err)
		}
		old, err := restored.accepted(ctx, key(0))
		if err != nil || old.State != "committed" || old.Index != 1 {
			t.Fatalf("restored historical receipt: %+v %v", old, err)
		}
	})
	t.Run("unrelated tenant assignment preserves spilled membership", func(t *testing.T) {
		cmd, err := newCommand("sharding")
		if err != nil {
			t.Fatal(err)
		}
		cmd.Body = []byte(`{"operation":"own","tenant_id":"other","epoch":1}`)
		data, err := json.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		response, err := app.Apply(ctx, 6, data)
		var result httpResult
		if err != nil || json.Unmarshal(response, &result) != nil || result.Status != 200 {
			t.Fatalf("assign other tenant: %s %v", response, err)
		}
		cold(extra+1, extra+1, 0)
	})
}
