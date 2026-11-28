package ha

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestPartitionReceiptExpiryIndexRebuildAfterNotificationOverflow(t *testing.T) {
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	app := &Application{Files: files, Store: storage.NewTenantStore(files, "graphdb"), ShardID: "a"}
	cluster := &Cluster{App: app}
	index, err := openPartitionReceiptExpiryIndex(t.TempDir(), app.managedReceiptPrefix())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { index.db.Close() })
	t.Cleanup(files.OnChange(index.changed))
	ctx := storage.WithFileListPageCache(context.Background())
	finished := time.Now().UTC().Add(time.Hour)
	const ttl = time.Hour
	seed := func(batch string, position uint64, expires time.Time) string {
		t.Helper()
		request := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: batch}
		key := app.ingestKey("big", request.Source, request.CollectorID, request.BatchID, 1)
		record := acceptedRequest{Partitioned: true, Tenant: "big", Generation: 1, Index: position, State: "committed", Request: request,
			FinishedAt: expires.Add(-ttl), ReceiptRetention: ttl, Result: &storage.IngestResult{BatchID: batch}}
		if err := app.saveAccepted(ctx, key, record); err != nil {
			t.Fatal(err)
		}
		return app.managedReceiptKey(key)
	}
	for n := range 2*partitionReceiptPage + 1 {
		seed(fmt.Sprint(n), uint64(n+1), finished.Add(ttl))
	}
	step := func(cursor string) string {
		t.Helper()
		next, err := cluster.indexedPartitionReceiptExpiryStep(ctx, index, cursor)
		if err != nil {
			t.Fatal(err)
		}
		return next
	}
	cursor := step("")
	if cursor == "" || index.ready {
		t.Fatal("expected a partial cold scan")
	}
	for n := range maxReceiptExpiryChanges + 1 {
		index.changed(index.prefix + "dirty/" + fmt.Sprint(n))
	}
	// Use a previously unseen bucket. The overflow makes this receipt rely
	// on the rebuilt directory listing, even if its position is after cursor.
	var late string
	for n := 0; ; n++ {
		batch := fmt.Sprintf("late-%d", n)
		key := app.managedReceiptKey(app.ingestKey("big", "agent", "collector", batch, 1))
		if strings.HasPrefix(key, index.prefix+"ff/") {
			late = seed(batch, 1000, finished)
			break
		}
	}
	for n := 0; cursor != ""; n++ {
		if n >= 16 {
			t.Fatal("receipt directory scan did not finish")
		}
		cursor = step(cursor)
	}
	before, err := index.due(finished.Add(-time.Nanosecond))
	if err != nil || len(before) != 0 {
		t.Fatalf("before deadline: %v %v", before, err)
	}
	want := expiringPartitionReceipt{Key: late, Index: 1000}
	due, err := index.due(finished)
	if err != nil || len(due) != 1 || due[0] != want {
		t.Fatalf("at deadline: %v %v, want %v", due, err, want)
	}
	if err := files.Delete(ctx, late); err != nil {
		t.Fatal(err)
	}
	if cursor := step(""); cursor != "" {
		t.Fatal("warm poll restarted the cold scan")
	}
	due, err = index.due(finished)
	if err != nil || len(due) != 0 {
		t.Fatalf("deleted receipt remains indexed: %v %v", due, err)
	}
}
