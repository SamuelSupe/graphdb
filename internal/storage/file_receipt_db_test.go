package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestReceiptHandleEvictionAllowsConcurrentReopens(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range maxOpenReceiptDBs/4 + 4 {
				key := fmt.Sprintf("graphdb/tenants/t-%d-%d/ingest/agent/batches/collector/one.parquet", worker, i)
				if err := files.Put(ctx, key, []byte(key)); err != nil {
					t.Errorf("packed write: %v", err)
					return
				}
			}
			for i := range maxOpenReceiptDBs/4 + 4 {
				key := fmt.Sprintf("graphdb/tenants/t-%d-%d/ingest/agent/batches/collector/one.parquet", worker, i)
				if data, err := files.Get(ctx, key); err != nil || string(data) != key {
					t.Errorf("reopen %s: %q %v", key, data, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	if len(files.receiptDBs) > maxOpenReceiptDBs {
		t.Fatalf("idle handle cache grew beyond its limit: %d", len(files.receiptDBs))
	}
}

func TestPackedReceiptPublicationDrainsPendingDirectories(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(root, "not-yet-durable")
	files.runtime.pendingDirectories = map[string]struct{}{pending: {}}
	single := "graphdb/tenants/hot/ingest/agent/batches/collector/one.parquet"
	batch := "graphdb/tenants/hot/ingest/agent/batches/collector/two.parquet"
	alias := "graphdb/tenants/hot/ingest/agent/idempotency/collector/two.parquet"
	data := []byte("outcome")
	if err := files.Put(ctx, single, data); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("single receipt bypassed failed directory sync: %v", err)
	}
	if _, stored, err := files.putNewIngestReceiptPair(ctx, batch, alias, data); stored || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("paired receipt bypassed failed directory sync: stored=%v err=%v", stored, err)
	}
	for _, key := range []string{single, batch, alias} {
		if _, err := files.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("receipt %s is visible despite a failed publication barrier: %v", key, err)
		}
	}
	if err := os.Mkdir(pending, 0700); err != nil {
		t.Fatal(err)
	}
	if _, stored, err := files.putNewIngestReceiptPair(ctx, batch, alias, data); !stored || err != nil {
		t.Fatalf("paired publication did not recover: stored=%v err=%v", stored, err)
	}
	if len(files.runtime.pendingDirectories) != 0 {
		t.Fatal("successful receipt did not drain pending directories")
	}
}

func TestReceiptCreationRecoversStagingFilesButProtectsCanonicalData(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	base := filepath.Join(root, "graphdb", "tenants", "hot")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(base, ".tmp-receipts-interrupted")
	if err := os.WriteFile(temp, []byte("partial bbolt initialization"), 0600); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(root, "graphdb", "tenants", "other")
	if err := os.MkdirAll(other, 0700); err != nil {
		t.Fatal(err)
	}
	truncated := filepath.Join(other, receiptDatabase)
	if err := os.WriteFile(truncated, nil, 0600); err != nil {
		t.Fatal(err)
	}
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(temp); !os.IsNotExist(err) {
		t.Fatalf("abandoned staging file remains: %v", err)
	}
	key := "graphdb/tenants/hot/ingest/agent/batches/collector/one.parquet"
	if err := files.Put(ctx, key, []byte("outcome")); err != nil {
		t.Fatalf("interrupted creation prevented a new receipt: %v", err)
	}
	if info, err := os.Stat(filepath.Join(base, receiptDatabase)); err != nil || info.Size() == 0 {
		t.Fatalf("canonical database was not initialized: %v", err)
	}
	key = "graphdb/tenants/other/ingest/agent/batches/collector/one.parquet"
	if err := files.Put(ctx, key, []byte("outcome")); !errors.Is(err, ErrReceiptStoreUnavailable) {
		t.Fatalf("zero-byte canonical database was mistaken for a new one: %v", err)
	}
	if info, err := os.Stat(truncated); err != nil || info.Size() != 0 {
		t.Fatalf("truncated canonical file was overwritten: %v", err)
	}
}

func TestReceiptReadOnlyHandlePromotesAndCallbackPanicRollsBack(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	key := "graphdb/tenants/hot/ingest/agent/batches/collector/one.parquet"
	dbKey := "graphdb/tenants/hot/" + receiptDatabase
	open := func() *FileStore {
		t.Helper()
		files, err := OpenFileStore(root)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = files.Close() })
		if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
			t.Fatal(err)
		}
		return files
	}
	files := open()
	if err := files.Put(ctx, key, []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files = open()
	if _, err := files.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	if h := files.receiptDBs[dbKey]; h == nil || !h.db.IsReadOnly() {
		t.Fatal("first read opened the receipt database for writing")
	}
	release, err := files.beginOperation(ctx, dbKey)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	err = files.withReceiptDB(ctx, dbKey, true, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			location, _ := files.receipt(key)
			if err := tx.Bucket(receiptBucket).Put([]byte(location.key), []byte("uncommitted")); err != nil {
				return err
			}
			panic("application callback failed")
		})
	})
	if err == nil || errors.Is(err, ErrReceiptStoreUnavailable) {
		t.Fatalf("callback panic was treated as corruption: %v", err)
	}
	if data, err := files.Get(ctx, key); err != nil || string(data) != "original" {
		t.Fatalf("panic failed to roll back: %q %v", data, err)
	}
	if err := files.Put(ctx, key, []byte("new")); err != nil {
		t.Fatalf("callback panic blocked subsequent writes: %v", err)
	}
}

func TestReceiptCheckDetectsLivePageMarkedFree(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if count, err := CheckLocalReceiptDatabase(ctx, root, "graphdb", "absent"); err != nil || count != 0 {
		t.Fatalf("empty tenant check = %d, %v", count, err)
	}
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(files, "graphdb")
	request := ingestEntityRequest("checked", "host:checked")
	if _, err := store.Ingest(ctx, "hot", request); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(root, "graphdb", "tenants", "hot", receiptDatabase)
	db, err := bolt.Open(filename, 0600, &bolt.Options{ReadOnly: true, PreLoadFreelist: true})
	if err != nil {
		t.Fatal(err)
	}
	pageSize := int64(db.Info().PageSize)
	var live, free int64
	if err := db.View(func(tx *bolt.Tx) error {
		live = int64(tx.Bucket(receiptBucket).Root())
		for i := 2; ; i++ {
			page, err := tx.Page(i)
			if err != nil || page == nil {
				return err
			}
			if page.Type == "freelist" && page.Count > 0 {
				free = int64(i)
				return nil
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil || live == 0 || free == 0 {
		t.Fatalf("invalid freelist fixture: live=%d free=%d err=%v", live, free, err)
	}
	file, err := os.OpenFile(filename, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var id [8]byte
	binary.NativeEndian.PutUint64(id[:], uint64(live))
	_, err = file.WriteAt(id[:], free*pageSize+16)
	if err := errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckLocalReceiptDatabase(ctx, root, "graphdb", "hot"); !errors.Is(err, ErrReceiptStoreUnavailable) {
		t.Fatalf("structural checker accepted a live page listed as free: %v", err)
	}
}
