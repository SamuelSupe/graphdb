package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	bolt "go.etcd.io/bbolt"
)

func TestReceiptCheckContainsBoltWorkerPanic(t *testing.T) {
	const rootKey = "GRAPHDB_TEST_RECEIPT_CHECK_ROOT"
	const tenantKey = "GRAPHDB_TEST_RECEIPT_CHECK_TENANT"
	if root := os.Getenv(rootKey); root != "" {
		_, err := storage.CheckLocalReceiptDatabase(context.Background(), root, "graphdb", os.Getenv(tenantKey))
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "graphdb", "tenants", "damaged")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(dir, "receipts.v1.db")
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := db.Info().PageSize
	err = db.Update(func(tx *bolt.Tx) error {
		// The logical receipt scan skips this bucket. Only the bbolt
		// structural walker reaches its page and hits the assertion.
		bucket, err := tx.CreateBucket([]byte("other"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("key"), make([]byte, 2*pageSize))
	})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var page uint64
	err = db.View(func(tx *bolt.Tx) error {
		page = uint64(tx.Bucket([]byte("other")).Root())
		return nil
	})
	if err := errors.Join(err, db.Close()); err != nil || page == 0 {
		t.Fatalf("non-inline bucket fixture: page=%d err=%v", page, err)
	}
	f, err := os.OpenFile(filename, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var badID [8]byte
	binary.NativeEndian.PutUint64(badID[:], page+1)
	_, err = f.WriteAt(badID[:], int64(page)*int64(pageSize))
	if err := errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}

	check := func(tenant string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReceiptCheckContainsBoltWorkerPanic$")
		cmd.Env = append(os.Environ(), rootKey+"="+root, tenantKey+"="+tenant, "GOTRACEBACK=none")
		_, err := receiptCheckSubprocess(ctx, cmd)
		return err
	}
	if err := check("damaged"); !errors.Is(err, storage.ErrReceiptStoreUnavailable) {
		t.Fatalf("corrupt-page worker crash was not isolated: %v", err)
	}
	if err := check("absent"); err == nil || errors.Is(err, storage.ErrReceiptStoreUnavailable) || !strings.Contains(err.Error(), `tenant "absent" directory`) {
		t.Fatalf("ordinary missing-tenant error was misclassified: %v", err)
	}
}
