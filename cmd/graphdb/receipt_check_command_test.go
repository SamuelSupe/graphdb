package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	bolt "go.etcd.io/bbolt"
)

const receiptCheckTestCLI = "GRAPHDB_TEST_RECEIPT_CHECK_CLI"

func TestMain(m *testing.M) {
	if os.Getenv(receiptCheckTestCLI) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestReceiptCheckCLI(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const prefix = "receipt-e2e"
	files, err := storage.OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	if err := files.EnableLocalReceiptPacking(ctx, prefix); err != nil {
		t.Fatal(err)
	}
	store := storage.NewTenantStore(files, prefix)
	if _, err := store.InitTenant(ctx, "clean"); err != nil {
		t.Fatal(err)
	}
	_, err = store.Ingest(ctx, "damaged", storage.IngestRequest{
		Source: "agent", CollectorID: "test", BatchID: "receipt-check",
		Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "host:a", Kind: "host"}}},
	})
	if err := errors.Join(err, files.Close()); err != nil {
		t.Fatal(err)
	}

	check := func(tenant string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "receipt-check", tenant)
		cmd.Env = []string{receiptCheckTestCLI + "=1", "GRAPHDB_DATA_DIR=" + root, "GRAPHDB_PREFIX=" + prefix}
		return cmd.Output()
	}
	assertCount := func(tenant string, count int) {
		t.Helper()
		output, err := check(tenant)
		var report struct {
			TenantID string `json:"tenant_id"`
			Checked  *int   `json:"receipts_checked"`
		}
		if err != nil || json.Unmarshal(output, &report) != nil || report.TenantID != tenant || report.Checked == nil || *report.Checked != count {
			t.Fatalf("unexpected receipt check output for %q: %s %v", tenant, output, err)
		}
	}
	assertCount("clean", 0)

	filename := filepath.Join(root, prefix, "tenants", "damaged", "receipts.v1.db")
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
	assertCount("damaged", 1)

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

	for _, tc := range []struct {
		tenant     string
		wantPrefix string
		want       string
	}{
		{"damaged", "error: " + storage.ErrReceiptStoreUnavailable.Error() + ": damaged: ", "Page expected to be:"},
		{"absent", `error: tenant "absent" directory:`, ""},
	} {
		t.Run(tc.tenant, func(t *testing.T) {
			output, err := check(tc.tenant)
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || len(output) != 0 {
				t.Fatalf("expected a CLI diagnostic and exit 1, got %q %v", output, err)
			}
			diagnostic := strings.TrimSpace(string(exit.Stderr))
			if !strings.HasPrefix(diagnostic, tc.wantPrefix) || !strings.Contains(diagnostic, tc.want) || strings.Contains(diagnostic, "\n") {
				t.Fatalf("unexpected CLI diagnostic: %q", diagnostic)
			}
		})
	}
}

func TestReceiptCheckSubprocessInterrupt(t *testing.T) {
	for _, signal := range []string{"INT", "TERM"} {
		t.Run(signal, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `kill -"$1" "$$"`, "worker", signal)
			_, err := receiptCheckSubprocess(ctx, cmd)
			if !errors.Is(err, context.Canceled) || ctx.Err() != nil {
				t.Fatalf("worker signal should report cancellation before the parent context changes: %v, context=%v", err, ctx.Err())
			}
		})
	}
}

func TestReceiptCheckSubprocessKeepsDiagnosticTail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `printf '%s\n' 'discarded line' 'fatal error: out of memory' 'allocation failed' >&2; exit 2`)
	_, err := receiptCheckSubprocess(ctx, cmd)
	if !errors.Is(err, storage.ErrReceiptStoreUnavailable) || !strings.Contains(err.Error(), "fatal error: out of memory") ||
		!strings.Contains(err.Error(), "allocation failed") || strings.Contains(err.Error(), "discarded line") {
		t.Fatalf("unexpected worker failure: %v", err)
	}
}
