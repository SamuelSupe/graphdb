package backupstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestManifestCannotRedirectRestore(t *testing.T) {
	requests := 0
	hash := sha256.Sum256([]byte("snapshot"))
	m := Manifest{Format: Format, TenantID: "tenant-a", BackupID: "backup-a", Version: 1, CreatedAt: time.Now().UTC(), Bytes: 8, SHA256: hex.EncodeToString(hash[:]), SnapshotKey: "private/another-tenant.parquet"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++; json.NewEncoder(w).Encode(m) }))
	defer server.Close()
	repo, err := New(context.Background(), Config{Bucket: "test-bucket", Prefix: "backups", Endpoint: server.URL, PathStyle: true, AccessKeyID: "test", SecretAccessKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := repo.URI("tenant-a", "backup-a")
	if _, err := repo.ReadManifest(context.Background(), uri); err == nil {
		t.Fatal("accepted a manifest that points outside its snapshot")
	}
	before := requests
	for _, key := range []string{
		"s3://other-bucket/backups/tenant-a/backup-a/manifest.json",
		"s3://test-bucket/other-prefix/tenant-a/backup-a/manifest.json",
		"s3://test-bucket/backups/tenant-a/../manifest.json",
		uri + "?endpoint=http://example.com", strings.Replace(uri, "tenant-a", "%74enant-a", 1),
	} {
		if _, err := repo.ReadManifest(context.Background(), key); err == nil {
			t.Fatalf("accepted %q", key)
		}
	}
	if requests != before {
		t.Fatal("invalid backup URI triggered network access")
	}
}

func TestObjectBackupMultipartAndPublication(t *testing.T) {
	endpoint := os.Getenv("GRAPHDB_TEST_BACKUP_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set GRAPHDB_TEST_BACKUP_S3_ENDPOINT for S3 integration")
	}
	ctx := context.Background()
	repo, err := New(ctx, Config{
		Bucket: os.Getenv("GRAPHDB_TEST_BACKUP_S3_BUCKET"), Endpoint: endpoint,
		Prefix: fmt.Sprintf("transfer-%d", time.Now().UnixNano()), PathStyle: true,
		AccessKeyID: os.Getenv("GRAPHDB_TEST_BACKUP_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("GRAPHDB_TEST_BACKUP_S3_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var rejectManifest atomic.Bool
	var multipart atomic.Int32
	var cancelUpload atomic.Bool
	uploadCtx, stopUpload := context.WithCancel(ctx)
	defer stopUpload()
	rejectManifest.Store(true)
	options := repo.client.Options()
	base := options.HTTPClient
	options.RetryMaxAttempts = 1
	options.HTTPClient = testHTTPClient(func(request *http.Request) (*http.Response, error) {
		if request.Method == "PUT" && request.URL.Query().Get("partNumber") != "" {
			multipart.Add(1)
			if cancelUpload.Load() {
				stopUpload()
			}
		}
		if request.Method == "PUT" && strings.HasSuffix(request.URL.Path, "/manifest.json") && rejectManifest.Load() {
			return nil, errors.New("injected manifest publication failure")
		}
		return base.Do(request)
	})
	repo.client = s3.New(options)
	data := bytes.Repeat([]byte("0123456789abcdef"), (20<<20)/16)
	reader := func() *io.SectionReader { return io.NewSectionReader(bytes.NewReader(data), 0, int64(len(data))) }
	m := Manifest{TenantID: "tenant-a", BackupID: "backup-a", Version: 7, CreatedAt: time.Now().UTC()}
	if _, err := repo.Publish(ctx, m, reader()); err == nil {
		t.Fatal("manifest failure was ignored")
	}
	page, err := repo.List(ctx, "tenant-a", "", 1)
	if err != nil || len(page.Backups) != 0 {
		t.Fatalf("partial upload became visible: %+v %v", page, err)
	}
	if multipart.Load() < 2 {
		t.Fatalf("upload did not use multipart: %d", multipart.Load())
	}
	rejectManifest.Store(false)
	entry, err := repo.Publish(ctx, m, reader())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Publish(ctx, m, reader()); err != nil {
		t.Fatalf("idempotent publication: %v", err)
	}
	m.Version++
	if _, err := repo.Publish(ctx, m, reader()); err == nil {
		t.Fatal("replaced a published backup")
	}
	var downloaded bytes.Buffer
	if err := repo.Download(ctx, entry.Manifest, &downloaded); err != nil || !bytes.Equal(data, downloaded.Bytes()) {
		t.Fatalf("download differs: %v", err)
	}
	m.BackupID = "backup-b"
	if _, err := repo.Publish(ctx, m, reader()); err != nil {
		t.Fatal(err)
	}
	page, err = repo.List(ctx, "tenant-a", "", 1)
	if err != nil || len(page.Backups) != 1 || page.NextCursor == "" {
		t.Fatalf("first page: %+v %v", page, err)
	}
	next, err := repo.List(ctx, "tenant-a", page.NextCursor, 1)
	if err != nil || len(next.Backups) != 1 || next.Backups[0].BackupID == page.Backups[0].BackupID || next.NextCursor != "" {
		t.Fatalf("next page: %+v %v", next, err)
	}
	if _, err := repo.List(ctx, "tenant-b", page.NextCursor, 1); err == nil {
		t.Fatal("accepted another tenant's cursor")
	}
	cancelUpload.Store(true)
	m.BackupID = "canceled-upload"
	if _, err := repo.Publish(uploadCtx, m, reader()); err == nil {
		t.Fatal("canceled upload succeeded")
	}
	cancelUpload.Store(false)
	canceledURI, _ := repo.URI(m.TenantID, m.BackupID)
	if _, err := repo.ReadManifest(ctx, canceledURI); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled backup published: %v", err)
	}
	uploads, err := repo.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(repo.bucket), Prefix: aws.String(repo.prefix + "/")})
	if err != nil || len(uploads.Uploads) != 0 {
		t.Fatalf("multipart upload leaked: %+v %v", uploads, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := repo.Download(canceled, entry.Manifest, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled download: %v", err)
	}
	// A length-preserving change must fail before a consumer can apply data.
	data[0] ^= 0xff
	_, err = repo.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(repo.bucket), Key: aws.String(entry.SnapshotKey), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Download(ctx, entry.Manifest, io.Discard); err == nil {
		t.Fatal("accepted corrupted snapshot")
	}
}

type testHTTPClient func(*http.Request) (*http.Response, error)

func (f testHTTPClient) Do(r *http.Request) (*http.Response, error) { return f(r) }
