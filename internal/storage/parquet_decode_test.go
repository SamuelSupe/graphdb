package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
)

func TestParquetDecodeAdmissionHonorsContext(t *testing.T) {
	ConfigureParquetDecodeMaxConcurrent(1)
	t.Cleanup(func() { ConfigureParquetDecodeMaxConcurrent(0) })

	release, err := acquireParquetDecode(context.Background())
	if err != nil {
		t.Fatalf("acquire first slot: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := acquireParquetDecode(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked acquire error = %v, want context canceled", err)
	}
}

func TestBulkDecodeLeavesControlMetadataCapacityWithinTotalLimit(t *testing.T) {
	data, err := marshalParquetTenantMetadata(context.Background(), TenantMetadata{TenantID: "tenant-a", Status: TenantStatusActive})
	if err != nil {
		t.Fatal(err)
	}
	ConfigureParquetDecodeMaxConcurrent(3)
	t.Cleanup(func() { ConfigureParquetDecodeMaxConcurrent(0) })
	for range 2 {
		release, err := acquireParquetDecode(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(release)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	metadata, err := decodeParquetTenantMetadata(ctx, data)
	if err != nil || metadata.TenantID != "tenant-a" || metadata.Status != TenantStatusActive {
		t.Fatalf("control metadata blocked by bulk decoders: %+v, %v", metadata, err)
	}
	blocked, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if _, err := acquireParquetDecode(blocked); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bulk decoder consumed reserved capacity: %v", err)
	}
	stop()
	control := context.WithValue(context.Background(), parquetControlDecodeKey{}, true)
	release, err := acquireParquetDecode(control)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	blocked, stop = context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	if _, err := decodeParquetTenantMetadata(blocked, data); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("control decoder exceeded configured total concurrency: %v", err)
	}
}

func TestQueuedEntityDecodeLeavesControlReadsAvailable(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	ctx := context.Background()
	if err := files.Put(ctx, "control", []byte("ready")); err != nil {
		t.Fatal(err)
	}
	guarded := NewReadProtectedObjectStore(files, ReadProtectionConfig{MaxConcurrent: 1})
	store := NewTenantStore(guarded, "test")
	page := EntityPageData{TenantID: "tenant-a", Shard: "00", Version: 1, Entities: []graph.Entity{{ID: "host:a", Kind: "host"}}}
	meta, err := store.putParquetEntityPage(ctx, "tenant-a", page)
	if err != nil {
		t.Fatal(err)
	}
	ConfigureParquetDecodeMaxConcurrent(1)
	t.Cleanup(func() { ConfigureParquetDecodeMaxConcurrent(0) })
	release, err := acquireParquetDecode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	release = sync.OnceFunc(release)
	t.Cleanup(release)
	loadCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		_, err := store.loadLocalEntityPage(loadCtx, "tenant-a", 1, EntityPageSpec{Shard: "00", Objects: []IndexObject{{Role: "page", Key: meta.Key}}}, EntityScanOptions{}, scanCursor{})
		done <- err
	}()
	// The decoder is held by this test. Give the queued lookup a chance to
	// open its object, which previously consumed the only read permit.
	time.Sleep(30 * time.Millisecond)
	readCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	data, _, err := guarded.GetWithMeta(readCtx, "control")
	stop()
	release()
	if decodeErr := <-done; decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if err != nil || string(data) != "ready" {
		t.Fatalf("control read behind queued decoder: data=%q error=%v", data, err)
	}
}

func TestSnapshotDecodeFailureReleasesReaderAndAdmission(t *testing.T) {
	fields := graph.Fields{}
	for i := 0; i < 20_000; i++ {
		fields[fmt.Sprintf("field-%05d", i)] = float64(i)
	}
	data, err := marshalParquetSnapshotRecord(context.Background(), snapshotRecord{
		TenantID: "tenant-a", Snapshot: graph.Snapshot{Version: 1, Entities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: fields}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	file, err := pqfile.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if file.NumRowGroups() < 2 {
		t.Fatal("fixture must span multiple row groups")
	}
	column, err := file.MetaData().RowGroup(1).ColumnChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	failAt := column.DataPageOffset()
	file.Close()
	ConfigureParquetDecodeMaxConcurrent(1)
	t.Cleanup(func() { ConfigureParquetDecodeMaxConcurrent(0) })
	readFailure := errors.New("snapshot read failure")
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled-%t", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := &snapshotFailureReader{Reader: bytes.NewReader(data), failAt: failAt, err: readFailure}
			want := readFailure
			if canceled {
				source.cancel = cancel
				want = context.Canceled
			}
			_, err := decodeParquetSnapshotRecordReader(ctx, source)
			if !errors.Is(err, want) {
				t.Fatalf("decode error = %v, want %v", err, want)
			}
			if !source.reached || !source.closed {
				t.Fatalf("failure reached = %v, reader closed = %v", source.reached, source.closed)
			}
			next, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			release, err := acquireParquetDecode(next)
			if err != nil {
				t.Fatalf("decode admission leaked: %v", err)
			}
			release()
		})
	}
}

type snapshotFailureReader struct {
	*bytes.Reader
	failAt  int64
	err     error
	cancel  context.CancelFunc
	reached bool
	closed  bool
}

func (r *snapshotFailureReader) ReadAt(p []byte, off int64) (int, error) {
	if off <= r.failAt && r.failAt < off+int64(len(p)) {
		r.reached = true
		if r.cancel == nil {
			return 0, r.err
		}
		r.cancel()
	}
	return r.Reader.ReadAt(p, off)
}

func (r *snapshotFailureReader) Close() error {
	r.closed = true
	return nil
}
