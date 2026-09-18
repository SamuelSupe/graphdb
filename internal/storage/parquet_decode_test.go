package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
