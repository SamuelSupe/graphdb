package storage

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type parquetDecodeAdmission struct {
	mu    sync.RWMutex
	limit chan struct{}
}

var processParquetDecodeAdmission parquetDecodeAdmission

type parquetDecodeTraceKey struct{}

type parquetDecodeTraceStats struct {
	mu         sync.Mutex
	admissions int
	wait       time.Duration
}

func withParquetDecodeTraceStats(ctx context.Context, stats *parquetDecodeTraceStats) context.Context {
	return context.WithValue(ctx, parquetDecodeTraceKey{}, stats)
}

func (s *parquetDecodeTraceStats) recordAdmission(wait time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.admissions++
	s.wait += wait
	s.mu.Unlock()
}

func (s *parquetDecodeTraceStats) snapshot() (int, time.Duration) {
	if s == nil {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admissions, s.wait
}

// ConfigureParquetDecodeMaxConcurrent bounds Arrow/Parquet decoding for this
// process. A non-positive value leaves decoding unbounded.
func ConfigureParquetDecodeMaxConcurrent(maxConcurrent int) {
	var limit chan struct{}
	if maxConcurrent > 0 {
		limit = make(chan struct{}, maxConcurrent)
	}
	processParquetDecodeAdmission.mu.Lock()
	processParquetDecodeAdmission.limit = limit
	processParquetDecodeAdmission.mu.Unlock()
}

func acquireParquetDecode(ctx context.Context) (func(), error) {
	processParquetDecodeAdmission.mu.RLock()
	limit := processParquetDecodeAdmission.limit
	processParquetDecodeAdmission.mu.RUnlock()
	if limit == nil {
		return func() {}, nil
	}
	waitStarted := time.Now()
	select {
	case limit <- struct{}{}:
		if stats, _ := ctx.Value(parquetDecodeTraceKey{}).(*parquetDecodeTraceStats); stats != nil {
			stats.recordAdmission(time.Since(waitStarted))
		}
		return func() { <-limit }, nil
	case <-ctx.Done():
		if stats, _ := ctx.Value(parquetDecodeTraceKey{}).(*parquetDecodeTraceStats); stats != nil {
			stats.recordAdmission(time.Since(waitStarted))
		}
		return nil, ctx.Err()
	}
}

// readParquetTable keeps its admission slot until the caller releases the
// returned table. Arrow owns the large decode buffers for that whole period.
func readParquetTable(ctx context.Context, data []byte) (arrow.Table, func(), error) {
	release, err := acquireParquetDecode(ctx)
	if err != nil {
		return nil, nil, err
	}
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		release()
		return nil, nil, err
	}
	return table, release, nil
}

// readParquetRecordReader holds one admission slot while a streaming reader
// owns its Arrow decode buffers. Callers must release both returned values.
func readParquetRecordReader(ctx context.Context, reader *pqarrow.FileReader, columns []int, rowGroups []int) (pqarrow.RecordReader, func(), error) {
	release, err := acquireParquetDecode(ctx)
	if err != nil {
		return nil, nil, err
	}
	recordReader, err := reader.GetRecordReader(ctx, columns, rowGroups)
	if err != nil {
		release()
		return nil, nil, err
	}
	return recordReader, release, nil
}
