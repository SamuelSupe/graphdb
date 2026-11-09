// Package parquetio owns Arrow I/O helpers and process-wide decode admission.
package parquetio

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type parquetDecodeAdmission struct {
	mu    sync.RWMutex
	limit chan struct{}
	data  chan struct{}
}

var processParquetDecodeAdmission parquetDecodeAdmission

type parquetDecodeAdmissionKey struct{}

type parquetControlDecodeKey struct{}

type parquetDecodeTraceKey struct{}

type DecodeTraceStats struct {
	mu         sync.Mutex
	admissions int
	wait       time.Duration
}

func WithDecodeTraceStats(ctx context.Context, stats *DecodeTraceStats) context.Context {
	return context.WithValue(ctx, parquetDecodeTraceKey{}, stats)
}

func (s *DecodeTraceStats) recordAdmission(wait time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.admissions++
	s.wait += wait
	s.mu.Unlock()
}

func (s *DecodeTraceStats) Snapshot() (int, time.Duration) {
	if s == nil {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admissions, s.wait
}

// ConfigureParquetDecodeMaxConcurrent bounds Arrow/Parquet decoding for this
// process. Limits above one reserve capacity for control metadata and publication
// loads; bulk decoding uses at most one fewer slot. Non-positive is unbounded.
func ConfigureParquetDecodeMaxConcurrent(maxConcurrent int) {
	var limit, data chan struct{}
	if maxConcurrent > 0 {
		limit = make(chan struct{}, maxConcurrent)
		if maxConcurrent > 1 {
			data = make(chan struct{}, maxConcurrent-1)
		}
	}
	processParquetDecodeAdmission.mu.Lock()
	processParquetDecodeAdmission.limit = limit
	processParquetDecodeAdmission.data = data
	processParquetDecodeAdmission.mu.Unlock()
}

func AcquireDecode(ctx context.Context) (func(), error) {
	if admitted, _ := ctx.Value(parquetDecodeAdmissionKey{}).(bool); admitted {
		return func() {}, nil
	}
	processParquetDecodeAdmission.mu.RLock()
	limit := processParquetDecodeAdmission.limit
	data := processParquetDecodeAdmission.data
	processParquetDecodeAdmission.mu.RUnlock()
	if limit == nil {
		return func() {}, nil
	}
	waitStarted := time.Now()
	// Bulk pages must leave room for lifecycle, lease and publication metadata.
	// Both classes still share the configured total concurrency limit.
	control, _ := ctx.Value(parquetControlDecodeKey{}).(bool)
	if data != nil && !control {
		select {
		case data <- struct{}{}:
		case <-ctx.Done():
			if stats, _ := ctx.Value(parquetDecodeTraceKey{}).(*DecodeTraceStats); stats != nil {
				stats.recordAdmission(time.Since(waitStarted))
			}
			return nil, ctx.Err()
		}
	} else {
		data = nil
	}
	select {
	case limit <- struct{}{}:
		if stats, _ := ctx.Value(parquetDecodeTraceKey{}).(*DecodeTraceStats); stats != nil {
			stats.recordAdmission(time.Since(waitStarted))
		}
		return func() {
			<-limit
			if data != nil {
				<-data
			}
		}, nil
	case <-ctx.Done():
		if data != nil {
			<-data
		}
		if stats, _ := ctx.Value(parquetDecodeTraceKey{}).(*DecodeTraceStats); stats != nil {
			stats.recordAdmission(time.Since(waitStarted))
		}
		return nil, ctx.Err()
	}
}

func ReadControlTable(ctx context.Context, data []byte) (arrow.Table, func(), error) {
	return ReadTable(WithControlDecode(ctx), data)
}

// ReadTable keeps its admission slot until the caller releases the
// returned table. Arrow owns the large decode buffers for that whole period.
func ReadTable(ctx context.Context, data []byte) (arrow.Table, func(), error) {
	return ReadTableReader(ctx, bytes.NewReader(data))
}

func ReadTableReader(ctx context.Context, source parquet.ReaderAtSeeker) (arrow.Table, func(), error) {
	release, err := AcquireDecode(ctx)
	if err != nil {
		return nil, nil, err
	}
	table, err := pqarrow.ReadTable(ctx, source, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		release()
		return nil, nil, err
	}
	return table, release, nil
}

// ReadRecordReader holds one admission slot while a streaming reader
// owns its Arrow decode buffers. Callers must release both returned values.
func ReadRecordReader(ctx context.Context, reader *pqarrow.FileReader, columns []int, rowGroups []int) (pqarrow.RecordReader, func(), error) {
	release, err := AcquireDecode(ctx)
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

// WithControlDecode allows metadata loads to use the reserved control capacity.
func WithControlDecode(ctx context.Context) context.Context {
	return context.WithValue(ctx, parquetControlDecodeKey{}, true)
}

// WithDecodeAdmission reuses a slot already held by an enclosing decode.
// The enclosing caller must retain that slot until all nested decoding finishes.
func WithDecodeAdmission(ctx context.Context) context.Context {
	return context.WithValue(ctx, parquetDecodeAdmissionKey{}, true)
}
