package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"

type (
	parquetDecodeTraceStats = parquetio.DecodeTraceStats
	parquetValue            = parquetio.Value
)

const (
	maxParquetMetadataRowGroupRows = parquetio.MaxParquetMetadataRowGroupRows
	parquetValueKindString         = parquetio.ParquetValueKindString
)

// ConfigureParquetDecodeMaxConcurrent bounds process-wide Arrow/Parquet decoding.
// Limits above one reserve one slot for control metadata; non-positive is unbounded.
func ConfigureParquetDecodeMaxConcurrent(maxConcurrent int) {
	parquetio.ConfigureParquetDecodeMaxConcurrent(maxConcurrent)
}
