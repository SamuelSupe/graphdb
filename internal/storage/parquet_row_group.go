package storage

const maxParquetMetadataRowGroupRows int64 = 1024

func parquetMetadataRowGroupRows(rows int64) int64 {
	if rows < 1 {
		return 1
	}
	return min(rows, maxParquetMetadataRowGroupRows)
}
