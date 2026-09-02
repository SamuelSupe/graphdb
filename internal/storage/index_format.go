package storage

import "strings"

const (
	IndexFormatParquet         = "parquet"
	parquetEntityPageCodec     = "arrow-parquet-entity-page-v1"
	parquetSecondaryIndexCodec = "arrow-parquet-secondary-index-v1"
	parquetEdgeShardCodec      = "arrow-parquet-edge-shard-v1"
)

func specFormat(format string) string {
	return strings.ToLower(strings.TrimSpace(format))
}
