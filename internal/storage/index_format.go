package storage

import (
	"fmt"
	"strings"
)

const (
	IndexFormatParquet         = "parquet"
	parquetEntityPageCodec     = "arrow-parquet-entity-page-v1"
	parquetSecondaryIndexCodec = "arrow-parquet-secondary-index-v1"
	parquetEdgeShardCodec      = "arrow-parquet-edge-shard-v1"
)

type IndexRebuildOptions struct {
	Format string
}

func normalizeIndexFormat(format string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "":
		return IndexFormatParquet, nil
	case IndexFormatParquet:
		return IndexFormatParquet, nil
	default:
		return "", fmt.Errorf("unsupported index format %q", format)
	}
}

func NormalizeIndexFormatForConfig(format string) (string, error) {
	return normalizeIndexFormat(format)
}

func (s *TenantStore) effectiveIndexFormat(format string) (string, error) {
	if strings.TrimSpace(format) == "" && s != nil {
		format = s.IndexFormat
	}
	return normalizeIndexFormat(format)
}

func specFormat(format string) string {
	return strings.ToLower(strings.TrimSpace(format))
}

func catalogUsesParquet(catalog IndexCatalog) bool {
	for _, index := range catalog.Indexes {
		if specFormat(index.Format) == IndexFormatParquet {
			return true
		}
	}
	for _, shard := range catalog.EdgeShards {
		if specFormat(shard.Format) == IndexFormatParquet {
			return true
		}
	}
	for _, page := range catalog.EntityPages {
		if specFormat(page.Format) == IndexFormatParquet {
			return true
		}
	}
	return false
}
