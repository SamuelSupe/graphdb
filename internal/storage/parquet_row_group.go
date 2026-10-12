package storage

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
)

func parquetDataWriterProperties(schema *arrow.Schema) *parquet.WriterProperties {
	options := []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(false),
	}
	// Prefix encoding avoids per-row string dictionary allocations in the wide
	// data schemas. Numeric dictionaries retain compact repeated metadata.
	for _, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.STRING:
			options = append(options, parquet.WithEncodingFor(field.Name, parquet.Encodings.DeltaByteArray))
		case arrow.INT64, arrow.FLOAT64:
			options = append(options, parquet.WithDictionaryFor(field.Name, true))
		}
	}
	return parquet.NewWriterProperties(options...)
}

const maxParquetMetadataRowGroupRows int64 = 1024

func parquetMetadataRowGroupRows(rows int64) int64 {
	if rows < 1 {
		return 1
	}
	return min(rows, maxParquetMetadataRowGroupRows)
}
