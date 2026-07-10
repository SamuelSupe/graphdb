package storage

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

var sharedParquetEntityRecordFileSchema = mustParquetEntityRecordFileSchema()

func mustParquetEntityRecordFileSchema() *schema.Schema {
	result, err := pqarrow.ToParquet(
		parquetEntityRecordArrowSchema(),
		parquetEntityRecordWriterProperties,
		parquetEntityRecordArrowProperties,
	)
	if err != nil {
		panic(err)
	}
	return result
}

func writeParquetEntityRecordBatch(output io.Writer, batch arrow.RecordBatch) error {
	writer := file.NewParquetWriter(
		output,
		sharedParquetEntityRecordFileSchema.Root(),
		file.WithWriterProps(parquetEntityRecordWriterProperties),
	)
	rowGroup := writer.AppendRowGroup()
	for column := 0; column < int(batch.NumCols()); column++ {
		columnWriter, err := rowGroup.NextColumn()
		if err != nil {
			_ = writer.Close()
			return err
		}
		if err := writeParquetEntityRecordColumn(columnWriter, batch.Column(column)); err != nil {
			_ = writer.Close()
			return fmt.Errorf("write entity record column %d: %w", column, err)
		}
	}
	if err := rowGroup.Close(); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func writeParquetEntityRecordColumn(writer file.ColumnChunkWriter, values arrow.Array) error {
	if values.NullN() != 0 {
		return fmt.Errorf("unexpected null values")
	}
	switch typed := values.(type) {
	case *array.String:
		column, ok := writer.(*file.ByteArrayColumnChunkWriter)
		if !ok {
			return fmt.Errorf("unexpected string writer %T", writer)
		}
		offsets := typed.ValueOffsets()
		data := typed.ValueBytes()
		base := offsets[0]
		encoded := make([]parquet.ByteArray, typed.Len())
		for i := range encoded {
			encoded[i] = data[offsets[i]-base : offsets[i+1]-base]
		}
		_, err := column.WriteBatch(encoded, nil, nil)
		return err
	case *array.Int64:
		column, ok := writer.(*file.Int64ColumnChunkWriter)
		if !ok {
			return fmt.Errorf("unexpected int64 writer %T", writer)
		}
		_, err := column.WriteBatch(typed.Int64Values(), nil, nil)
		return err
	case *array.Float64:
		column, ok := writer.(*file.Float64ColumnChunkWriter)
		if !ok {
			return fmt.Errorf("unexpected float64 writer %T", writer)
		}
		_, err := column.WriteBatch(typed.Float64Values(), nil, nil)
		return err
	case *array.Boolean:
		column, ok := writer.(*file.BooleanColumnChunkWriter)
		if !ok {
			return fmt.Errorf("unexpected bool writer %T", writer)
		}
		encoded := make([]bool, typed.Len())
		for i := range encoded {
			encoded[i] = typed.Value(i)
		}
		_, err := column.WriteBatch(encoded, nil, nil)
		return err
	default:
		return fmt.Errorf("unsupported array type %T", values)
	}
}
