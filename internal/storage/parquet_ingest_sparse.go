package storage

import (
	"fmt"
	"math"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

const sparseIngestTenantColumn = "ingest_sparse_v1_tenant_id"

func keepIngestColumn(name string) bool {
	switch name {
	case "tenant_id", "commit_tenant_id", "commit_id", "content_hash", "row_kind":
		return true
	default:
		return false
	}
}

func sparseIngestBatch(batch arrow.RecordBatch) arrow.RecordBatch {
	var fields []arrow.Field
	var columns []arrow.Array
	for index, field := range batch.Schema().Fields() {
		column := batch.Column(index)
		if keepIngestColumn(field.Name) || nonzeroIngestColumn(column) {
			if field.Name == "tenant_id" {
				field.Name = sparseIngestTenantColumn
			}
			fields = append(fields, field)
			columns = append(columns, column)
		}
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, batch.NumRows())
}

func nonzeroIngestColumn(column arrow.Array) bool {
	for row := 0; row < column.Len(); row++ {
		switch values := column.(type) {
		case *array.String:
			if values.Value(row) != "" {
				return true
			}
		case *array.Int64:
			if values.Value(row) != 0 {
				return true
			}
		case *array.Float64:
			if math.Float64bits(values.Value(row)) != 0 {
				return true
			}
		case *array.Boolean:
			if values.Value(row) {
				return true
			}
		default:
			return true
		}
	}
	return false
}

func expandSparseIngestTable(table arrow.Table) (arrow.Table, error) {
	schema := codec.ParquetCommitArrowSchema()
	stored := make(map[string]*arrow.Chunked, table.NumCols())
	for index, field := range table.Schema().Fields() {
		if field.Name == sparseIngestTenantColumn {
			field.Name = "tenant_id"
		}
		matches := schema.FieldIndices(field.Name)
		column := table.Column(index).Data()
		if len(matches) != 1 || !arrow.TypeEqual(schema.Field(matches[0]).Type, field.Type) || stored[field.Name] != nil || column.NullN() != 0 {
			return nil, fmt.Errorf("invalid sparse parquet ingest column %q", field.Name)
		}
		stored[field.Name] = column
	}
	columns := make([]arrow.Column, 0, schema.NumFields())
	defer func() {
		for index := range columns {
			columns[index].Release()
		}
	}()
	// All omitted columns of a given type share a zero-filled buffer.
	zeros := make(map[arrow.Type]*arrow.Chunked)
	defer func() {
		for _, column := range zeros {
			column.Release()
		}
	}()
	for _, field := range schema.Fields() {
		column := stored[field.Name]
		if column == nil {
			if keepIngestColumn(field.Name) {
				return nil, fmt.Errorf("missing sparse parquet ingest column %q", field.Name)
			}
			column = zeros[field.Type.ID()]
			if column == nil {
				values, err := zeroIngestArray(field.Type, int(table.NumRows()))
				if err != nil {
					return nil, err
				}
				column = arrow.NewChunked(field.Type, []arrow.Array{values})
				values.Release()
				zeros[field.Type.ID()] = column
			}
		}
		columns = append(columns, *arrow.NewColumn(field, column))
	}
	return array.NewTable(schema, columns, table.NumRows()), nil
}

func zeroIngestArray(dataType arrow.DataType, length int) (arrow.Array, error) {
	var value scalar.Scalar
	switch dataType.ID() {
	case arrow.STRING:
		text := scalar.NewStringScalar("")
		defer text.Release()
		value = text
	case arrow.INT64:
		value = scalar.NewInt64Scalar(0)
	case arrow.FLOAT64:
		value = scalar.NewFloat64Scalar(0)
	case arrow.BOOL:
		value = scalar.NewBooleanScalar(false)
	default:
		return nil, fmt.Errorf("unsupported sparse parquet ingest type %s", dataType)
	}
	return scalar.MakeArrayFromScalar(value, length, memory.DefaultAllocator)
}
