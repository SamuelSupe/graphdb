package parquetio

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func StringColumn(record arrow.RecordBatch, index int, name string) (*array.String, error) {
	column, ok := record.Column(index).(*array.String)
	if !ok {
		return nil, fmt.Errorf("parquet entity page column %q has unexpected type", name)
	}
	return column, nil
}

func Int64Column(record arrow.RecordBatch, index int, name string) (*array.Int64, error) {
	column, ok := record.Column(index).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("parquet entity page column %q has unexpected type", name)
	}
	return column, nil
}

func FormatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func ParseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func BoolColumn(record arrow.RecordBatch, index int, name string) (*array.Boolean, error) {
	column, ok := record.Column(index).(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("parquet column %q has unexpected type", name)
	}
	return column, nil
}

func Float64Column(record arrow.RecordBatch, index int, name string) (*array.Float64, error) {
	column, ok := record.Column(index).(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("parquet column %q has unexpected type", name)
	}
	return column, nil
}
