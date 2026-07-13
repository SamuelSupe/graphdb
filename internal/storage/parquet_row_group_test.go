package storage

import "testing"

func TestParquetMetadataRowGroupRows(t *testing.T) {
	tests := []struct {
		rows int64
		want int64
	}{
		{rows: 0, want: 1},
		{rows: 1, want: 1},
		{rows: maxParquetMetadataRowGroupRows, want: maxParquetMetadataRowGroupRows},
		{rows: maxParquetMetadataRowGroupRows + 1, want: maxParquetMetadataRowGroupRows},
	}
	for _, test := range tests {
		if got := parquetMetadataRowGroupRows(test.rows); got != test.want {
			t.Fatalf("rows %d: got %d, want %d", test.rows, got, test.want)
		}
	}
}
