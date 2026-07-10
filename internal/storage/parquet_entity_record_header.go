package storage

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
)

type parquetEntityRecordHeader struct {
	PageHash string
	PageETag string
	Version  int64
}

func readParquetEntityRecordHeader(data []byte) (parquetEntityRecordHeader, error) {
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return parquetEntityRecordHeader{}, err
	}
	defer reader.Close()
	if reader.NumRowGroups() == 0 {
		return parquetEntityRecordHeader{}, fmt.Errorf("parquet entity record is empty")
	}
	rowGroup := reader.RowGroup(0)
	pageHash, err := readFirstParquetString(rowGroup, parquetEntityRecordColumnPageHash)
	if err != nil {
		return parquetEntityRecordHeader{}, err
	}
	pageETag, err := readFirstParquetString(rowGroup, parquetEntityRecordColumnPageETag)
	if err != nil {
		return parquetEntityRecordHeader{}, err
	}
	version, err := readFirstParquetInt64(rowGroup, parquetEntityRecordColumnVersion)
	if err != nil {
		return parquetEntityRecordHeader{}, err
	}
	return parquetEntityRecordHeader{PageHash: pageHash, PageETag: pageETag, Version: version}, nil
}

func readFirstParquetString(rowGroup *file.RowGroupReader, column int) (string, error) {
	reader, err := rowGroup.Column(column)
	if err != nil {
		return "", err
	}
	typed, ok := reader.(*file.ByteArrayColumnChunkReader)
	if !ok {
		return "", fmt.Errorf("parquet entity record column %d is not a string", column)
	}
	values := make([]parquet.ByteArray, 1)
	rowsRead, valuesRead, err := typed.ReadBatch(1, values, nil, nil)
	if err != nil {
		return "", err
	}
	if rowsRead != 1 || valuesRead != 1 {
		return "", fmt.Errorf("parquet entity record column %d is empty", column)
	}
	return string(values[0]), nil
}

func readFirstParquetInt64(rowGroup *file.RowGroupReader, column int) (int64, error) {
	reader, err := rowGroup.Column(column)
	if err != nil {
		return 0, err
	}
	typed, ok := reader.(*file.Int64ColumnChunkReader)
	if !ok {
		return 0, fmt.Errorf("parquet entity record column %d is not int64", column)
	}
	values := make([]int64, 1)
	rowsRead, valuesRead, err := typed.ReadBatch(1, values, nil, nil)
	if err != nil {
		return 0, err
	}
	if rowsRead != 1 || valuesRead != 1 {
		return 0, fmt.Errorf("parquet entity record column %d is empty", column)
	}
	return values[0], nil
}
