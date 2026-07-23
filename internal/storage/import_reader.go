package storage

import (
	"bytes"
	"fmt"
)

type importRecordReader interface {
	Next() (IngestItem, int, bool, error)
}

func newImportRecordReader(format string, data []byte) (importRecordReader, error) {
	switch format {
	case "jsonl":
		return newJSONLImportReader(data), nil
	case "csv":
		return newCSVImportReader(data)
	default:
		return nil, fmt.Errorf("unsupported import format %q", format)
	}
}

func estimatedImportRecords(format string, data []byte) int {
	lines := bytes.Count(data, []byte{'\n'})
	if len(data) > 0 && data[len(data)-1] != '\n' {
		lines++
	}
	if format == "csv" && lines > 0 {
		lines--
	}
	if lines < 1 {
		return 1
	}
	return lines
}
