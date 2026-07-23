package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
)

const maxJSONLRecordBytes = 8 << 20

type jsonlImportReader struct {
	scanner *bufio.Scanner
	line    int
}

func newJSONLImportReader(data []byte) *jsonlImportReader {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64<<10), maxJSONLRecordBytes)
	return &jsonlImportReader{scanner: scanner}
}

func (r *jsonlImportReader) Next() (IngestItem, int, bool, error) {
	for r.scanner.Scan() {
		r.line++
		data := bytes.TrimSpace(r.scanner.Bytes())
		if len(data) == 0 {
			continue
		}
		var item IngestItem
		if err := json.Unmarshal(data, &item); err != nil {
			return IngestItem{}, r.line, true, fmt.Errorf("decode JSONL line %d: %w", r.line, err)
		}
		return item, r.line, true, nil
	}
	if err := r.scanner.Err(); err != nil {
		return IngestItem{}, r.line + 1, false, fmt.Errorf("read JSONL: %w", err)
	}
	return IngestItem{}, 0, false, nil
}
