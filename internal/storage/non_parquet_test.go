package storage

import "encoding/json"

func marshalNonParquetJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}
