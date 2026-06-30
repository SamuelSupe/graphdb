package storage

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

func parseScanCursor(raw string, version int64, queryHash string) (scanCursor, error) {
	if raw == "" {
		return scanCursor{}, nil
	}
	cursor, err := decodeScanCursor(raw)
	if err != nil {
		return scanCursor{}, err
	}
	if cursor.Version != 0 && cursor.Version != version {
		return scanCursor{}, fmt.Errorf("cursor version %d does not match current version %d", cursor.Version, version)
	}
	if cursor.Query != "" && cursor.Query != queryHash {
		return scanCursor{}, fmt.Errorf("cursor query does not match request")
	}
	return cursor, nil
}

func scanCursorPinnedCatalog(raw string) (int64, string, bool, error) {
	if raw == "" {
		return 0, "", false, nil
	}
	cursor, err := decodeScanCursor(raw)
	if err != nil {
		return 0, "", false, err
	}
	if cursor.Version <= 0 {
		return 0, "", false, nil
	}
	return cursor.Version, cursor.CatalogHash, true, nil
}

func decodeScanCursor(raw string) (scanCursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return scanCursor{}, fmt.Errorf("invalid cursor")
	}
	var cursor scanCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return scanCursor{}, fmt.Errorf("invalid cursor")
	}
	return cursor, nil
}

func encodeScanCursor(cursor scanCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func entityScanQueryHash(options EntityScanOptions) string {
	options.Cursor = ""
	options.Limit = 0
	options.MinVersion = 0
	return scanQueryHash(options)
}

func edgeScanQueryHash(options EdgeScanOptions) string {
	options.Cursor = ""
	options.Limit = 0
	options.MinVersion = 0
	return scanQueryHash(options)
}

func scanQueryHash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

func normalizedScanLimit(limit int) int {
	if limit <= 0 {
		return defaultScanLimit
	}
	if limit > maxScanLimit {
		return maxScanLimit
	}
	return limit
}

func scanKey(group string, id string) string {
	return group + "\x00" + id
}
