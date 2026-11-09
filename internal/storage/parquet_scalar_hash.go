package storage

import (
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
)

func parquetScalarContentHash(parts ...string) string {
	return codec.ContentHash([]byte(strings.Join(parts, "\x00")))
}

func formatInt64ForHash(value int64) string {
	return strconv.FormatInt(value, 10)
}

func formatBoolForHash(value bool) string {
	return strconv.FormatBool(value)
}

func formatFloat64ForHash(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}
