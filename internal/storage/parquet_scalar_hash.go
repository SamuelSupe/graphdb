package storage

import (
	"strconv"
	"strings"
)

func parquetScalarContentHash(parts ...string) string {
	return objectContentHash([]byte(strings.Join(parts, "\x00")))
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
