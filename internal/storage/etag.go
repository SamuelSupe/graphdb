package storage

import "strings"

func cleanETag(etag string) string {
	return strings.Trim(etag, `"`)
}

func quoteETag(etag string) string {
	if etag == "" || strings.HasPrefix(etag, `"`) {
		return etag
	}
	return `"` + etag + `"`
}
