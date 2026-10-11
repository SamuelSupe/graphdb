package storage

import "strings"

const (
	ObjectProviderGenericS3 = "generic-s3"

	WriterTopologyCAS    = "cas"
	WriterTopologySingle = "single"

	BucketVersioningDisabled = "disabled"
)

func NormalizeObjectProvider(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ObjectProviderGenericS3
	}
	return value
}
