package storage

import (
	"net/url"
	"strings"
)

func (s *TenantStore) parquetSecondaryIndexIdentityFromKey(tenantID string, key string) (string, string, bool) {
	version, ok := s.parquetVersionFromKey(tenantID, key)
	if !ok {
		return "", "", false
	}
	prefix := s.parquetVersionPrefix(tenantID, version) + "/fields/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(parts) < 2 {
		return "", "", false
	}
	fieldSegment := parts[1]
	switch {
	case len(parts) == 2 && strings.HasSuffix(fieldSegment, ".parquet"):
		fieldSegment = strings.TrimSuffix(fieldSegment, ".parquet")
	case len(parts) == 4 && parts[2] == "shards" && strings.HasSuffix(parts[3], ".parquet"):
	default:
		return "", "", false
	}
	kind, kindErr := url.PathUnescape(parts[0])
	field, fieldErr := url.PathUnescape(fieldSegment)
	if kindErr != nil || fieldErr != nil || strings.TrimSpace(kind) == "" || strings.TrimSpace(field) == "" {
		return "", "", false
	}
	return kind, field, true
}
