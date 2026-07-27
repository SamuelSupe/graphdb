package storage

import (
	"context"
	"strings"
)

func (s *TenantStore) tenantDataExists(ctx context.Context, tenantID string) (bool, error) {
	prefix := s.tenantObjectPrefix(tenantID)
	return objectPrefixMatches(ctx, s.Objects, prefix, func(object ObjectInfo) bool {
		return tenantDataObject(strings.TrimPrefix(object.Key, prefix))
	})
}

func tenantDataObject(relativeKey string) bool {
	return relativeKey != "" &&
		!strings.HasPrefix(relativeKey, "control/") &&
		!strings.HasPrefix(relativeKey, "coordination/") &&
		!strings.HasPrefix(relativeKey, "tasks/")
}
