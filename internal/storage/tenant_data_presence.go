package storage

import (
	"context"
	"strings"
)

func (s *TenantStore) tenantDataExists(ctx context.Context, tenantID string) (bool, error) {
	objects, err := s.Objects.List(ctx, s.tenantObjectPrefix(tenantID))
	if err != nil {
		return false, err
	}
	prefix := s.tenantObjectPrefix(tenantID)
	for _, object := range objects {
		if tenantDataObject(strings.TrimPrefix(object.Key, prefix)) {
			return true, nil
		}
	}
	return false, nil
}

func tenantDataObject(relativeKey string) bool {
	return relativeKey != "" &&
		!strings.HasPrefix(relativeKey, "control/") &&
		!strings.HasPrefix(relativeKey, "coordination/") &&
		!strings.HasPrefix(relativeKey, "tasks/")
}
