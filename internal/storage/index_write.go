package storage

import (
	"context"
)

const indexWriteConcurrency = 4

func (s *TenantStore) putBytesWithMeta(ctx context.Context, key string, data []byte, meta ObjectMeta) error {
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	_, err := s.Objects.PutConditional(ctx, key, data, condition)
	return err
}

func (s *TenantStore) putBytesWithMetaResult(ctx context.Context, key string, data []byte, meta ObjectMeta) (ObjectMeta, error) {
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	return s.Objects.PutConditional(ctx, key, data, condition)
}
