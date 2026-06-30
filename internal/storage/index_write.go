package storage

import (
	"bytes"
	"context"
	"errors"
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

func (s *TenantStore) putBytesIfChangedMeta(ctx context.Context, key string, data []byte) (ObjectMeta, error) {
	existing, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
	}
	if err != nil {
		return ObjectMeta{}, err
	}
	if bytes.Equal(existing, data) {
		return meta, nil
	}
	return s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag})
}
