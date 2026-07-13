package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"
)

func (s *TenantStore) tenantPurgeTombstoneKey(tenantID string) string {
	return path.Join(s.Prefix, "control", "tenant-purges", objectSegment(tenantID)+".parquet")
}

func (s *TenantStore) putTenantPurgeTombstone(ctx context.Context, tenantID string) error {
	now := time.Now().UTC()
	record := TenantMetadata{
		TenantID:  tenantID,
		Status:    TenantStatusDeleted,
		UpdatedAt: now,
		DeletedAt: now,
	}
	data, err := marshalParquetTenantMetadata(ctx, record)
	if err != nil {
		return err
	}
	key := s.tenantPurgeTombstoneKey(tenantID)
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	metadata, err := decodeParquetTenantMetadata(ctx, existing)
	if err != nil {
		return fmt.Errorf("decode tenant purge tombstone: %w", err)
	}
	if metadata.TenantID != tenantID || normalizeTenantStatus(metadata.Status) != TenantStatusDeleted {
		return fmt.Errorf("tenant purge tombstone mismatch for %q", tenantID)
	}
	return nil
}

func (s *TenantStore) tenantPurgeTombstoneExists(ctx context.Context, tenantID string) (bool, error) {
	key := s.tenantPurgeTombstoneKey(tenantID)
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(key)
	}
	data, err := s.Objects.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	metadata, err := decodeParquetTenantMetadata(ctx, data)
	if err != nil {
		return false, fmt.Errorf("decode tenant purge tombstone: %w", err)
	}
	if metadata.TenantID != tenantID || normalizeTenantStatus(metadata.Status) != TenantStatusDeleted {
		return false, fmt.Errorf("tenant purge tombstone mismatch for %q", tenantID)
	}
	return true, nil
}

func (s *TenantStore) clearTenantPurgeTombstone(ctx context.Context, tenantID string) error {
	key := s.tenantPurgeTombstoneKey(tenantID)
	_, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	err = s.Objects.DeleteConditional(ctx, key, PutCondition{IfMatch: meta.ETag})
	if errors.Is(err, ErrConditionalDeleteUnsupported) {
		return s.Objects.Delete(ctx, key)
	}
	return err
}
