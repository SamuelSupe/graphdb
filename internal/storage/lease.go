package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type WriterLease struct {
	TenantID  string    `json:"tenant_id"`
	OwnerID   string    `json:"owner_id"`
	ExpiresAt time.Time `json:"expires_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *TenantStore) acquireWriterLease(ctx context.Context, tenantID string) error {
	now := time.Now().UTC()
	next := WriterLease{
		TenantID:  tenantID,
		OwnerID:   s.InstanceID,
		UpdatedAt: now,
		ExpiresAt: now.Add(s.leaseTTL()),
	}
	key := s.writerLeaseKey(tenantID)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		current, meta, err := s.getWriterLease(ctx, tenantID, key)
		switch {
		case errors.Is(err, ErrNotFound):
			if _, err := s.putLease(ctx, key, next, ObjectMeta{Key: key}); err == nil {
				return nil
			} else if !errors.Is(err, ErrConflict) {
				return err
			}
		case err != nil:
			return err
		case current.OwnerID == s.InstanceID || current.ExpiresAt.Before(now):
			if _, err := s.putLease(ctx, key, next, meta); err == nil {
				return nil
			} else if !errors.Is(err, ErrConflict) {
				return err
			}
		default:
			return fmt.Errorf("%w: tenant %q lease owner %q until %s", ErrLeaseHeld, tenantID, current.OwnerID, current.ExpiresAt.Format(time.RFC3339))
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: tenant %q lease changed during acquire", ErrConflict, tenantID)
}

func (s *TenantStore) GetWriterLease(ctx context.Context, tenantID string) (WriterLease, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return WriterLease{}, err
	}
	lease, _, err := s.getWriterLease(ctx, tenantID, s.writerLeaseKey(tenantID))
	return lease, err
}

func (s *TenantStore) getWriterLease(ctx context.Context, tenantID string, key string) (WriterLease, ObjectMeta, error) {
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return WriterLease{}, meta, err
	}
	if !isParquetBytes(data) {
		return WriterLease{}, meta, fmt.Errorf("unsupported writer lease: only parquet leases are readable")
	}
	lease, err := decodeParquetWriterLease(ctx, data)
	if err != nil {
		return WriterLease{}, meta, err
	}
	if lease.TenantID != "" && lease.TenantID != tenantID {
		return WriterLease{}, meta, fmt.Errorf("writer lease tenant mismatch: path tenant %q contains tenant %q", tenantID, lease.TenantID)
	}
	if lease.TenantID == "" {
		lease.TenantID = tenantID
	}
	return lease, meta, nil
}

func (s *TenantStore) putLease(ctx context.Context, key string, lease WriterLease, meta ObjectMeta) (ObjectMeta, error) {
	data, err := marshalParquetWriterLease(ctx, lease)
	if err != nil {
		return ObjectMeta{}, err
	}
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	return s.Objects.PutConditional(ctx, key, data, condition)
}

func (s *TenantStore) leaseTTL() time.Duration {
	if s.LeaseTTL <= 0 {
		return 30 * time.Second
	}
	return s.LeaseTTL
}

func (s *TenantStore) retryCount() int {
	if s.MaxRetries < 1 {
		return 1
	}
	return s.MaxRetries
}
