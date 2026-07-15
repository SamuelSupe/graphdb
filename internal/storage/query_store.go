package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

type SavedQuery struct {
	TenantID    string        `json:"tenant_id,omitempty"`
	Name        string        `json:"name"`
	Description string        `json:"description,omitempty"`
	Request     query.Request `json:"request"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

func (s *TenantStore) SaveQuery(ctx context.Context, tenantID string, saved SavedQuery) (SavedQuery, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return SavedQuery{}, err
	}
	saved.Name = strings.TrimSpace(saved.Name)
	if saved.Name == "" {
		return SavedQuery{}, errors.New("saved query name is required")
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return SavedQuery{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return SavedQuery{}, err
	}
	existing, exists, meta, err := s.getSavedQueryWithMeta(ctx, tenantID, saved.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return SavedQuery{}, err
	}
	saved.TenantID = tenantID
	now := time.Now().UTC()
	if saved.CreatedAt.IsZero() && exists {
		saved.CreatedAt = existing.CreatedAt
	}
	if saved.CreatedAt.IsZero() {
		saved.CreatedAt = now
	}
	saved.UpdatedAt = now
	if err := s.putSavedQueryWithMeta(ctx, tenantID, saved, meta); err != nil {
		if errors.Is(err, ErrConflict) {
			return SavedQuery{}, fmt.Errorf("%w: saved query %q for tenant %q changed while publishing", ErrConflict, saved.Name, tenantID)
		}
		return SavedQuery{}, err
	}
	return saved, nil
}

func (s *TenantStore) GetSavedQuery(ctx context.Context, tenantID string, name string) (SavedQuery, error) {
	saved, _, _, err := s.getSavedQueryWithMeta(ctx, tenantID, name)
	return saved, err
}

func (s *TenantStore) getSavedQueryWithMeta(ctx context.Context, tenantID string, name string) (SavedQuery, bool, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return SavedQuery{}, false, ObjectMeta{}, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return SavedQuery{}, false, ObjectMeta{}, errors.New("saved query name is required")
	}
	key := s.savedQueryKey(tenantID, name)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return SavedQuery{}, false, ObjectMeta{Key: key}, ErrNotFound
	}
	if err != nil {
		return SavedQuery{}, false, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return SavedQuery{}, false, ObjectMeta{}, fmt.Errorf("unsupported saved query: only parquet queries are readable")
	}
	saved, err := decodeParquetSavedQuery(ctx, data)
	if err != nil {
		return SavedQuery{}, false, ObjectMeta{}, err
	}
	if strings.TrimSpace(saved.Name) != name {
		return SavedQuery{}, false, ObjectMeta{}, fmt.Errorf("saved query identity mismatch for %q", name)
	}
	if saved.TenantID != "" && saved.TenantID != tenantID {
		return SavedQuery{}, false, ObjectMeta{}, fmt.Errorf("saved query tenant mismatch: path tenant %q contains tenant %q", tenantID, saved.TenantID)
	}
	if saved.TenantID == "" {
		saved.TenantID = tenantID
	}
	return saved, true, meta, nil
}

func (s *TenantStore) ListSavedQueries(ctx context.Context, tenantID string) ([]SavedQuery, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	objects, err := s.Objects.List(ctx, s.savedQueryPrefix(tenantID))
	if err != nil {
		return nil, err
	}
	items := make([]SavedQuery, 0, len(objects))
	for _, object := range objects {
		data, err := s.Objects.Get(ctx, object.Key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		if !isParquetBytes(data) {
			continue
		}
		saved, err := decodeParquetSavedQuery(ctx, data)
		if err != nil {
			continue
		}
		saved.Name = strings.TrimSpace(saved.Name)
		if saved.Name == "" || object.Key != s.savedQueryKey(tenantID, saved.Name) {
			continue
		}
		if saved.TenantID != "" && saved.TenantID != tenantID {
			continue
		}
		if saved.TenantID == "" {
			saved.TenantID = tenantID
		}
		items = append(items, saved)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, nil
}

func (s *TenantStore) putSavedQueryWithMeta(ctx context.Context, tenantID string, saved SavedQuery, meta ObjectMeta) error {
	saved.TenantID = tenantID
	data, err := marshalParquetSavedQuery(ctx, saved)
	if err != nil {
		return err
	}
	return s.putTenantBytesWithMeta(ctx, tenantID, s.savedQueryKey(tenantID, saved.Name), data, meta)
}
