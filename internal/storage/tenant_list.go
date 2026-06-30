package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

type tenantRegistry struct {
	TenantIDs []string `json:"tenant_ids"`
	UpdatedAt string   `json:"updated_at,omitempty"`
}

func (s *TenantStore) ListTenants(ctx context.Context) ([]string, error) {
	if tenants, ok, err := s.getTenantRegistry(ctx); err != nil {
		return nil, err
	} else if ok {
		return tenants, nil
	}
	return s.listTenantsByPrefix(ctx)
}

func (s *TenantStore) ListManagedTenants(ctx context.Context) ([]string, error) {
	tenants, ok, err := s.getTenantRegistry(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []string{}, nil
	}
	return tenants, nil
}

func (s *TenantStore) ListTenantsIncludingLegacy(ctx context.Context) ([]string, error) {
	managed, _, err := s.getTenantRegistry(ctx)
	if err != nil {
		return nil, err
	}
	legacy, err := s.listTenantsByPrefix(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, tenantID := range managed {
		seen[tenantID] = struct{}{}
	}
	for _, tenantID := range legacy {
		seen[tenantID] = struct{}{}
	}
	tenants := make([]string, 0, len(seen))
	for tenantID := range seen {
		tenants = append(tenants, tenantID)
	}
	sort.Strings(tenants)
	return tenants, nil
}

func (s *TenantStore) RebuildTenantRegistry(ctx context.Context) ([]string, error) {
	tenants, err := s.listTenantsByPrefix(ctx)
	if err != nil {
		return nil, err
	}
	key := s.tenantRegistryKey()
	_, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		meta = ObjectMeta{Key: key}
	} else if err != nil {
		return nil, err
	}
	if err := s.putTenantRegistryWithMeta(ctx, tenantRegistry{TenantIDs: tenants}, meta); err != nil {
		return nil, err
	}
	return tenants, nil
}

func (s *TenantStore) listTenantsByPrefix(ctx context.Context) ([]string, error) {
	prefix := path.Join(s.Prefix, "tenants") + "/"
	objects, err := s.Objects.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, object := range objects {
		rest := strings.TrimPrefix(object.Key, prefix)
		tenantID, _, ok := strings.Cut(rest, "/")
		if !ok || tenantID == "" {
			continue
		}
		if err := ValidateTenantID(tenantID); err != nil {
			continue
		}
		seen[tenantID] = struct{}{}
	}
	tenants := make([]string, 0, len(seen))
	for tenantID := range seen {
		tenants = append(tenants, tenantID)
	}
	sort.Strings(tenants)
	return tenants, nil
}

func (s *TenantStore) addTenantToRegistry(ctx context.Context, tenantID string) error {
	return s.updateTenantRegistry(ctx, func(seen map[string]struct{}) {
		seen[tenantID] = struct{}{}
	})
}

func (s *TenantStore) removeTenantFromRegistry(ctx context.Context, tenantID string) error {
	return s.updateTenantRegistry(ctx, func(seen map[string]struct{}) {
		delete(seen, tenantID)
	})
}

func (s *TenantStore) updateTenantRegistry(ctx context.Context, update func(map[string]struct{})) error {
	key := s.tenantRegistryKey()
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	registry := tenantRegistry{}
	if errors.Is(err, ErrNotFound) {
		meta = ObjectMeta{Key: key}
	} else if err != nil {
		return err
	} else {
		if !isParquetBytes(data) {
			return fmt.Errorf("unsupported tenant registry: only parquet registry is readable")
		}
		registry, err = decodeParquetTenantRegistry(ctx, data)
		if err != nil {
			return err
		}
	}
	seen := map[string]struct{}{}
	for _, tenantID := range registry.TenantIDs {
		if ValidateTenantID(tenantID) == nil {
			seen[tenantID] = struct{}{}
		}
	}
	update(seen)
	tenants := make([]string, 0, len(seen))
	for tenantID := range seen {
		tenants = append(tenants, tenantID)
	}
	sort.Strings(tenants)
	return s.putTenantRegistryWithMeta(ctx, tenantRegistry{TenantIDs: tenants}, meta)
}

func (s *TenantStore) getTenantRegistry(ctx context.Context) ([]string, bool, error) {
	data, _, err := s.Objects.GetWithMeta(ctx, s.tenantRegistryKey())
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !isParquetBytes(data) {
		return nil, false, fmt.Errorf("unsupported tenant registry: only parquet registry is readable")
	}
	registry, err := decodeParquetTenantRegistry(ctx, data)
	if err != nil {
		return nil, false, err
	}
	seen := map[string]struct{}{}
	for _, tenantID := range registry.TenantIDs {
		if ValidateTenantID(tenantID) == nil {
			seen[tenantID] = struct{}{}
		}
	}
	tenants := make([]string, 0, len(seen))
	for tenantID := range seen {
		tenants = append(tenants, tenantID)
	}
	sort.Strings(tenants)
	return tenants, true, nil
}

func (s *TenantStore) putTenantRegistryWithMeta(ctx context.Context, registry tenantRegistry, meta ObjectMeta) error {
	if registry.UpdatedAt == "" {
		registry.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	data, err := marshalParquetTenantRegistry(ctx, registry)
	if err != nil {
		return err
	}
	return s.putBytesWithMeta(ctx, s.tenantRegistryKey(), data, meta)
}
