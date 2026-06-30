package storage

import (
	"context"
	"fmt"
)

func (s *TenantStore) CurrentScanCatalog(ctx context.Context, tenantID string) (IndexCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexCatalog{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, err
	}
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, err
	}
	if catalog.Version <= 0 || catalog.Version > manifest.Version {
		return IndexCatalog{}, ErrNotFound
	}
	return catalog, nil
}

func (s *TenantStore) ListEntitiesFromCatalog(ctx context.Context, tenantID string, catalog IndexCatalog, options EntityScanOptions) (EntityScanResult, error) {
	if err := validateScanCatalog(tenantID, catalog); err != nil {
		return EntityScanResult{}, err
	}
	options.normalize()
	cursor, err := parseScanCursor(options.Cursor, catalog.Version, entityScanQueryHash(options))
	if err != nil {
		return EntityScanResult{}, err
	}
	result, ok, err := s.listEntitiesFromPages(ctx, tenantID, catalog.Version, catalog, options, cursor)
	if err != nil {
		return EntityScanResult{}, err
	}
	if !ok {
		return EntityScanResult{}, ErrNotFound
	}
	return result, nil
}

func (s *TenantStore) ListEdgesFromCatalog(ctx context.Context, tenantID string, catalog IndexCatalog, options EdgeScanOptions) (EdgeScanResult, error) {
	if err := validateScanCatalog(tenantID, catalog); err != nil {
		return EdgeScanResult{}, err
	}
	options.normalize()
	cursor, err := parseScanCursor(options.Cursor, catalog.Version, edgeScanQueryHash(options))
	if err != nil {
		return EdgeScanResult{}, err
	}
	result, ok, err := s.listEdgesFromShards(ctx, tenantID, catalog.Version, catalog, options, cursor)
	if err != nil {
		return EdgeScanResult{}, err
	}
	if !ok {
		return EdgeScanResult{}, ErrNotFound
	}
	return result, nil
}

func validateScanCatalog(tenantID string, catalog IndexCatalog) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if catalog.TenantID != "" && catalog.TenantID != tenantID {
		return fmt.Errorf("index catalog tenant mismatch: path tenant %q contains tenant %q", tenantID, catalog.TenantID)
	}
	if catalog.Version <= 0 {
		return ErrNotFound
	}
	return nil
}

func scanCatalogContentHash(catalog IndexCatalog) string {
	hash, err := indexCatalogContentHash(catalog)
	if err != nil {
		return ""
	}
	return hash
}
