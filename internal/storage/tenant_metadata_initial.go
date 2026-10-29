package storage

import "context"

// Stage graph objects only after an empty head exists, so a failed first
// commit remains retryable instead of resembling a lost published manifest.
func (s *TenantStore) persistInitialTenantHead(ctx context.Context, tenant string, loaded *loadedGraph) error {
	if loaded.Meta.Exists {
		return nil
	}
	initial := loaded.Manifest
	initial.LayoutVersion = CurrentObjectLayoutVersion
	var err error
	initial.DataHash, err = loaded.Graph.ContentHash()
	if err != nil {
		return err
	}
	loaded.Meta, err = s.putManifestMeta(ctx, tenant, initial, loaded.Meta)
	return err
}

// Initial graph publication must not succeed without management state. This
// runs only for a missing head, so ordinary commits add no metadata IO. Unlike
// read-side recovery, an explicit first write can finish a pending initial
// metadata write even when its manifest has not yet been published.
func (s *TenantStore) ensureInitialTenantMetadata(ctx context.Context, tenant string) error {
	var metadata TenantMetadata
	var meta ObjectMeta
	var configured, repair bool
	if files := s.recoverableMetadataFiles(ctx); files != nil && files.Exclusive() {
		unlock, err := s.lockLocalTenantMetadata(ctx, tenant)
		if err != nil {
			return err
		}
		primary, candidate, _, needsRepair, err := s.localTenantMetadataCandidate(ctx, tenant)
		unlock()
		if err != nil {
			return err
		}
		metadata, configured, meta, repair = candidate.metadata, candidate.valid, primary.meta, needsRepair
	} else {
		var err error
		metadata, configured, meta, err = s.getTenantMetadataWithMeta(ctx, tenant)
		if err != nil {
			return err
		}
	}
	if configured {
		switch metadata.Status {
		case TenantStatusDisabled:
			return ErrTenantDisabled
		case TenantStatusDeleted:
			return ErrTenantDeleted
		}
		if !repair {
			return nil
		}
	} else {
		now := mutationTime(ctx)
		metadata = TenantMetadata{TenantID: tenant, Status: TenantStatusActive, CreatedAt: now, UpdatedAt: now}
	}
	if purged, err := s.tenantPurgeTombstoneExists(ctx, tenant); err != nil {
		return err
	} else if purged {
		return ErrTenantDeleted
	}
	_, err := s.putTenantMetadataWithMeta(ctx, tenant, metadata, meta)
	return err
}
