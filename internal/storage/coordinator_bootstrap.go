package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type CoordinatorBootstrapTenant struct {
	TenantID         string `json:"tenant_id"`
	GraphVersion     int64  `json:"graph_version"`
	ManifestKey      string `json:"manifest_key,omitempty"`
	ManifestHash     string `json:"manifest_hash,omitempty"`
	WriteContextKey  string `json:"write_context_key,omitempty"`
	WriteContextHash string `json:"write_context_hash,omitempty"`
	AlreadyExists    bool   `json:"already_exists,omitempty"`
}

type CoordinatorBootstrapReport struct {
	DryRun    bool                         `json:"dry_run"`
	Backend   string                       `json:"backend"`
	Namespace string                       `json:"namespace"`
	Tenants   []CoordinatorBootstrapTenant `json:"tenants"`
}

func (s *TenantStore) BootstrapCoordinator(ctx context.Context, coordinator WriteCoordinator, dryRun bool) (CoordinatorBootstrapReport, error) {
	if coordinator == nil || coordinator.Backend() != CoordinationPostgres {
		return CoordinatorBootstrapReport{}, fmt.Errorf("PostgreSQL coordinator is required")
	}
	report := CoordinatorBootstrapReport{
		DryRun:    dryRun,
		Backend:   coordinator.Backend(),
		Namespace: coordinator.Namespace(),
		Tenants:   []CoordinatorBootstrapTenant{},
	}
	tenants, err := s.ListTenantsIncludingLegacy(ctx)
	if err != nil {
		return report, err
	}
	for _, tenantID := range tenants {
		item, err := s.bootstrapCoordinatorTenant(ctx, coordinator, tenantID, dryRun)
		if err != nil {
			return report, err
		}
		report.Tenants = append(report.Tenants, item)
	}
	if !dryRun {
		if err := s.PutCoordinationMarker(ctx, CoordinationPostgres, coordinator.Namespace()); err != nil {
			return report, err
		}
	}
	return report, nil
}

func (s *TenantStore) bootstrapCoordinatorTenant(
	ctx context.Context,
	coordinator WriteCoordinator,
	tenantID string,
	dryRun bool,
) (CoordinatorBootstrapTenant, error) {
	data, _, err := s.Objects.GetWithMeta(ctx, s.manifestKey(tenantID))
	if errors.Is(err, ErrNotFound) {
		return CoordinatorBootstrapTenant{}, fmt.Errorf("bootstrap tenant %q: legacy manifest is missing", tenantID)
	}
	if err != nil {
		return CoordinatorBootstrapTenant{}, err
	}
	if !isParquetBytes(data) {
		return CoordinatorBootstrapTenant{}, fmt.Errorf("bootstrap tenant %q: legacy manifest is not parquet", tenantID)
	}
	manifest, err := decodeParquetManifest(ctx, data)
	if err != nil {
		return CoordinatorBootstrapTenant{}, err
	}
	if manifest.TenantID != tenantID {
		return CoordinatorBootstrapTenant{}, fmt.Errorf("bootstrap tenant %q: manifest contains tenant %q", tenantID, manifest.TenantID)
	}
	hash := objectContentHash(data)
	key := s.coordinatorManifestKey(tenantID, manifest.Version, 1, hash)
	writeContext, contextData, contextKey, contextHash, err := s.bootstrapWriteContext(ctx, tenantID)
	if err != nil {
		return CoordinatorBootstrapTenant{}, err
	}
	item := CoordinatorBootstrapTenant{
		TenantID:         tenantID,
		GraphVersion:     manifest.Version,
		ManifestKey:      key,
		ManifestHash:     hash,
		WriteContextKey:  contextKey,
		WriteContextHash: contextHash,
	}
	if current, exists, headErr := coordinator.Head(ctx, tenantID); headErr != nil {
		return CoordinatorBootstrapTenant{}, headErr
	} else if exists {
		item.AlreadyExists = true
		if current.GraphVersion != manifest.Version || current.ManifestHash != hash {
			return CoordinatorBootstrapTenant{}, fmt.Errorf(
				"bootstrap tenant %q: coordinator head does not match the legacy manifest",
				tenantID,
			)
		}
		item.ManifestKey = current.ManifestKey
		item.ManifestHash = current.ManifestHash
		item.WriteContextKey = current.WriteContextKey
		item.WriteContextHash = current.WriteContextHash
		return item, nil
	}
	if dryRun {
		return item, nil
	}
	if err := s.putImmutableCoordinatorObject(ctx, key, data); err != nil {
		return CoordinatorBootstrapTenant{}, err
	}
	if err := s.putImmutableCoordinatorObject(ctx, contextKey, contextData); err != nil {
		return CoordinatorBootstrapTenant{}, err
	}
	err = coordinator.BootstrapHead(ctx, CoordinationHead{
		TenantID:             tenantID,
		Generation:           1,
		Status:               "active",
		Revision:             1,
		GraphVersion:         manifest.Version,
		ManifestKey:          key,
		ManifestHash:         hash,
		CommitID:             manifest.HeadCommitID,
		WriteContextRevision: writeContext.Revision,
		WriteContextKey:      contextKey,
		WriteContextHash:     contextHash,
		UpdatedAt:            time.Now().UTC(),
	}, true)
	return item, err
}

func (s *TenantStore) bootstrapWriteContext(
	ctx context.Context,
	tenantID string,
) (WriteContextSnapshot, []byte, string, string, error) {
	snapshot := emptyWriteContext(tenantID)
	snapshot.Revision = 1
	snapshot.UpdatedAt = time.Now().UTC()
	policy, policyConfigured, err := s.GetSourcePolicy(ctx, tenantID)
	if err != nil {
		return WriteContextSnapshot{}, nil, "", "", err
	}
	snapshot.SourcePolicy = policy
	snapshot.SourcePolicyConfigured = policyConfigured
	config, configConfigured, err := s.GetTenantConfig(ctx, tenantID)
	if err != nil {
		return WriteContextSnapshot{}, nil, "", "", err
	}
	snapshot.TenantConfig = config
	snapshot.TenantConfigConfigured = configConfigured
	catalog, err := s.GetRelationSchemas(ctx, tenantID)
	if err != nil {
		return WriteContextSnapshot{}, nil, "", "", err
	}
	snapshot.RelationSchemas = catalog
	data, err := json.Marshal(snapshot)
	if err != nil {
		return WriteContextSnapshot{}, nil, "", "", err
	}
	hash := objectContentHash(data)
	key := s.coordinatorWriteContextKey(tenantID, snapshot.Revision, hash)
	return snapshot, data, key, hash, nil
}
