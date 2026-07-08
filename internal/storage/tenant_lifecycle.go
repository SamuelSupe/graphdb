package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"graphdb/internal/graph"
)

const (
	TenantStatusActive   = "active"
	TenantStatusDisabled = "disabled"
	TenantStatusDeleted  = "deleted"
)

type TenantMetadata struct {
	TenantID    string            `json:"tenant_id"`
	Status      string            `json:"status"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Metadata    map[string]any    `json:"metadata,omitempty"`
	ClonedFrom  string            `json:"cloned_from,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	DisabledAt  time.Time         `json:"disabled_at,omitempty"`
	DeletedAt   time.Time         `json:"deleted_at,omitempty"`
}

type TenantInfo struct {
	TenantMetadata
	ManifestVersion  int64     `json:"manifest_version"`
	SnapshotVersion  int64     `json:"snapshot_version"`
	CommitTailLength int       `json:"commit_tail_length"`
	Exists           bool      `json:"exists"`
	LastUpdatedAt    time.Time `json:"last_updated_at,omitempty"`
}

type TenantCreateOptions struct {
	Name         string              `json:"name,omitempty"`
	Description  string              `json:"description,omitempty"`
	Labels       map[string]string   `json:"labels,omitempty"`
	Metadata     map[string]any      `json:"metadata,omitempty"`
	Config       *TenantConfig       `json:"config,omitempty"`
	SourcePolicy *graph.SourcePolicy `json:"source_policy,omitempty"`
}

type TenantCloneOptions struct {
	TargetTenantID string            `json:"target_tenant_id"`
	Name           string            `json:"name,omitempty"`
	Description    string            `json:"description,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Metadata       map[string]any    `json:"metadata,omitempty"`
}

type TenantPurgeReport struct {
	TenantID    string   `json:"tenant_id"`
	Deleted     int      `json:"deleted"`
	DeletedKeys []string `json:"deleted_keys,omitempty"`
}

func (s *TenantStore) CreateTenant(ctx context.Context, tenantID string, options TenantCreateOptions) (TenantInfo, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantInfo{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	existing, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if configured && existing.Status != TenantStatusDeleted {
		if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
			return TenantInfo{}, err
		}
		return s.tenantInfoFromMetadata(ctx, existing, true)
	}
	manifest, meta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if !meta.Exists {
		manifest = Manifest{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, UpdatedAt: time.Now().UTC()}
		if _, err := s.putManifestMeta(ctx, tenantID, manifest, meta); err != nil {
			return TenantInfo{}, err
		}
	}
	now := time.Now().UTC()
	metadata := TenantMetadata{
		TenantID:    tenantID,
		Status:      TenantStatusActive,
		Name:        options.Name,
		Description: options.Description,
		Labels:      cloneStringMap(options.Labels),
		Metadata:    cloneAnyMap(options.Metadata),
		CreatedAt:   firstTime(existing.CreatedAt, now),
		UpdatedAt:   now,
	}
	if err := s.putTenantMetadata(ctx, tenantID, metadata); err != nil {
		return TenantInfo{}, err
	}
	if err := s.applyTenantCreateTemplates(ctx, tenantID, options); err != nil {
		return TenantInfo{}, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	return s.tenantInfoFromMetadata(ctx, metadata, true)
}

func (s *TenantStore) GetTenantInfo(ctx context.Context, tenantID string) (TenantInfo, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantInfo{}, err
	}
	metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if configured {
		return s.tenantInfoFromMetadata(ctx, metadata, true)
	}
	exists, err := s.tenantPrefixExists(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if !exists {
		return TenantInfo{}, ErrNotFound
	}
	return s.tenantInfoFromMetadata(ctx, legacyTenantMetadata(tenantID), false)
}

func (s *TenantStore) ListTenantInfos(ctx context.Context) ([]TenantInfo, error) {
	tenants, err := s.ListTenants(ctx)
	if err != nil {
		return nil, err
	}
	return s.tenantInfos(ctx, tenants)
}

func (s *TenantStore) ListManagedTenantInfos(ctx context.Context) ([]TenantInfo, error) {
	tenants, err := s.ListManagedTenants(ctx)
	if err != nil {
		return nil, err
	}
	return s.tenantInfos(ctx, tenants)
}

func (s *TenantStore) ListTenantInfosIncludingLegacy(ctx context.Context) ([]TenantInfo, error) {
	tenants, err := s.ListTenantsIncludingLegacy(ctx)
	if err != nil {
		return nil, err
	}
	return s.tenantInfos(ctx, tenants)
}

func (s *TenantStore) tenantInfos(ctx context.Context, tenants []string) ([]TenantInfo, error) {
	items := make([]TenantInfo, 0, len(tenants))
	for _, tenantID := range tenants {
		metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		if !configured {
			metadata = legacyTenantMetadata(tenantID)
		}
		info, err := s.tenantInfoFromMetadata(ctx, metadata, true)
		if err != nil {
			return nil, err
		}
		items = append(items, info)
	}
	return items, nil
}

func (s *TenantStore) UpdateTenantMetadata(ctx context.Context, tenantID string, options TenantCreateOptions) (TenantInfo, error) {
	metadata, err := s.mutableTenantMetadata(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	metadata.Name = options.Name
	metadata.Description = options.Description
	metadata.Labels = cloneStringMap(options.Labels)
	metadata.Metadata = cloneAnyMap(options.Metadata)
	metadata.UpdatedAt = time.Now().UTC()
	if err := s.putTenantMetadata(ctx, tenantID, metadata); err != nil {
		return TenantInfo{}, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	return s.tenantInfoFromMetadata(ctx, metadata, true)
}

func (s *TenantStore) SetTenantStatus(ctx context.Context, tenantID string, status string) (TenantInfo, error) {
	if status != TenantStatusActive && status != TenantStatusDisabled && status != TenantStatusDeleted {
		return TenantInfo{}, fmt.Errorf("unsupported tenant status %q", status)
	}
	metadata, err := s.mutableTenantMetadata(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	now := time.Now().UTC()
	metadata.Status = status
	metadata.UpdatedAt = now
	if status == TenantStatusDisabled {
		metadata.DisabledAt = now
	} else if status == TenantStatusDeleted {
		metadata.DeletedAt = now
	} else {
		metadata.DisabledAt = time.Time{}
		metadata.DeletedAt = time.Time{}
	}
	if err := s.putTenantMetadata(ctx, tenantID, metadata); err != nil {
		return TenantInfo{}, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	return s.tenantInfoFromMetadata(ctx, metadata, true)
}

func (s *TenantStore) PurgeTenant(ctx context.Context, tenantID string, force bool) (TenantPurgeReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantPurgeReport{}, err
	}
	info, err := s.GetTenantInfo(ctx, tenantID)
	if err != nil {
		return TenantPurgeReport{}, err
	}
	if !force && info.Status != TenantStatusDeleted {
		return TenantPurgeReport{}, fmt.Errorf("tenant must be soft deleted before purge")
	}
	objects, err := s.Objects.List(ctx, s.tenantObjectPrefix(tenantID))
	if err != nil {
		return TenantPurgeReport{}, err
	}
	report := TenantPurgeReport{TenantID: tenantID}
	for _, object := range objects {
		if err := s.Objects.Delete(ctx, object.Key); err != nil {
			return report, err
		}
		report.Deleted++
		report.DeletedKeys = append(report.DeletedKeys, object.Key)
	}
	if err := s.removeTenantFromRegistry(ctx, tenantID); err != nil {
		return report, err
	}
	s.deleteWriteCache(tenantID)
	s.deleteCachedTenantMetadata(tenantID)
	return report, nil
}

func (s *TenantStore) CloneTenant(ctx context.Context, sourceTenantID string, options TenantCloneOptions) (TenantInfo, error) {
	if err := ValidateTenantID(sourceTenantID); err != nil {
		return TenantInfo{}, err
	}
	targetTenantID := options.TargetTenantID
	if err := ValidateTenantID(targetTenantID); err != nil {
		return TenantInfo{}, err
	}
	if sourceTenantID == targetTenantID {
		return TenantInfo{}, fmt.Errorf("target tenant must differ from source tenant")
	}
	sourceInfo, err := s.GetTenantInfo(ctx, sourceTenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if sourceInfo.Status == TenantStatusDeleted {
		return TenantInfo{}, ErrTenantDeleted
	}
	if exists, err := s.tenantPrefixExists(ctx, targetTenantID); err != nil {
		return TenantInfo{}, err
	} else if exists {
		return TenantInfo{}, fmt.Errorf("%w: target tenant %q already exists", ErrConflict, targetTenantID)
	}
	g, _, err := s.Load(ctx, sourceTenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	targetLock := s.lockTenant(targetTenantID)
	defer targetLock()
	if err := s.acquireWriterLease(ctx, targetTenantID); err != nil {
		return TenantInfo{}, err
	}
	snapshot := g.Snapshot()
	snapshotKey := ""
	snapshotCatalogKey := ""
	if snapshot.Version > 0 {
		catalog, err := s.putShardedSnapshot(ctx, targetTenantID, snapshot)
		if err != nil {
			return TenantInfo{}, err
		}
		snapshotCatalogKey = catalog.Key
		snapshotKey = s.snapshotKey(targetTenantID, snapshot.Version)
		record := snapshotRecord{LayoutVersion: CurrentObjectLayoutVersion, TenantID: targetTenantID, Snapshot: snapshot}
		if err := s.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, record); err != nil {
			return TenantInfo{}, err
		}
	}
	manifest := Manifest{
		LayoutVersion:      CurrentObjectLayoutVersion,
		TenantID:           targetTenantID,
		Version:            snapshot.Version,
		SnapshotKey:        snapshotKey,
		SnapshotCatalogKey: snapshotCatalogKey,
		SnapshotVersion:    snapshot.Version,
		UpdatedAt:          time.Now().UTC(),
	}
	if _, err := s.putManifestMeta(ctx, targetTenantID, manifest, ObjectMeta{Key: s.manifestKey(targetTenantID)}); err != nil {
		return TenantInfo{}, err
	}
	if err := s.cloneTenantConfigs(ctx, sourceTenantID, targetTenantID); err != nil {
		return TenantInfo{}, err
	}
	now := time.Now().UTC()
	metadata := TenantMetadata{
		TenantID:    targetTenantID,
		Status:      TenantStatusActive,
		Name:        options.Name,
		Description: options.Description,
		Labels:      cloneStringMap(options.Labels),
		Metadata:    cloneAnyMap(options.Metadata),
		ClonedFrom:  sourceTenantID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if metadata.Name == "" {
		metadata.Name = sourceInfo.Name
	}
	if metadata.Description == "" {
		metadata.Description = sourceInfo.Description
	}
	if metadata.Labels == nil {
		metadata.Labels = cloneStringMap(sourceInfo.Labels)
	}
	if metadata.Metadata == nil {
		metadata.Metadata = cloneAnyMap(sourceInfo.Metadata)
	}
	if err := s.putTenantMetadata(ctx, targetTenantID, metadata); err != nil {
		return TenantInfo{}, err
	}
	if err := s.addTenantToRegistry(ctx, targetTenantID); err != nil {
		return TenantInfo{}, err
	}
	return s.tenantInfoFromMetadata(ctx, metadata, true)
}

func (s *TenantStore) EnsureTenantWritable(ctx context.Context, tenantID string) error {
	status, err := s.tenantMetadataStatus(ctx, tenantID)
	if err != nil {
		return err
	}
	switch status {
	case TenantStatusDisabled:
		return ErrTenantDisabled
	case TenantStatusDeleted:
		return ErrTenantDeleted
	default:
		return nil
	}
}

func (s *TenantStore) TenantStatus(ctx context.Context, tenantID string) (string, error) {
	return s.tenantMetadataStatus(ctx, tenantID)
}

func (s *TenantStore) mutableTenantMetadata(ctx context.Context, tenantID string) (TenantMetadata, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantMetadata{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return TenantMetadata{}, err
	}
	metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return TenantMetadata{}, err
	}
	if configured {
		return metadata, nil
	}
	if exists, err := s.tenantPrefixExists(ctx, tenantID); err != nil {
		return TenantMetadata{}, err
	} else if !exists {
		return TenantMetadata{}, ErrNotFound
	}
	return legacyTenantMetadata(tenantID), nil
}

func (s *TenantStore) getTenantMetadataWithMeta(ctx context.Context, tenantID string) (TenantMetadata, bool, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	var metadata TenantMetadata
	key := s.tenantMetadataKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return TenantMetadata{}, false, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return TenantMetadata{}, false, ObjectMeta{}, fmt.Errorf("unsupported tenant metadata: only parquet metadata is readable")
	}
	metadata, err = decodeParquetTenantMetadata(ctx, data)
	if err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	if metadata.TenantID != tenantID {
		return TenantMetadata{}, false, ObjectMeta{}, fmt.Errorf("tenant metadata mismatch: path tenant %q contains tenant %q", tenantID, metadata.TenantID)
	}
	metadata.Status = normalizeTenantStatus(metadata.Status)
	return metadata, true, meta, nil
}

func (s *TenantStore) tenantMetadataStatus(ctx context.Context, tenantID string) (string, error) {
	metadata, configured, _, err := s.getTenantMetadataForWrite(ctx, tenantID)
	if err != nil {
		return "", err
	}
	if !configured {
		return TenantStatusActive, nil
	}
	return metadata.Status, nil
}

func (s *TenantStore) getTenantMetadataForWrite(ctx context.Context, tenantID string) (TenantMetadata, bool, ObjectMeta, error) {
	if metadata, configured, meta, ok := s.getCachedTenantMetadata(tenantID); ok {
		return metadata, configured, meta, nil
	}
	metadata, configured, meta, err := s.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	s.setCachedTenantMetadata(tenantID, metadata, configured, meta)
	return metadata, configured, meta, nil
}

func (s *TenantStore) tenantInfoFromMetadata(ctx context.Context, metadata TenantMetadata, exists bool) (TenantInfo, error) {
	manifest, _, err := s.getManifest(ctx, metadata.TenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	return TenantInfo{
		TenantMetadata:   metadata,
		ManifestVersion:  manifest.Version,
		SnapshotVersion:  manifest.SnapshotVersion,
		CommitTailLength: manifestCommitTailLength(manifest),
		Exists:           exists,
		LastUpdatedAt:    manifest.UpdatedAt,
	}, nil
}

func (s *TenantStore) tenantPrefixExists(ctx context.Context, tenantID string) (bool, error) {
	objects, err := s.Objects.List(ctx, s.tenantObjectPrefix(tenantID))
	if err != nil {
		return false, err
	}
	return len(objects) > 0, nil
}

func (s *TenantStore) cloneTenantConfigs(ctx context.Context, sourceTenantID string, targetTenantID string) error {
	if policy, ok, err := s.GetSourcePolicy(ctx, sourceTenantID); err != nil {
		return err
	} else if ok {
		meta, err := s.putSourcePolicyRecordWithMeta(ctx, targetTenantID, sourcePolicyRecord{TenantID: targetTenantID, SourcePolicy: policy}, ObjectMeta{Key: s.sourcePolicyKey(targetTenantID)})
		if err != nil {
			return err
		}
		s.setCachedSourcePolicy(targetTenantID, policy, true, meta)
	}
	if config, ok, err := s.GetTenantConfig(ctx, sourceTenantID); err != nil {
		return err
	} else if ok {
		meta, err := s.putTenantConfigRecordWithMeta(ctx, targetTenantID, tenantConfigRecord{TenantID: targetTenantID, Config: config}, ObjectMeta{Key: s.tenantConfigKey(targetTenantID)})
		if err != nil {
			return err
		}
		s.setCachedTenantConfig(targetTenantID, config, true, meta)
	}
	return nil
}

func (s *TenantStore) applyTenantCreateTemplates(ctx context.Context, tenantID string, options TenantCreateOptions) error {
	if options.Config != nil {
		if err := validateTenantConfig(*options.Config); err != nil {
			return err
		}
		_, _, meta, err := s.getTenantConfigWithMeta(ctx, tenantID)
		if err != nil {
			return err
		}
		nextMeta, err := s.putTenantConfigRecordWithMeta(ctx, tenantID, tenantConfigRecord{TenantID: tenantID, Config: *options.Config}, meta)
		if err != nil {
			return err
		}
		s.setCachedTenantConfig(tenantID, *options.Config, true, nextMeta)
	}
	if options.SourcePolicy != nil {
		normalized, err := graph.NormalizeSourcePolicy(*options.SourcePolicy)
		if err != nil {
			return err
		}
		_, _, meta, err := s.getSourcePolicyWithMeta(ctx, tenantID)
		if err != nil {
			return err
		}
		nextMeta, err := s.putSourcePolicyRecordWithMeta(ctx, tenantID, sourcePolicyRecord{TenantID: tenantID, SourcePolicy: normalized}, meta)
		if err != nil {
			return err
		}
		s.setCachedSourcePolicy(tenantID, normalized, true, nextMeta)
	}
	return nil
}

func (s *TenantStore) putTenantMetadata(ctx context.Context, tenantID string, metadata TenantMetadata) error {
	metadata.TenantID = tenantID
	data, err := marshalParquetTenantMetadata(ctx, metadata)
	if err != nil {
		return err
	}
	key := s.tenantMetadataKey(tenantID)
	if err := s.Objects.Put(ctx, key, data); err != nil {
		s.deleteCachedTenantMetadata(tenantID)
		return err
	}
	s.setCachedTenantMetadata(tenantID, metadata, true, ObjectMeta{Key: key, Exists: true})
	return nil
}

func legacyTenantMetadata(tenantID string) TenantMetadata {
	return TenantMetadata{TenantID: tenantID, Status: TenantStatusActive}
}

func normalizeTenantStatus(status string) string {
	switch status {
	case TenantStatusDisabled, TenantStatusDeleted:
		return status
	default:
		return TenantStatusActive
	}
}

func firstTime(value time.Time, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
