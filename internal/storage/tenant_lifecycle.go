package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
	TenantID             string   `json:"tenant_id"`
	Deleted              int      `json:"deleted"`
	DeletedKeys          []string `json:"deleted_keys,omitempty"`
	DeletedKeysTruncated bool     `json:"deleted_keys_truncated,omitempty"`
}

func (s *TenantStore) CreateTenant(ctx context.Context, tenantID string, options TenantCreateOptions) (TenantInfo, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantInfo{}, err
	}
	if s.coordinated() {
		return s.createCoordinatedTenant(ctx, tenantID, options)
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.prepareCreateAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	ctx = boundCtx
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
		_, dataMD5, _, err := newEmptyTenantGraph()
		if err != nil {
			return TenantInfo{}, err
		}
		manifest = Manifest{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, UpdatedAt: time.Now().UTC(), DataMD5: dataMD5}
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
	return s.mutateTenantMetadata(ctx, tenantID, func(metadata *TenantMetadata) {
		metadata.Name = options.Name
		metadata.Description = options.Description
		metadata.Labels = cloneStringMap(options.Labels)
		metadata.Metadata = cloneAnyMap(options.Metadata)
		metadata.UpdatedAt = time.Now().UTC()
	})
}

func (s *TenantStore) SetTenantStatus(ctx context.Context, tenantID string, status string) (TenantInfo, error) {
	if status != TenantStatusActive && status != TenantStatusDisabled && status != TenantStatusDeleted {
		return TenantInfo{}, fmt.Errorf("unsupported tenant status %q", status)
	}
	if s.coordinated() {
		return s.setCoordinatedTenantStatus(ctx, tenantID, status)
	}
	if status != TenantStatusActive && s.ingestBarrier != nil {
		if err := s.ingestBarrier(ctx, tenantID); err != nil {
			return TenantInfo{}, err
		}
	}
	info, err := s.mutateTenantMetadata(ctx, tenantID, func(metadata *TenantMetadata) {
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
	})
	if err == nil {
		s.deleteCachedRetrievalSnapshot(tenantID)
	}
	return info, err
}

func (s *TenantStore) PurgeTenant(ctx context.Context, tenantID string, force bool) (TenantPurgeReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantPurgeReport{}, err
	}
	if s.ingestBarrier != nil {
		if err := s.ingestBarrier(ctx, tenantID); err != nil {
			return TenantPurgeReport{}, err
		}
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	var purgeGeneration int64
	if s.coordinated() {
		purgeCtx, generation, candidateOnly, stopLease, err :=
			s.startCoordinatedPurge(
				ctx, tenantID, force,
			)
		if err != nil {
			return TenantPurgeReport{}, err
		}
		defer stopLease()
		ctx = purgeCtx
		purgeGeneration = generation
		s.deleteWriteCache(tenantID)
		s.deleteCachedRetrievalSnapshot(tenantID)
		if candidateOnly {
			return s.purgeCoordinatedTenantCandidate(ctx, tenantID)
		}
	}
	if metadata, exists, _, err := s.getTenantPurgeTombstone(ctx, tenantID); err != nil {
		return TenantPurgeReport{}, err
	} else if phase, _ := tenantPurgeState(metadata, exists); phase == tenantPurgePhaseComplete {
		residual, err := s.tenantResidualObjectsExist(ctx, tenantID)
		if err != nil {
			return TenantPurgeReport{}, err
		}
		if !residual {
			if s.coordinated() {
				if err := s.Coordinator.FinalizeTenantPurge(ctx, tenantID, purgeGeneration); err != nil {
					return TenantPurgeReport{TenantID: tenantID}, err
				}
			}
			return TenantPurgeReport{TenantID: tenantID}, nil
		}
		if err := s.reopenCompletedTenantPurge(ctx, tenantID); err != nil {
			return TenantPurgeReport{}, err
		}
	}
	if err := s.acquireWriterLeaseForPurge(ctx, tenantID); err != nil {
		return TenantPurgeReport{}, err
	}
	report, err := s.purgeTenantLockedAtGeneration(
		ctx, tenantID, force, purgeGeneration,
	)
	if err != nil {
		return report, err
	}
	if s.coordinated() {
		if err := s.Coordinator.FinalizeTenantPurge(ctx, tenantID, purgeGeneration); err != nil {
			return report, err
		}
	}
	return report, nil
}

func (s *TenantStore) purgeTenantLocked(ctx context.Context, tenantID string, force bool) (TenantPurgeReport, error) {
	return s.purgeTenantLockedAtGeneration(ctx, tenantID, force, 0)
}

func (s *TenantStore) purgeTenantLockedAtGeneration(
	ctx context.Context,
	tenantID string,
	force bool,
	purgeGeneration int64,
) (TenantPurgeReport, error) {
	metadata, markerExists, _, err := s.getTenantPurgeTombstone(ctx, tenantID)
	if err != nil {
		return TenantPurgeReport{}, err
	}
	phase, _ := tenantPurgeState(metadata, markerExists)
	if phase == tenantPurgePhaseComplete {
		return TenantPurgeReport{TenantID: tenantID}, nil
	}
	if phase != tenantPurgePhaseRunning {
		info, err := s.GetTenantInfo(ctx, tenantID)
		if err != nil {
			return TenantPurgeReport{}, err
		}
		if !force && info.Status != TenantStatusDeleted {
			return TenantPurgeReport{}, fmt.Errorf("tenant must be soft deleted before purge")
		}
	}
	operationID, alreadyComplete, err := s.beginTenantPurge(
		ctx, tenantID, purgeGeneration > 0,
	)
	if err != nil {
		return TenantPurgeReport{}, err
	}
	if alreadyComplete {
		return TenantPurgeReport{TenantID: tenantID}, nil
	}
	report := TenantPurgeReport{TenantID: tenantID}
	leaseKey := s.writerLeaseKey(tenantID)
	err = scanObjectPrefix(
		ctx,
		s.Objects,
		s.tenantObjectPrefix(tenantID),
		func(objects []ObjectInfo) error {
			if err := s.ensureCoordinatedPurgeCurrent(
				ctx, tenantID, purgeGeneration,
			); err != nil {
				return err
			}
			filtered := objects[:0]
			for _, object := range objects {
				if object.Key != leaseKey {
					filtered = append(filtered, object)
				}
			}
			deletedKeys, err := s.deleteTenantPurgePage(
				ctx, tenantID, filtered, purgeGeneration,
			)
			report.Deleted += len(deletedKeys)
			report.recordDeletedKeys(deletedKeys)
			if err != nil {
				return err
			}
			return s.ensureCoordinatedPurgeCurrent(
				ctx, tenantID, purgeGeneration,
			)
		},
	)
	if err != nil {
		return report, err
	}
	if err := s.ensureCoordinatedPurgeCurrent(
		ctx, tenantID, purgeGeneration,
	); err != nil {
		return report, err
	}
	if err := s.removeTenantFromRegistry(ctx, tenantID); err != nil {
		return report, err
	}
	if err := s.ensureCoordinatedPurgeCurrent(
		ctx, tenantID, purgeGeneration,
	); err != nil {
		return report, err
	}
	if err := s.completeTenantPurge(ctx, tenantID, operationID); err != nil {
		return report, err
	}
	if err := s.releaseWriterLeaseForPurge(ctx, tenantID); err != nil {
		return report, err
	}
	s.deleteWriteCache(tenantID)
	s.deleteCachedRetrievalSnapshot(tenantID)
	s.deleteCachedTenantMetadata(tenantID)
	s.deleteCachedTenantConfig(tenantID)
	s.deleteCachedSourcePolicy(tenantID)
	s.deleteCachedIndexCatalog(tenantID)
	s.deleteCachedRetrievalSnapshot(tenantID)
	s.deleteCachedWriterLease(tenantID)
	s.deleteCachedTenantPurgeTombstone(tenantID)
	s.clearObjectKeyPrefix(s.tenantObjectPrefix(tenantID))
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(s.tenantObjectPrefix(tenantID))
	}
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
	if s.ingestBarrier != nil {
		if err := s.ingestBarrier(ctx, sourceTenantID); err != nil {
			return TenantInfo{}, err
		}
	}
	sourceInfo, err := s.GetTenantInfo(ctx, sourceTenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if sourceInfo.Status == TenantStatusDeleted {
		return TenantInfo{}, ErrTenantDeleted
	}
	_, sourceRecord, dataMD5, err := s.captureTenantBackup(
		ctx, sourceTenantID,
	)
	if err != nil {
		return TenantInfo{}, err
	}
	var activationContext *WriteContextSnapshot
	if s.coordinated() {
		writeContext, err := tenantWriteContextFromBackupRecord(
			sourceRecord, targetTenantID,
		)
		if err != nil {
			return TenantInfo{}, err
		}
		if sourceRecord.Config != nil ||
			sourceRecord.SourcePolicy != nil ||
			len(sourceRecord.RelationSchemas) > 0 {
			activationContext = &writeContext
		}
	}
	targetLock := s.lockTenant(targetTenantID)
	defer targetLock()
	leaseCtx, stopLease, err := s.startCoordinatorOperationLease(ctx, targetTenantID, "clone")
	if err != nil {
		return TenantInfo{}, err
	}
	defer stopLease()
	ctx = leaseCtx
	coordinatedResume := false
	var coordinatedCandidate coordinatedTenantCandidate
	if s.coordinated() {
		coordinatedCandidate = newCoordinatedTenantCandidate(
			"clone",
			sourceTenantID,
			s.tenantObjectPrefix(sourceTenantID),
			targetTenantID,
		)
		coordinatedResume, err = s.prepareCoordinatedCloneTarget(
			ctx,
			targetTenantID,
			coordinatedCandidate,
		)
		if err != nil {
			return TenantInfo{}, err
		}
	} else {
		if exists, err := s.tenantPrefixExists(ctx, targetTenantID); err != nil {
			return TenantInfo{}, err
		} else if exists {
			return TenantInfo{}, fmt.Errorf(
				"%w: target tenant %q already exists",
				ErrConflict, targetTenantID,
			)
		}
		boundCtx, err := s.prepareCreateAndBindWriterFence(ctx, targetTenantID)
		if err != nil {
			return TenantInfo{}, err
		}
		ctx = boundCtx
	}
	snapshot := sourceRecord.Snapshot
	snapshotKey := ""
	snapshotCatalogKey := ""
	if snapshot.Version > 0 && !coordinatedResume {
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
		DataMD5:            dataMD5,
		UpdatedAt:          time.Now().UTC(),
	}
	if s.coordinated() && !coordinatedResume {
		if _, err := s.putCoordinatedManifest(
			ctx,
			targetTenantID,
			manifest,
			ObjectMeta{Key: s.manifestKey(targetTenantID)},
			nil,
			activationContext,
		); err != nil {
			return TenantInfo{}, err
		}
	} else {
		if !s.coordinated() {
			_, targetManifestMeta, err := s.getManifest(ctx, targetTenantID)
			if err != nil {
				return TenantInfo{}, err
			}
			if _, err := s.putManifestMeta(
				ctx, targetTenantID, manifest, targetManifestMeta,
			); err != nil {
				return TenantInfo{}, err
			}
			if err := s.putLocalLifecycleWriteContext(
				ctx, targetTenantID, sourceRecord, snapshot.Version,
			); err != nil {
				return TenantInfo{}, err
			}
		}
	}
	if s.coordinated() && activationContext != nil {
		if err := s.mirrorLatestWriteContext(ctx, targetTenantID); err != nil {
			return TenantInfo{}, err
		}
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
	info, err := s.tenantInfoFromMetadata(ctx, metadata, true)
	if err != nil {
		return TenantInfo{}, err
	}
	if s.coordinated() {
		if err := s.completeCoordinatedTenantCandidate(
			ctx, targetTenantID, coordinatedCandidate,
		); err != nil {
			return TenantInfo{}, err
		}
	}
	return info, nil
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

func (s *TenantStore) setCoordinatedTenantStatus(
	ctx context.Context,
	tenantID string,
	status string,
) (TenantInfo, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantInfo{}, err
	}
	operationCtx, stopLease, err := s.startCoordinatorOperationLease(
		ctx, tenantID, "status",
	)
	if err != nil {
		return TenantInfo{}, err
	}
	defer stopLease()
	ctx = operationCtx
	if _, exists, err := s.Coordinator.Head(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	} else if !exists {
		if _, err := s.ensureCoordinatedTenantHead(ctx, tenantID); err != nil {
			return TenantInfo{}, err
		}
	}
	metadata, configured, _, err := s.getTenantMetadataWithMeta(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if !configured {
		metadata = legacyTenantMetadata(tenantID)
	}
	now := time.Now().UTC()
	metadata.Status = status
	metadata.UpdatedAt = now
	switch status {
	case TenantStatusDisabled:
		metadata.DisabledAt = now
		metadata.DeletedAt = time.Time{}
	case TenantStatusDeleted:
		metadata.DeletedAt = now
	case TenantStatusActive:
		metadata.DisabledAt = time.Time{}
		metadata.DeletedAt = time.Time{}
	}
	head, err := s.Coordinator.TransitionTenant(ctx, tenantID, status, true)
	if err != nil {
		return TenantInfo{}, err
	}
	data, err := marshalParquetTenantMetadata(ctx, metadata)
	if err != nil {
		return TenantInfo{}, err
	}
	if err := s.putLegacyLifecycleMirrorObject(
		ctx, tenantID, s.tenantMetadataKey(tenantID), data, head,
	); err != nil {
		return TenantInfo{}, err
	}
	s.deleteWriteCache(tenantID)
	s.deleteCachedTenantMetadata(tenantID)
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	return s.tenantInfoFromMetadata(ctx, metadata, true)
}

func (s *TenantStore) TenantStatus(ctx context.Context, tenantID string) (string, error) {
	return s.tenantMetadataStatus(ctx, tenantID)
}

func (s *TenantStore) mutateTenantMetadata(ctx context.Context, tenantID string, mutate func(*TenantMetadata)) (TenantInfo, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantInfo{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	ctx = boundCtx
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		metadata, configured, meta, err := s.getTenantMetadataWithMeta(ctx, tenantID)
		if err != nil {
			return TenantInfo{}, err
		}
		if !configured {
			if exists, err := s.tenantPrefixExists(ctx, tenantID); err != nil {
				return TenantInfo{}, err
			} else if !exists {
				return TenantInfo{}, ErrNotFound
			}
			metadata = legacyTenantMetadata(tenantID)
		}
		mutate(&metadata)
		if _, err := s.putTenantMetadataWithMeta(ctx, tenantID, metadata, meta); err != nil {
			s.deleteCachedTenantMetadata(tenantID)
			if errors.Is(err, ErrConflict) && attempt+1 < s.retryCount() {
				if err := retryDelay(ctx, attempt); err != nil {
					return TenantInfo{}, err
				}
				continue
			}
			return TenantInfo{}, err
		}
		if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
			return TenantInfo{}, err
		}
		return s.tenantInfoFromMetadata(ctx, metadata, true)
	}
	return TenantInfo{}, fmt.Errorf("%w: tenant metadata for %q changed while publishing", ErrConflict, tenantID)
}

func (s *TenantStore) getTenantMetadataWithMeta(ctx context.Context, tenantID string) (TenantMetadata, bool, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	var metadata TenantMetadata
	key := s.tenantMetadataKey(tenantID)
	s.clearCoordinatedWriterObjectKey(key)
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
	if s.coordinated() {
		head, exists, err := s.Coordinator.Head(ctx, tenantID)
		if err != nil {
			return "", err
		}
		if !exists {
			return TenantStatusActive, nil
		}
		return head.Status, nil
	}
	purged, err := s.tenantPurgeTombstoneExistsCached(ctx, tenantID)
	if err != nil {
		return "", err
	}
	if purged {
		return TenantStatusDeleted, nil
	}
	metadata, configured, _, err := s.getTenantMetadataForStatus(ctx, tenantID)
	if err != nil {
		return "", err
	}
	if !configured {
		return TenantStatusActive, nil
	}
	return metadata.Status, nil
}

func (s *TenantStore) getTenantMetadataForStatus(ctx context.Context, tenantID string) (TenantMetadata, bool, ObjectMeta, error) {
	if metadata, configured, meta, ok := s.getCachedTenantMetadataFresh(tenantID, time.Now().UTC()); ok {
		return metadata, configured, meta, nil
	}
	metadata, configured, meta, err := s.getTenantMetadataWithMetaFresh(ctx, tenantID)
	if err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	s.setCachedTenantMetadata(tenantID, metadata, configured, meta)
	return metadata, configured, meta, nil
}

func (s *TenantStore) getTenantMetadataWithMetaFresh(ctx context.Context, tenantID string) (TenantMetadata, bool, ObjectMeta, error) {
	s.clearWriterObjectKey(s.tenantMetadataKey(tenantID))
	return s.getTenantMetadataWithMeta(ctx, tenantID)
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
	return s.tenantDataExists(ctx, tenantID)
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
	if err := s.putTenantObject(ctx, tenantID, key, data); err != nil {
		s.deleteCachedTenantMetadata(tenantID)
		return err
	}
	s.setCachedTenantMetadata(tenantID, metadata, true, ObjectMeta{Key: key, Exists: true})
	return nil
}

func (s *TenantStore) putTenantMetadataWithMeta(ctx context.Context, tenantID string, metadata TenantMetadata, meta ObjectMeta) (ObjectMeta, error) {
	metadata.TenantID = tenantID
	data, err := marshalParquetTenantMetadata(ctx, metadata)
	if err != nil {
		return ObjectMeta{}, err
	}
	next, err := s.putTenantBytesWithMetaResult(ctx, tenantID, s.tenantMetadataKey(tenantID), data, meta)
	if err != nil {
		return ObjectMeta{}, err
	}
	s.setCachedTenantMetadata(tenantID, metadata, true, next)
	return next, nil
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
