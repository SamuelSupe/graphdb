package storage

import (
	"context"
	"fmt"
	"time"
)

func (s *TenantStore) captureTenantBackup(
	ctx context.Context,
	tenantID string,
) (loadedGraph, TenantBackupRecord, string, error) {
	if s.coordinated() {
		return s.captureCoordinatedTenantBackup(ctx, tenantID)
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	loaded, err := s.loadWithMeta(ctx, tenantID)
	if err != nil {
		return loadedGraph{}, TenantBackupRecord{}, "", err
	}
	record, dataMD5, err := s.tenantBackupRecordFromLocalState(
		ctx, tenantID, loaded,
	)
	return loaded, record, dataMD5, err
}

func (s *TenantStore) captureCoordinatedTenantBackup(
	ctx context.Context,
	tenantID string,
) (loadedGraph, TenantBackupRecord, string, error) {
	for attempt := 0; attempt < s.CoordinatorRetryLimit+1; attempt++ {
		loaded, err := s.loadWithMeta(ctx, tenantID)
		if err != nil {
			return loadedGraph{}, TenantBackupRecord{}, "", err
		}
		token, err := parseCoordinatedHeadToken(loaded.Meta)
		if err != nil {
			return loadedGraph{}, TenantBackupRecord{}, "", err
		}
		metadata, err := s.tenantBackupMetadata(ctx, tenantID)
		if err != nil {
			return loadedGraph{}, TenantBackupRecord{}, "", err
		}
		writeContext, head, err := s.loadCoordinatedWriteContext(
			ctx, tenantID,
		)
		if err != nil {
			return loadedGraph{}, TenantBackupRecord{}, "", err
		}
		if head.Status != TenantStatusActive {
			return loadedGraph{}, TenantBackupRecord{}, "", ErrTenantDeleted
		}
		if !sameCoordinationPoint(head, token) {
			if err := coordinatorRetryDelay(ctx, attempt); err != nil {
				return loadedGraph{}, TenantBackupRecord{}, "", err
			}
			continue
		}
		record := newTenantBackupRecord(tenantID, loaded, metadata)
		if writeContext.TenantConfigConfigured {
			config := writeContext.TenantConfig
			record.Config = &config
		}
		if writeContext.SourcePolicyConfigured {
			policy := writeContext.SourcePolicy
			record.SourcePolicy = &policy
		}
		record.RelationSchemas = append(
			[]RelationSchema(nil),
			writeContext.RelationSchemas.RelationSchemas...,
		)
		dataMD5, err := validateCapturedTenantBackup(
			record, tenantID, loaded,
		)
		if err != nil {
			return loadedGraph{}, TenantBackupRecord{}, "", fmt.Errorf(
				"captured backup is not restorable: %w", err,
			)
		}
		return loaded, record, dataMD5, nil
	}
	return loadedGraph{}, TenantBackupRecord{}, "", fmt.Errorf(
		"%w: tenant %q changed while capturing backup",
		ErrWriteConflict, tenantID,
	)
}

func (s *TenantStore) tenantBackupRecordFromLocalState(
	ctx context.Context,
	tenantID string,
	loaded loadedGraph,
) (TenantBackupRecord, string, error) {
	metadata, err := s.tenantBackupMetadata(ctx, tenantID)
	if err != nil {
		return TenantBackupRecord{}, "", err
	}
	record := newTenantBackupRecord(tenantID, loaded, metadata)
	if config, ok, err := s.GetTenantConfig(ctx, tenantID); err != nil {
		return TenantBackupRecord{}, "", err
	} else if ok {
		record.Config = &config
	}
	if policy, ok, err := s.GetSourcePolicy(ctx, tenantID); err != nil {
		return TenantBackupRecord{}, "", err
	} else if ok {
		record.SourcePolicy = &policy
	}
	schemas, err := s.GetRelationSchemas(ctx, tenantID)
	if err != nil {
		return TenantBackupRecord{}, "", err
	}
	record.RelationSchemas = append(
		[]RelationSchema(nil), schemas.RelationSchemas...,
	)
	dataMD5, err := validateCapturedTenantBackup(
		record, tenantID, loaded,
	)
	if err != nil {
		return TenantBackupRecord{}, "", fmt.Errorf(
			"captured backup is not restorable: %w", err,
		)
	}
	return record, dataMD5, nil
}

func validateCapturedTenantBackup(
	record TenantBackupRecord,
	tenantID string,
	loaded loadedGraph,
) (string, error) {
	writeContext, err := tenantWriteContextFromBackupRecord(
		record, tenantID,
	)
	if err != nil {
		return "", err
	}
	if err := validateRelationSchemaGraph(
		loaded.Graph, writeContext.RelationSchemas,
	); err != nil {
		return "", err
	}
	if loaded.DataMD5 != "" {
		return loaded.DataMD5, nil
	}
	return loaded.Graph.ContentMD5()
}

func (s *TenantStore) tenantBackupMetadata(
	ctx context.Context,
	tenantID string,
) (TenantMetadata, error) {
	metadata, configured, _, err := s.getTenantMetadataWithMeta(
		ctx, tenantID,
	)
	if err != nil {
		return TenantMetadata{}, err
	}
	if !configured {
		metadata = legacyTenantMetadata(tenantID)
	}
	return metadata, nil
}

func newTenantBackupRecord(
	tenantID string,
	loaded loadedGraph,
	metadata TenantMetadata,
) TenantBackupRecord {
	return TenantBackupRecord{
		TenantID:  tenantID,
		Version:   loaded.Manifest.Version,
		CreatedAt: time.Now().UTC(),
		Metadata:  metadata,
		Snapshot:  loaded.Graph.Snapshot(),
	}
}
