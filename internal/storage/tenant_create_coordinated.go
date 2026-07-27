package storage

import (
	"context"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) createCoordinatedTenant(
	ctx context.Context,
	tenantID string,
	options TenantCreateOptions,
) (TenantInfo, error) {
	unlock := s.lockTenant(tenantID)
	defer unlock()
	operationCtx, stopLease, err := s.startCoordinatorOperationLease(
		ctx, tenantID, "create",
	)
	if err != nil {
		return TenantInfo{}, err
	}
	defer stopLease()
	ctx = operationCtx

	existing, configured, metadataMeta, err := s.getTenantMetadataWithMeta(
		ctx, tenantID,
	)
	if err != nil {
		return TenantInfo{}, err
	}
	if configured && existing.Status != TenantStatusDeleted {
		if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
			return TenantInfo{}, err
		}
		return s.tenantInfoFromMetadata(ctx, existing, true)
	}

	head, headExists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return TenantInfo{}, err
	}
	if !headExists {
		if _, candidateExists, _, err :=
			s.getCoordinatedTenantCandidate(ctx, tenantID); err != nil {
			return TenantInfo{}, err
		} else if candidateExists {
			return TenantInfo{}, fmt.Errorf(
				"%w: tenant %q has an unfinished lifecycle candidate",
				ErrConflict, tenantID,
			)
		}
	}
	if headExists && head.Status == TenantStatusDisabled {
		return TenantInfo{}, ErrTenantDisabled
	}
	if headExists && head.Status == TenantStatusDeleted {
		residual, err := s.tenantResidualObjectsExist(ctx, tenantID)
		if err != nil {
			return TenantInfo{}, err
		}
		if residual {
			return TenantInfo{}, ErrTenantDeleted
		}
	}

	contextSnapshot, hasContext, err := tenantCreateWriteContext(
		tenantID, options, emptyWriteContext(tenantID),
	)
	if err != nil {
		return TenantInfo{}, err
	}
	if headExists && head.Status == TenantStatusActive {
		if hasContext {
			if err := s.publishTenantCreateWriteContext(
				ctx, tenantID, options,
			); err != nil {
				return TenantInfo{}, err
			}
		}
	} else {
		manifest := Manifest{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			UpdatedAt:     time.Now().UTC(),
		}
		if _, dataMD5, _, emptyErr := newEmptyTenantGraph(); emptyErr == nil {
			manifest.DataMD5 = dataMD5
		}
		var activationContext *WriteContextSnapshot
		if hasContext {
			activationContext = &contextSnapshot
		}
		if _, err := s.putCoordinatedManifest(
			ctx,
			tenantID,
			manifest,
			ObjectMeta{Key: s.manifestKey(tenantID)},
			nil,
			activationContext,
		); err != nil {
			return TenantInfo{}, err
		}
	}
	if hasContext {
		if err := s.mirrorLatestWriteContext(ctx, tenantID); err != nil {
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
	if _, err := s.putTenantMetadataWithMeta(
		ctx, tenantID, metadata, metadataMeta,
	); err != nil {
		return TenantInfo{}, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return TenantInfo{}, err
	}
	return s.tenantInfoFromMetadata(ctx, metadata, true)
}

func (s *TenantStore) publishTenantCreateWriteContext(
	ctx context.Context,
	tenantID string,
	options TenantCreateOptions,
) error {
	for attempt := 0; attempt < s.CoordinatorRetryLimit+1; attempt++ {
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return err
		}
		if head.Status != TenantStatusActive {
			return ErrTenantDeleted
		}
		snapshot, _, err = tenantCreateWriteContext(
			tenantID, options, snapshot,
		)
		if err != nil {
			return err
		}
		_, published, err := s.publishCoordinatedWriteContext(
			ctx, head, snapshot,
		)
		if err != nil {
			return err
		}
		if published {
			return nil
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf(
		"%w: tenant create context for %q changed while publishing",
		ErrWriteConflict,
		tenantID,
	)
}

func tenantCreateWriteContext(
	tenantID string,
	options TenantCreateOptions,
	snapshot WriteContextSnapshot,
) (WriteContextSnapshot, bool, error) {
	changed := false
	if options.Config != nil {
		if err := validateTenantConfig(*options.Config); err != nil {
			return WriteContextSnapshot{}, false, err
		}
		snapshot.TenantConfig = *options.Config
		snapshot.TenantConfigConfigured = true
		changed = true
	}
	if options.SourcePolicy != nil {
		normalized, err := graph.NormalizeSourcePolicy(*options.SourcePolicy)
		if err != nil {
			return WriteContextSnapshot{}, false, err
		}
		snapshot.SourcePolicy = normalized
		snapshot.SourcePolicyConfigured = true
		changed = true
	}
	snapshot.TenantID = tenantID
	return snapshot, changed, nil
}
