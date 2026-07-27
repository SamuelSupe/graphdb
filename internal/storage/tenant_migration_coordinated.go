package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const coordinatorMigrationTaskType = coordinatorLifecycleTaskType

type tenantMigrationContext struct {
	config          TenantConfig
	hasConfig       bool
	sourcePolicy    sourcePolicyRecord
	hasSourcePolicy bool
	relationSchemas []RelationSchema
}

func copyTenantObjectsCoordinated(
	ctx context.Context,
	source *TenantStore,
	sourceTenantID string,
	target *TenantStore,
	targetTenantID string,
	options TenantMigrationOptions,
	report TenantMigrationReport,
) (TenantMigrationReport, error) {
	leaseCtx, stopLease, err := target.startCoordinatorOperationLease(
		ctx, targetTenantID, coordinatorMigrationTaskType,
	)
	if err != nil {
		return report, err
	}
	defer stopLease()
	ctx = leaseCtx

	sourceManifest, writeContext, err :=
		captureTenantMigrationSource(ctx, source, sourceTenantID)
	if err != nil {
		return report, err
	}
	candidate := newCoordinatedTenantCandidate(
		"migration",
		sourceTenantID,
		source.tenantObjectPrefix(sourceTenantID),
		targetTenantID,
	)
	targetExists, alreadyActivated, err := prepareCoordinatedMigrationTarget(
		ctx,
		target,
		targetTenantID,
		options.Overwrite,
		candidate,
	)
	if err != nil {
		return report, err
	}
	report.TargetExists = targetExists
	if alreadyActivated {
		if err := target.mirrorLatestWriteContext(ctx, targetTenantID); err != nil {
			return report, err
		}
		if err := target.addTenantToRegistry(ctx, targetTenantID); err != nil {
			return report, err
		}
		if err := target.completeCoordinatedTenantCandidate(
			ctx, targetTenantID, candidate,
		); err != nil {
			return report, err
		}
		report.FinishedAt = time.Now().UTC()
		return report, nil
	}

	writeObject := func(ctx context.Context, key string, data []byte) error {
		return target.putCoordinatedCandidateObject(ctx, key, data)
	}
	segmentHashes, segmentObjectHashes, err := source.copyTenantMigrationSegments(
		ctx,
		sourceTenantID,
		report.TargetPrefix,
		sourceManifest,
		writeObject,
	)
	if err != nil {
		return report, err
	}
	found := false
	err = scanObjectPrefix(ctx, source.Objects, report.SourcePrefix, func(
		objects []ObjectInfo,
	) error {
		pending := make([]tenantMigrationPendingObject, 0, len(objects))
		for _, object := range objects {
			if err := objectContextErr(ctx); err != nil {
				return err
			}
			found = true
			relative := strings.TrimPrefix(object.Key, report.SourcePrefix)
			report.Objects++
			report.Bytes += object.Size
			if skipCoordinatorMigrationObject(relative) {
				report.Skipped++
				continue
			}
			targetKey := report.TargetPrefix + relative
			sampleIndex := -1
			if len(report.Samples) < tenantMigrationSampleLimit {
				report.Samples = append(report.Samples, TenantMigrationObject{
					SourceKey: object.Key,
					TargetKey: targetKey,
					Bytes:     object.Size,
					ETag:      object.ETag,
				})
				sampleIndex = len(report.Samples) - 1
			}
			if strings.HasPrefix(relative, "commits/segments/") {
				hash, copied := segmentObjectHashes[object.Key]
				if !copied {
					report.Skipped++
					continue
				}
				report.Copied++
				if sampleIndex >= 0 {
					report.Samples[sampleIndex].SHA256 = hash
				}
				continue
			}
			pending = append(pending, tenantMigrationPendingObject{
				object: object, targetKey: targetKey, sampleIndex: sampleIndex,
			})
		}
		hashes, copied, err := source.copyTenantMigrationPage(
			ctx,
			sourceTenantID,
			targetTenantID,
			report.TargetPrefix,
			segmentHashes,
			writerFenceRef{},
			pending,
			writeObject,
		)
		for index, ok := range copied {
			if !ok {
				continue
			}
			report.Copied++
			if sampleIndex := pending[index].sampleIndex; sampleIndex >= 0 {
				report.Samples[sampleIndex].SHA256 = hashes[index]
			}
		}
		return err
	})
	if err != nil {
		return report, err
	}
	if !found {
		return report, ErrNotFound
	}

	rewriteTenantMigrationManifest(
		&sourceManifest,
		targetTenantID,
		report.SourcePrefix,
		report.TargetPrefix,
		writerFenceRef{},
		segmentHashes,
	)
	sourceManifest.UpdatedAt = time.Now().UTC()
	activationContext := writeContext.coordinatedSnapshot(
		targetTenantID,
		sourceManifest.Version,
	)
	if _, err := target.putCoordinatedManifest(
		ctx,
		targetTenantID,
		sourceManifest,
		ObjectMeta{Key: target.manifestKey(targetTenantID)},
		nil,
		activationContext,
	); err != nil {
		return report, err
	}
	if activationContext != nil {
		if err := target.mirrorLatestWriteContext(ctx, targetTenantID); err != nil {
			return report, err
		}
	}
	if err := target.addTenantToRegistry(ctx, targetTenantID); err != nil {
		return report, err
	}
	if err := target.completeCoordinatedTenantCandidate(
		ctx, targetTenantID, candidate,
	); err != nil {
		return report, err
	}
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func prepareCoordinatedMigrationTarget(
	ctx context.Context,
	target *TenantStore,
	tenantID string,
	overwrite bool,
	candidate coordinatedTenantCandidate,
) (bool, bool, error) {
	head, headExists, err := target.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return false, false, err
	}
	dataExists, err := target.tenantDataExists(ctx, tenantID)
	if err != nil {
		return false, false, err
	}
	currentCandidate, candidateExists, _, err :=
		target.getCoordinatedTenantCandidate(ctx, tenantID)
	if err != nil {
		return false, false, err
	}
	resumable := candidateExists && currentCandidate == candidate
	targetExists := dataExists || (headExists && head.Status != TenantStatusDeleted)
	if headExists && head.Status == TenantStatusActive && resumable {
		if _, _, err := target.getCoordinatedManifest(ctx, tenantID); err != nil {
			return targetExists, false, err
		}
		return targetExists, true, nil
	}
	if targetExists && !overwrite &&
		(!resumable || (headExists && head.Status != TenantStatusDeleted)) {
		return targetExists, false, fmt.Errorf(
			"%w: target tenant %q already exists", ErrConflict, tenantID,
		)
	}
	if resumable && (!headExists || head.Status == TenantStatusDeleted) {
		return targetExists, false, nil
	}
	if dataExists && !headExists {
		return targetExists, false, fmt.Errorf(
			"%w: target tenant %q has unowned data",
			ErrCoordinatorHeadMissing,
			tenantID,
		)
	}
	if headExists && (dataExists || head.Status != TenantStatusDeleted) {
		if _, err := target.PurgeTenant(ctx, tenantID, true); err != nil {
			return targetExists, false, err
		}
	}
	_, err = target.prepareCoordinatedTenantCandidate(
		ctx, tenantID, candidate,
	)
	return targetExists, false, err
}

func skipCoordinatorMigrationObject(relative string) bool {
	return relative == "manifest.parquet" ||
		relative == "control/writer-lease.parquet" ||
		strings.HasPrefix(relative, "coordination/")
}

func loadTenantMigrationContext(
	ctx context.Context,
	source *TenantStore,
	tenantID string,
) (tenantMigrationContext, error) {
	var out tenantMigrationContext
	config, configured, err := source.GetTenantConfig(ctx, tenantID)
	if err != nil {
		return out, err
	}
	out.config = config
	out.hasConfig = configured
	policy, configured, err := source.GetSourcePolicy(ctx, tenantID)
	if err != nil {
		return out, err
	}
	out.sourcePolicy = sourcePolicyRecord{TenantID: tenantID, SourcePolicy: policy}
	out.hasSourcePolicy = configured
	schemas, err := source.GetRelationSchemas(ctx, tenantID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return out, err
	}
	out.relationSchemas = append([]RelationSchema(nil), schemas.RelationSchemas...)
	return out, nil
}

func (c tenantMigrationContext) coordinatedSnapshot(
	tenantID string,
	graphVersion int64,
) *WriteContextSnapshot {
	if !c.hasConfig && !c.hasSourcePolicy && len(c.relationSchemas) == 0 {
		return nil
	}
	snapshot := emptyWriteContext(tenantID)
	snapshot.TenantConfig = c.config
	snapshot.TenantConfigConfigured = c.hasConfig
	snapshot.SourcePolicy = c.sourcePolicy.SourcePolicy
	snapshot.SourcePolicyConfigured = c.hasSourcePolicy
	snapshot.RelationSchemas.RelationSchemas = append(
		[]RelationSchema(nil), c.relationSchemas...,
	)
	if len(c.relationSchemas) > 0 {
		snapshot.RelationSchemas.GraphVersion = graphVersion
	}
	return &snapshot
}
