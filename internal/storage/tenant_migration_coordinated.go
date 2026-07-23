package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const coordinatorMigrationTaskType = "migration"

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
	sourceObjects []ObjectInfo,
) (TenantMigrationReport, error) {
	leaseCtx, stopLease, err := target.startCoordinatorOperationLease(
		ctx, targetTenantID, coordinatorMigrationTaskType,
	)
	if err != nil {
		return report, err
	}
	defer stopLease()
	ctx = leaseCtx

	sourceManifest, _, err := source.getManifest(ctx, sourceTenantID)
	if err != nil {
		return report, err
	}
	writeContext, err := loadTenantMigrationContext(ctx, source, sourceTenantID)
	if err != nil {
		return report, err
	}
	targetExists, err := prepareCoordinatedMigrationTarget(
		ctx, target, targetTenantID, options.Overwrite,
	)
	if err != nil {
		return report, err
	}
	report.TargetExists = targetExists

	filtered := coordinatorMigrationObjects(
		sourceObjects, source.tenantObjectPrefix(sourceTenantID),
	)
	rewrites, segmentHashes, err := source.prepareTenantMigrationRewrites(
		ctx,
		sourceTenantID,
		targetTenantID,
		filtered,
		report.TargetPrefix,
		writerFenceRef{},
	)
	if err != nil {
		return report, err
	}
	for _, object := range sourceObjects {
		if err := objectContextErr(ctx); err != nil {
			return report, err
		}
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
		data := rewrites[object.Key]
		if len(data) == 0 {
			data, err = source.Objects.Get(ctx, object.Key)
			if err != nil {
				return report, err
			}
		}
		if _, err := target.Objects.PutConditional(
			ctx, targetKey, data, PutCondition{IfNoneMatch: true},
		); err != nil {
			return report, err
		}
		report.Copied++
		if sampleIndex >= 0 {
			report.Samples[sampleIndex].SHA256 = objectContentHash(data)
		}
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
	if _, err := target.putManifestMeta(
		ctx,
		targetTenantID,
		sourceManifest,
		ObjectMeta{Key: target.manifestKey(targetTenantID)},
	); err != nil {
		return report, err
	}
	if err := applyTenantMigrationContext(
		ctx, target, targetTenantID, sourceManifest.Version, writeContext,
	); err != nil {
		return report, err
	}
	if err := target.addTenantToRegistry(ctx, targetTenantID); err != nil {
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
) (bool, error) {
	head, headExists, err := target.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return false, err
	}
	dataExists, err := target.tenantDataExists(ctx, tenantID)
	if err != nil {
		return false, err
	}
	targetExists := dataExists || (headExists && head.Status != TenantStatusDeleted)
	if targetExists && !overwrite {
		return targetExists, fmt.Errorf(
			"%w: target tenant %q already exists", ErrConflict, tenantID,
		)
	}
	if dataExists && !headExists {
		return targetExists, fmt.Errorf(
			"%w: bootstrap target tenant %q before PostgreSQL migration",
			ErrCoordinatorHeadMissing,
			tenantID,
		)
	}
	if !headExists {
		head, err = target.ensureCoordinatedTenantHeadForCreate(ctx, tenantID)
		if err != nil {
			return targetExists, err
		}
		headExists = true
	}
	if !dataExists {
		head, err = target.Coordinator.TransitionTenant(
			ctx, tenantID, TenantStatusDeleted, true,
		)
		if err != nil {
			return targetExists, err
		}
		if err := target.Coordinator.FinalizeTenantPurge(
			ctx, tenantID, head.Generation,
		); err != nil {
			return targetExists, err
		}
		target.deleteWriteCache(tenantID)
		return targetExists, nil
	}
	_, err = target.PurgeTenant(ctx, tenantID, true)
	return targetExists, err
}

func coordinatorMigrationObjects(
	objects []ObjectInfo,
	sourcePrefix string,
) []ObjectInfo {
	filtered := make([]ObjectInfo, 0, len(objects))
	for _, object := range objects {
		relative := strings.TrimPrefix(object.Key, sourcePrefix)
		if !skipCoordinatorMigrationObject(relative) {
			filtered = append(filtered, object)
		}
	}
	return filtered
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

func applyTenantMigrationContext(
	ctx context.Context,
	target *TenantStore,
	tenantID string,
	graphVersion int64,
	writeContext tenantMigrationContext,
) error {
	if writeContext.hasConfig {
		if _, err := target.PutTenantConfig(ctx, tenantID, writeContext.config); err != nil {
			return err
		}
	}
	if writeContext.hasSourcePolicy {
		if _, err := target.PutSourcePolicy(
			ctx, tenantID, writeContext.sourcePolicy.SourcePolicy,
		); err != nil {
			return err
		}
	}
	return target.putRelationSchemasForLifecycle(
		ctx, tenantID, writeContext.relationSchemas, graphVersion,
	)
}
