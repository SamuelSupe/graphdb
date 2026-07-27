package storage

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func prepareTenantRestoreContext(
	record TenantBackupRecord,
	tenantID string,
) (WriteContextSnapshot, string, error) {
	if record.Version != record.Snapshot.Version {
		return WriteContextSnapshot{}, "", fmt.Errorf(
			"backup version %d does not match snapshot version %d",
			record.Version, record.Snapshot.Version,
		)
	}
	g, err := graph.FromSnapshot(record.Snapshot)
	if err != nil {
		return WriteContextSnapshot{}, "", err
	}
	dataMD5, err := g.ContentMD5()
	if err != nil {
		return WriteContextSnapshot{}, "", err
	}
	snapshot, err := tenantWriteContextFromBackupRecord(record, tenantID)
	if err != nil {
		return WriteContextSnapshot{}, "", err
	}
	if err := validateRelationSchemaGraph(g, snapshot.RelationSchemas); err != nil {
		return WriteContextSnapshot{}, "", err
	}
	return snapshot, dataMD5, nil
}

func tenantWriteContextFromBackupRecord(
	record TenantBackupRecord,
	tenantID string,
) (WriteContextSnapshot, error) {
	snapshot := emptyWriteContext(tenantID)
	if record.Config != nil {
		if err := validateTenantConfig(*record.Config); err != nil {
			return WriteContextSnapshot{}, err
		}
		snapshot.TenantConfig = *record.Config
		snapshot.TenantConfigConfigured = true
	}
	if record.SourcePolicy != nil {
		policy, err := graph.NormalizeSourcePolicy(*record.SourcePolicy)
		if err != nil {
			return WriteContextSnapshot{}, err
		}
		snapshot.SourcePolicy = policy
		snapshot.SourcePolicyConfigured = true
	}
	if len(record.RelationSchemas) > 0 {
		catalog := emptyRelationSchemaCatalog(tenantID)
		catalog.Revision = 1
		catalog.GraphVersion = record.Version
		catalog.RelationSchemas = append(
			[]RelationSchema(nil), record.RelationSchemas...,
		)
		catalog, err := normalizeRelationSchemaCatalog(catalog)
		if err != nil {
			return WriteContextSnapshot{}, err
		}
		snapshot.RelationSchemas = catalog
	}
	return snapshot, nil
}

func (s *TenantStore) pinCoordinatedRestoreContext(
	ctx context.Context,
	tenantID string,
	expectedGraphVersion int64,
	desired WriteContextSnapshot,
) (ObjectMeta, error) {
	current, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
	if err != nil {
		return ObjectMeta{}, err
	}
	if head.Status != TenantStatusActive {
		return ObjectMeta{}, ErrTenantDeleted
	}
	if head.GraphVersion != expectedGraphVersion {
		return ObjectMeta{}, fmt.Errorf(
			"%w: target tenant %q changed during restore",
			ErrConflict, tenantID,
		)
	}
	if sameRestoreWriteContext(current, desired) {
		return coordinatedManifestMeta(head.ManifestKey, head), nil
	}
	if head.WriteContextRevision != 0 {
		return ObjectMeta{}, fmt.Errorf(
			"%w: target tenant %q write context changed during restore",
			ErrConflict, tenantID,
		)
	}
	next, published, err := s.publishCoordinatedWriteContext(
		ctx, head, desired,
	)
	if err != nil {
		return ObjectMeta{}, err
	}
	if !published {
		return ObjectMeta{}, fmt.Errorf(
			"%w: target tenant %q changed during restore",
			ErrConflict, tenantID,
		)
	}
	return coordinatedManifestMeta(next.ManifestKey, next), nil
}

func sameRestoreWriteContext(left, right WriteContextSnapshot) bool {
	left.Revision = 0
	left.UpdatedAt = time.Time{}
	left.RelationSchemas.Revision = 0
	left.RelationSchemas.UpdatedAt = time.Time{}
	right.Revision = 0
	right.UpdatedAt = time.Time{}
	right.RelationSchemas.Revision = 0
	right.RelationSchemas.UpdatedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}
