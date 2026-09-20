package storage

import (
	"fmt"
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

const writeContextLayoutVersion = 1

type WriteContextSnapshot struct {
	LayoutVersion          int                   `json:"layout_version"`
	TenantID               string                `json:"tenant_id"`
	Revision               int64                 `json:"revision"`
	SourcePolicy           graph.SourcePolicy    `json:"source_policy,omitempty"`
	SourcePolicyConfigured bool                  `json:"source_policy_configured,omitempty"`
	TenantConfig           TenantConfig          `json:"tenant_config,omitempty"`
	TenantConfigConfigured bool                  `json:"tenant_config_configured,omitempty"`
	RelationSchemas        RelationSchemaCatalog `json:"relation_schemas"`
	UpdatedAt              time.Time             `json:"updated_at"`
}

func emptyWriteContext(tenantID string) WriteContextSnapshot {
	return WriteContextSnapshot{
		LayoutVersion:   writeContextLayoutVersion,
		TenantID:        tenantID,
		RelationSchemas: emptyRelationSchemaCatalog(tenantID),
	}
}
