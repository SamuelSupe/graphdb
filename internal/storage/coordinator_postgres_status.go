package storage

import (
	"context"
	"time"
)

func (c *PostgresCoordinator) Status(ctx context.Context) (CoordinatorStatus, error) {
	status := CoordinatorStatus{
		Backend:       CoordinationPostgres,
		Available:     true,
		SchemaVersion: coordinatorSchemaVersion,
		Namespace:     c.namespace,
		CheckedAt:     time.Now().UTC(),
	}
	err := c.pool.QueryRow(ctx,
		`SELECT
			(SELECT count(*) FROM `+c.table("tenant_heads")+` WHERE namespace = $1),
			(SELECT count(*) FROM `+c.table("legacy_manifest_outbox")+`
			 WHERE namespace = $1 AND status <> 'done'),
			(SELECT count(*) FROM `+c.table("derived_tasks")+`
			 WHERE namespace = $1 AND target_version > processed_version),
			(SELECT COALESCE(max(head_revision - legacy_manifest_revision), 0)
			 FROM `+c.table("tenant_heads")+` WHERE namespace = $1)`,
		c.namespace,
	).Scan(&status.Tenants, &status.OutboxBacklog, &status.DerivedBacklog, &status.MaxMirrorLag)
	if err != nil {
		status.Available = false
		status.LastError = err.Error()
		return status, coordinatorUnavailable(err)
	}
	return status, nil
}
