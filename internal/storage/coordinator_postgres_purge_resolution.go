package storage

import (
	"context"
	"fmt"
	"time"
)

func (c *PostgresCoordinator) resolveAmbiguousTenantPurge(
	tenantID string,
	generation int64,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var finalized bool
	err := c.pool.QueryRow(ctx,
		`SELECT
		    h.generation = $3
		    AND h.status = 'deleted'
		    AND h.legacy_manifest_revision = h.head_revision
		    AND NOT EXISTS (
		        SELECT 1 FROM `+c.table("commit_idempotency")+` i
		        WHERE i.namespace = h.namespace AND i.tenant_id = h.tenant_id
		    )
		    AND NOT EXISTS (
		        SELECT 1 FROM `+c.table("collector_state")+` s
		        WHERE s.namespace = h.namespace AND s.tenant_id = h.tenant_id
		    )
		    AND NOT EXISTS (
		        SELECT 1 FROM `+c.table("derived_tasks")+` d
		        WHERE d.namespace = h.namespace AND d.tenant_id = h.tenant_id
		    )
		    AND NOT EXISTS (
		        SELECT 1 FROM `+c.table("legacy_manifest_outbox")+` o
		        WHERE o.namespace = h.namespace AND o.tenant_id = h.tenant_id
		    )
		 FROM `+c.table("tenant_heads")+` h
		 WHERE h.namespace = $1 AND h.tenant_id = $2`,
		c.namespace, tenantID, generation,
	).Scan(&finalized)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if finalized {
		return nil
	}
	return fmt.Errorf(
		"%w: PostgreSQL tenant purge outcome is unknown",
		ErrCoordinatorUnavailable,
	)
}
