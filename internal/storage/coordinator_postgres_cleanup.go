package storage

import (
	"context"
	"time"
)

func (c *PostgresCoordinator) Cleanup(
	ctx context.Context,
	config CoordinatorCleanupConfig,
) (CoordinatorCleanupReport, error) {
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultCoordinatorCleanupConfig().BatchSize
	}
	var report CoordinatorCleanupReport
	if config.IdempotencyRetention > 0 {
		now := time.Now().UTC()
		pendingTTL := config.PendingReservationTTL
		if pendingTTL <= 0 {
			pendingTTL = coordinatorPendingReservationTTL
		}
		pendingRetention := max(config.IdempotencyRetention, pendingTTL)
		tag, err := c.pool.Exec(ctx,
			`WITH candidates AS (
				SELECT item.ctid
				FROM `+c.table("commit_idempotency")+` item
				WHERE item.namespace = $1
				  AND (
				    (item.status = 'committed' AND item.updated_at < $2)
				    OR (item.status = 'pending' AND item.updated_at < $3)
				  )
				ORDER BY item.updated_at
				FOR UPDATE SKIP LOCKED
				LIMIT $4
			)
			DELETE FROM `+c.table("commit_idempotency")+` item
			USING candidates
			WHERE item.ctid = candidates.ctid`,
			c.namespace,
			now.Add(-config.IdempotencyRetention),
			now.Add(-pendingRetention),
			config.BatchSize,
		)
		if err != nil {
			return report, coordinatorUnavailable(err)
		}
		report.IdempotencyDeleted = tag.RowsAffected()
	}
	if config.OutboxRetention > 0 {
		tag, err := c.pool.Exec(ctx,
			`WITH candidates AS (
				SELECT item.ctid
				FROM `+c.table("legacy_manifest_outbox")+` item
				JOIN `+c.table("tenant_heads")+` head
				  ON head.namespace = item.namespace
				 AND head.tenant_id = item.tenant_id
				WHERE item.namespace = $1
				  AND item.status = 'done'
				  AND item.updated_at < $2
				  AND item.head_revision <= head.legacy_manifest_revision
				ORDER BY item.updated_at
				FOR UPDATE OF item SKIP LOCKED
				LIMIT $3
			)
			DELETE FROM `+c.table("legacy_manifest_outbox")+` item
			USING candidates
			WHERE item.ctid = candidates.ctid`,
			c.namespace,
			time.Now().UTC().Add(-config.OutboxRetention),
			config.BatchSize,
		)
		if err != nil {
			return report, coordinatorUnavailable(err)
		}
		report.OutboxDeleted = tag.RowsAffected()
	}
	return report, nil
}
