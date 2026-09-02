package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) TaskLease(
	ctx context.Context,
	tenantID string,
	taskType string,
) (CoordinatorTaskLease, bool, error) {
	lease, err := scanCoordinatorTaskLease(c.pool.QueryRow(ctx,
		`SELECT tenant_id, task_type, owner_token, fence_epoch, expires_at
		 FROM `+c.table("task_leases")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND owner_token <> '' AND expires_at > now()`,
		c.namespace, tenantID, taskType,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinatorTaskLease{}, false, nil
	}
	if err != nil {
		return CoordinatorTaskLease{}, false, coordinatorUnavailable(err)
	}
	return lease, true, nil
}

func (c *PostgresCoordinator) AcquireTaskLease(
	ctx context.Context,
	tenantID string,
	taskType string,
	ownerToken string,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	lease, err := scanCoordinatorTaskLease(c.pool.QueryRow(ctx,
		`INSERT INTO `+c.table("task_leases")+` AS current (
			namespace, tenant_id, task_type, owner_token, fence_epoch, expires_at, updated_at
		) VALUES ($1,$2,$3,$4,1,now() + $5::interval,now())
		ON CONFLICT (namespace, tenant_id, task_type) DO UPDATE
		SET owner_token = EXCLUDED.owner_token,
		    fence_epoch = CASE
		        WHEN current.owner_token = EXCLUDED.owner_token
		        THEN current.fence_epoch
		        ELSE current.fence_epoch + 1
		    END,
		    expires_at = EXCLUDED.expires_at,
		    updated_at = EXCLUDED.updated_at
		WHERE current.owner_token = EXCLUDED.owner_token
		   OR current.expires_at <= now()
		RETURNING tenant_id, task_type, owner_token, fence_epoch, expires_at`,
		c.namespace, tenantID, taskType, ownerToken, postgresInterval(ttl),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinatorTaskLease{}, false, nil
	}
	if err != nil {
		return CoordinatorTaskLease{}, false, coordinatorUnavailable(err)
	}
	return lease, true, nil
}

func (c *PostgresCoordinator) AcquireIngestPublishSlot(
	ctx context.Context,
	tenantID string,
	ownerToken string,
	ttl time.Duration,
) (CoordinatorTaskLease, CoordinationHead, bool, bool, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	// The head read must be a separate READ COMMITTED statement: lease takeover
	// can wait for the previous publisher, whose head update must then be visible.
	// SendBatch preserves that snapshot boundary without a second network trip.
	batch := &pgx.Batch{}
	batch.Queue(
		`INSERT INTO `+c.table("task_leases")+` AS current (
			namespace, tenant_id, task_type, owner_token, fence_epoch, expires_at, updated_at
		) VALUES ($1,$2,$3,$4,1,now() + $5::interval,now())
		ON CONFLICT (namespace, tenant_id, task_type) DO UPDATE
		SET owner_token = EXCLUDED.owner_token,
		    fence_epoch = CASE
		        WHEN current.owner_token = EXCLUDED.owner_token
		        THEN current.fence_epoch
		        ELSE current.fence_epoch + 1
		    END,
		    expires_at = EXCLUDED.expires_at,
		    updated_at = EXCLUDED.updated_at
		WHERE current.owner_token = EXCLUDED.owner_token
		   OR current.expires_at <= now()
		RETURNING tenant_id, task_type, owner_token, fence_epoch, expires_at`,
		c.namespace, tenantID, coordinatorIngestPublishTaskType, ownerToken,
		postgresInterval(ttl),
	)
	batch.Queue(
		`SELECT `+coordinatorHeadColumns+` FROM `+c.table("tenant_heads")+`
		 WHERE namespace = $1 AND tenant_id = $2`,
		c.namespace, tenantID,
	)
	batchResults := c.pool.SendBatch(ctx, batch)
	lease, leaseErr := scanCoordinatorTaskLease(batchResults.QueryRow())
	if errors.Is(leaseErr, pgx.ErrNoRows) {
		_, headErr := scanCoordinationHead(batchResults.QueryRow())
		if headErr != nil && !errors.Is(headErr, pgx.ErrNoRows) {
			_ = batchResults.Close()
			return CoordinatorTaskLease{}, CoordinationHead{}, false, false, coordinatorUnavailable(headErr)
		}
		if err := batchResults.Close(); err != nil {
			return CoordinatorTaskLease{}, CoordinationHead{}, false, false, coordinatorUnavailable(err)
		}
		return CoordinatorTaskLease{}, CoordinationHead{}, false, false, nil
	}
	if leaseErr != nil {
		_ = batchResults.Close()
		return CoordinatorTaskLease{}, CoordinationHead{}, false, false, coordinatorUnavailable(leaseErr)
	}
	head, headErr := scanCoordinationHead(batchResults.QueryRow())
	headExists := true
	if errors.Is(headErr, pgx.ErrNoRows) {
		head = CoordinationHead{}
		headExists = false
		headErr = nil
	}
	closeErr := batchResults.Close()
	if headErr != nil || closeErr != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.ReleaseTaskLease(releaseCtx, lease)
		if headErr != nil {
			return CoordinatorTaskLease{}, CoordinationHead{}, false, false, coordinatorUnavailable(headErr)
		}
		return CoordinatorTaskLease{}, CoordinationHead{}, false, false, coordinatorUnavailable(closeErr)
	}
	return lease, head, headExists, true, nil
}

func (c *PostgresCoordinator) RenewTaskLease(
	ctx context.Context,
	lease CoordinatorTaskLease,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	next, err := scanCoordinatorTaskLease(c.pool.QueryRow(ctx,
		`UPDATE `+c.table("task_leases")+`
		 SET expires_at = now() + $6::interval, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND owner_token = $4 AND fence_epoch = $5 AND expires_at > now()
		 RETURNING tenant_id, task_type, owner_token, fence_epoch, expires_at`,
		c.namespace, lease.TenantID, lease.TaskType, lease.OwnerToken,
		lease.FenceEpoch, postgresInterval(ttl),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinatorTaskLease{}, false, nil
	}
	if err != nil {
		return CoordinatorTaskLease{}, false, coordinatorUnavailable(err)
	}
	return next, true, nil
}

func (c *PostgresCoordinator) ReleaseTaskLease(ctx context.Context, lease CoordinatorTaskLease) error {
	tag, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("task_leases")+`
		 SET owner_token = '', expires_at = now() - interval '1 microsecond', updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND owner_token = $4 AND fence_epoch = $5`,
		c.namespace, lease.TenantID, lease.TaskType, lease.OwnerToken, lease.FenceEpoch,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (c *PostgresCoordinator) releaseTaskLeaseTx(
	ctx context.Context,
	tx pgx.Tx,
	lease CoordinatorTaskLease,
) error {
	tag, err := tx.Exec(ctx,
		`UPDATE `+c.table("task_leases")+`
		 SET owner_token = '', expires_at = now() - interval '1 microsecond', updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND owner_token = $4 AND fence_epoch = $5
		   AND expires_at > clock_timestamp()`,
		c.namespace, lease.TenantID, lease.TaskType, lease.OwnerToken, lease.FenceEpoch,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrTaskLeaseHeld
	}
	return nil
}

type coordinatorTaskLeaseScanner interface {
	Scan(...any) error
}

func scanCoordinatorTaskLease(row coordinatorTaskLeaseScanner) (CoordinatorTaskLease, error) {
	var lease CoordinatorTaskLease
	err := row.Scan(
		&lease.TenantID,
		&lease.TaskType,
		&lease.OwnerToken,
		&lease.FenceEpoch,
		&lease.ExpiresAt,
	)
	return lease, err
}
