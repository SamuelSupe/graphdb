package storage

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

const derivedTaskIndexes = "indexes"

func (c *PostgresCoordinator) enqueueDerivedIndexes(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
	graphVersion int64,
) error {
	query := `INSERT INTO ` + c.table("derived_tasks") + ` AS current (
			namespace, tenant_id, task_type, target_version, next_attempt_at
			) VALUES ($1,$2,$3,$4,now() + $5::interval)
			ON CONFLICT (namespace, tenant_id, task_type) DO UPDATE
			SET target_version = GREATEST(current.target_version, EXCLUDED.target_version),
			    status = CASE
			        WHEN EXCLUDED.target_version > current.target_version THEN
			            CASE
			                WHEN current.status = 'running'
			                  AND COALESCE(current.lease_until > now(), false)
			                THEN current.status
			                ELSE 'pending'
			            END
			        ELSE current.status
			    END,
			    owner_token = CASE
			        WHEN EXCLUDED.target_version > current.target_version
			          AND NOT (
			              current.status = 'running'
			              AND COALESCE(current.lease_until > now(), false)
			          )
			        THEN ''
			        ELSE current.owner_token
			    END,
			    lease_until = CASE
			        WHEN EXCLUDED.target_version > current.target_version
			          AND NOT (
			              current.status = 'running'
			              AND COALESCE(current.lease_until > now(), false)
			          )
			        THEN NULL
			        ELSE current.lease_until
			    END,
			    next_attempt_at = CASE
			        WHEN EXCLUDED.target_version > current.target_version
			          AND NOT (
			              current.status = 'running'
			              AND COALESCE(current.lease_until > now(), false)
			          )
			        THEN now() + (
			            $5::interval * power(2, LEAST(current.attempts, 7))::double precision
			        )
			        ELSE current.next_attempt_at
			    END,
			    last_error = CASE
			        WHEN EXCLUDED.target_version > current.target_version
			        THEN ''
			        ELSE current.last_error
			    END,
			    updated_at = now()`
	var err error
	if tx == nil {
		_, err = c.pool.Exec(
			ctx, query, c.namespace, tenantID, derivedTaskIndexes, graphVersion,
			postgresInterval(derivedTaskDebounce),
		)
	} else {
		_, err = tx.Exec(
			ctx, query, c.namespace, tenantID, derivedTaskIndexes, graphVersion,
			postgresInterval(derivedTaskDebounce),
		)
	}
	return coordinatorUnavailable(err)
}

func (c *PostgresCoordinator) ClaimDerivedTask(
	ctx context.Context,
	ownerToken string,
	leaseTTL time.Duration,
) (DerivedTaskJob, bool, error) {
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	var job DerivedTaskJob
	err := c.pool.QueryRow(ctx,
		`WITH candidate AS (
			SELECT namespace, tenant_id, task_type
			FROM `+c.table("derived_tasks")+`
			WHERE namespace = $1
			  AND target_version > processed_version
			  AND next_attempt_at <= now()
			  AND (status = 'pending' OR (status = 'running' AND lease_until < now()))
			ORDER BY updated_at, tenant_id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE `+c.table("derived_tasks")+` task
		SET status = 'running', owner_token = $2,
		    lease_until = now() + $3::interval,
		    attempts = attempts + 1, updated_at = now()
		FROM candidate
		WHERE task.namespace = candidate.namespace
		  AND task.tenant_id = candidate.tenant_id
		  AND task.task_type = candidate.task_type
		RETURNING task.tenant_id, task.task_type, task.target_version,
		          task.owner_token, task.attempts`,
		c.namespace, ownerToken, postgresInterval(leaseTTL),
	).Scan(
		&job.TenantID,
		&job.TaskType,
		&job.TargetVersion,
		&job.OwnerToken,
		&job.Attempts,
	)
	if err == pgx.ErrNoRows {
		return DerivedTaskJob{}, false, nil
	}
	if err != nil {
		return DerivedTaskJob{}, false, coordinatorUnavailable(err)
	}
	return job, true, nil
}

func (c *PostgresCoordinator) RenewDerivedTask(
	ctx context.Context,
	job DerivedTaskJob,
	leaseTTL time.Duration,
) (bool, error) {
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	tag, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("derived_tasks")+`
		 SET lease_until = now() + $5::interval, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND owner_token = $4 AND status = 'running' AND lease_until > now()`,
		c.namespace, job.TenantID, job.TaskType, job.OwnerToken,
		postgresInterval(leaseTTL),
	)
	if err != nil {
		return false, coordinatorUnavailable(err)
	}
	return tag.RowsAffected() == 1, nil
}

func (c *PostgresCoordinator) CompleteDerivedTask(
	ctx context.Context,
	job DerivedTaskJob,
	processedVersion int64,
) error {
	tag, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("derived_tasks")+`
		 SET processed_version = GREATEST(processed_version, $6),
		     status = CASE WHEN target_version <= $6 THEN 'done' ELSE 'pending' END,
		     owner_token = '', lease_until = NULL, last_error = '',
		     attempts = CASE WHEN target_version <= $6 THEN 0 ELSE attempts END,
		     next_attempt_at = now(), updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND owner_token = $4 AND status = 'running' AND target_version >= $5`,
		c.namespace, job.TenantID, job.TaskType, job.OwnerToken, job.TargetVersion, processedVersion,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (c *PostgresCoordinator) AcknowledgeDerivedTaskVersion(
	ctx context.Context,
	tenantID string,
	taskType string,
	processedVersion int64,
) error {
	_, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("derived_tasks")+`
		 SET processed_version = GREATEST(processed_version, $4),
		     status = CASE WHEN target_version <= $4 THEN 'done' ELSE 'pending' END,
		     owner_token = '', lease_until = NULL, last_error = '',
		     attempts = CASE WHEN target_version <= $4 THEN 0 ELSE attempts END,
		     next_attempt_at = now(), updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND (
		       status <> 'running'
		       OR COALESCE(lease_until <= now(), true)
		   )`,
		c.namespace, tenantID, taskType, processedVersion,
	)
	return coordinatorUnavailable(err)
}

func (c *PostgresCoordinator) FailDerivedTask(
	ctx context.Context,
	job DerivedTaskJob,
	cause error,
) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	delay := time.Duration(job.Attempts) * time.Second
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	_, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("derived_tasks")+`
		 SET status = 'pending', owner_token = '', lease_until = NULL,
		     last_error = $5, next_attempt_at = now() + $6::interval, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
		   AND owner_token = $4 AND status = 'running'`,
		c.namespace, job.TenantID, job.TaskType, job.OwnerToken, message, postgresInterval(delay),
	)
	return coordinatorUnavailable(err)
}
