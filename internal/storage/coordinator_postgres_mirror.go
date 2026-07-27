package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) ClaimLegacyManifest(ctx context.Context, ownerToken string, leaseTTL time.Duration) (LegacyManifestJob, bool, error) {
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	var job LegacyManifestJob
	err := c.pool.QueryRow(ctx,
		`WITH candidate_head AS (
			SELECT h.namespace, h.tenant_id, h.generation, h.legacy_manifest_revision
			FROM `+c.table("tenant_heads")+` h
			WHERE h.namespace = $1
			  AND h.status <> 'deleted'
			  AND h.legacy_manifest_revision < h.head_revision
			  AND NOT EXISTS (
				SELECT 1
				FROM `+c.table("legacy_manifest_outbox")+` running
				WHERE running.namespace = h.namespace
				  AND running.tenant_id = h.tenant_id
				  AND running.status = 'running'
				  AND running.lease_until >= now()
			  )
			  AND EXISTS (
				SELECT 1
				FROM `+c.table("legacy_manifest_outbox")+` ready
				WHERE ready.namespace = h.namespace
				  AND ready.tenant_id = h.tenant_id
				  AND ready.head_revision > h.legacy_manifest_revision
				  AND ready.next_attempt_at <= now()
				  AND (
					ready.status = 'pending'
					OR (ready.status = 'running' AND ready.lease_until < now())
				  )
			  )
			ORDER BY h.updated_at, h.tenant_id
			FOR UPDATE OF h SKIP LOCKED
			LIMIT 1
		), candidate AS (
			SELECT o.namespace, o.tenant_id, o.head_revision, h.generation
			FROM candidate_head h
			JOIN LATERAL (
				SELECT pending.namespace, pending.tenant_id, pending.head_revision
				FROM `+c.table("legacy_manifest_outbox")+` pending
				WHERE pending.namespace = h.namespace
				  AND pending.tenant_id = h.tenant_id
				  AND pending.head_revision > h.legacy_manifest_revision
				  AND pending.next_attempt_at <= now()
				  AND (
					pending.status = 'pending'
					OR (pending.status = 'running' AND pending.lease_until < now())
				  )
				ORDER BY pending.head_revision DESC
				FOR UPDATE OF pending SKIP LOCKED
				LIMIT 1
			) o ON true
		)
		UPDATE `+c.table("legacy_manifest_outbox")+` o
		SET status = 'running', owner_token = $2, lease_until = now() + $3::interval,
		    attempts = attempts + 1, updated_at = now()
		FROM candidate c
		WHERE o.namespace = c.namespace AND o.tenant_id = c.tenant_id
		  AND o.head_revision = c.head_revision
		RETURNING o.tenant_id, c.generation, o.head_revision, o.graph_version, o.manifest_key,
		          o.manifest_hash, o.owner_token, o.attempts`,
		c.namespace, ownerToken, postgresInterval(leaseTTL),
	).Scan(
		&job.TenantID,
		&job.Generation,
		&job.HeadRevision,
		&job.GraphVersion,
		&job.ManifestKey,
		&job.ManifestHash,
		&job.OwnerToken,
		&job.Attempts,
	)
	if err == pgx.ErrNoRows {
		return LegacyManifestJob{}, false, nil
	}
	if err != nil {
		return LegacyManifestJob{}, false, coordinatorUnavailable(err)
	}
	return job, true, nil
}

func (c *PostgresCoordinator) RenewLegacyManifest(
	ctx context.Context,
	job LegacyManifestJob,
	leaseTTL time.Duration,
) (bool, error) {
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	tag, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("legacy_manifest_outbox")+`
		 SET lease_until = now() + $5::interval
		 WHERE namespace = $1 AND tenant_id = $2 AND head_revision = $3
		   AND owner_token = $4 AND status = 'running'`,
		c.namespace,
		job.TenantID,
		job.HeadRevision,
		job.OwnerToken,
		postgresInterval(leaseTTL),
	)
	if err != nil {
		return false, coordinatorUnavailable(err)
	}
	return tag.RowsAffected() == 1, nil
}

func (c *PostgresCoordinator) CompleteLegacyManifest(ctx context.Context, job LegacyManifestJob) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var owned bool
	err = tx.QueryRow(ctx,
		`SELECT true
		 FROM `+c.table("legacy_manifest_outbox")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND head_revision = $3
		   AND owner_token = $4 AND status = 'running'
		 FOR UPDATE`,
		c.namespace, job.TenantID, job.HeadRevision, job.OwnerToken,
	).Scan(&owned)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrConflict
	}
	if err != nil {
		return coordinatorUnavailable(err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE `+c.table("tenant_heads")+`
		 SET legacy_manifest_revision = $3, updated_at = updated_at
		 WHERE namespace = $1 AND tenant_id = $2
		   AND legacy_manifest_revision < $3
		   AND head_revision >= $3
		   AND generation = $4 AND status <> 'deleted'`,
		c.namespace, job.TenantID, job.HeadRevision, job.Generation,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	tag, err = tx.Exec(ctx,
		`UPDATE `+c.table("legacy_manifest_outbox")+`
		 SET status = 'done', owner_token = '', lease_until = NULL,
		     last_error = '', updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND head_revision <= $3
		   AND status <> 'done'`,
		c.namespace, job.TenantID, job.HeadRevision,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() < 1 {
		return ErrConflict
	}
	return coordinatorUnavailable(tx.Commit(ctx))
}

func (c *PostgresCoordinator) FailLegacyManifest(ctx context.Context, job LegacyManifestJob, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	delay := time.Duration(job.Attempts) * time.Second
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	_, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("legacy_manifest_outbox")+`
		 SET status = 'pending', owner_token = '', lease_until = NULL,
		     next_attempt_at = now() + $6::interval, last_error = $5, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND head_revision = $3
		   AND owner_token = $4 AND status = 'running'`,
		c.namespace, job.TenantID, job.HeadRevision, job.OwnerToken,
		message, postgresInterval(delay),
	)
	return coordinatorUnavailable(err)
}

func postgresInterval(value time.Duration) string {
	return fmt.Sprintf("%.6f seconds", value.Seconds())
}
