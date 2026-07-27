package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) TransitionTenant(
	ctx context.Context,
	tenantID string,
	status string,
	advanceGeneration bool,
) (CoordinationHead, error) {
	generationIncrement := int64(0)
	if advanceGeneration {
		generationIncrement = 1
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return CoordinationHead{}, coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := c.requirePostgresModeTx(ctx, tx); err != nil {
		return CoordinationHead{}, err
	}
	head, err := scanCoordinationHead(tx.QueryRow(ctx,
		`UPDATE `+c.table("tenant_heads")+`
		 SET status = $3,
		     generation = generation + $4,
		     updated_at = $5
		 WHERE namespace = $1 AND tenant_id = $2
		 RETURNING `+coordinatorHeadColumns,
		c.namespace, tenantID, status, generationIncrement, time.Now().UTC(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinationHead{}, ErrCoordinatorHeadMissing
	}
	if err != nil {
		return CoordinationHead{}, coordinatorUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return c.resolveAmbiguousTenantTransition(head, advanceGeneration)
	}
	return head, nil
}

func (c *PostgresCoordinator) FinalizeTenantPurge(
	ctx context.Context,
	tenantID string,
	generation int64,
) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := c.requirePostgresModeTx(ctx, tx); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE `+c.table("tenant_heads")+`
		 SET legacy_manifest_revision = head_revision, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2
		   AND generation = $3 AND status = 'deleted'`,
		c.namespace, tenantID, generation,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	for _, table := range []string{
		"commit_idempotency",
		"collector_state",
		"derived_tasks",
		"legacy_manifest_outbox",
	} {
		if _, err := tx.Exec(ctx,
			`DELETE FROM `+c.table(table)+` WHERE namespace = $1 AND tenant_id = $2`,
			c.namespace, tenantID,
		); err != nil {
			return coordinatorUnavailable(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return c.resolveAmbiguousTenantPurge(tenantID, generation)
	}
	return nil
}

func (c *PostgresCoordinator) ActivateTenantHead(
	ctx context.Context,
	request HeadPublishRequest,
) (CoordinationHead, bool, error) {
	contextRevision, err := initialHeadWriteContextRevision(request)
	if err != nil {
		return CoordinationHead{}, false, err
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return CoordinationHead{}, false, coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := c.requirePostgresModeTx(ctx, tx); err != nil {
		return CoordinationHead{}, false, err
	}
	head, err := scanCoordinationHead(tx.QueryRow(ctx,
		`UPDATE `+c.table("tenant_heads")+`
		 SET status = 'active',
		     generation = generation + 1,
		     head_revision = head_revision + 1,
		     graph_version = $4,
		     manifest_key = $5,
		     manifest_hash = $6,
		     commit_id = $7,
		     write_context_revision = $8,
		     write_context_key = $9,
		     write_context_hash = $10,
		     updated_at = $11
		 WHERE namespace = $1 AND tenant_id = $2
		   AND generation = $3 AND status = 'deleted'
		 RETURNING `+coordinatorHeadColumns,
		c.namespace,
		request.TenantID,
		request.ExpectedGeneration,
		request.GraphVersion,
		request.ManifestKey,
		request.ManifestHash,
		request.CommitID,
		contextRevision,
		request.InitialWriteContextKey,
		request.InitialWriteContextHash,
		time.Now().UTC(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		current, _, headErr := c.Head(ctx, request.TenantID)
		return current, false, headErr
	}
	if err != nil {
		return CoordinationHead{}, false, coordinatorUnavailable(err)
	}
	if err := c.insertLegacyManifestJob(
		ctx, tx, head.TenantID, head.Revision, head.GraphVersion,
		head.ManifestKey, head.ManifestHash, head.CommitID,
	); err != nil {
		return CoordinationHead{}, false, err
	}
	if err := c.enqueueDerivedIndexes(ctx, tx, head.TenantID, head.GraphVersion); err != nil {
		return CoordinationHead{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return c.resolveAmbiguousPublish(ctx, request, head)
	}
	return head, true, nil
}
