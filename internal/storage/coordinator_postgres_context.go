package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) PublishWriteContext(
	ctx context.Context,
	request WriteContextPublishRequest,
) (CoordinationHead, bool, error) {
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
		 SET write_context_revision = write_context_revision + 1,
		     write_context_key = $6,
		     write_context_hash = $7,
		     updated_at = $8
		 WHERE namespace = $1 AND tenant_id = $2
		   AND head_revision = $3
		   AND generation = $4
		   AND write_context_revision = $5
		   AND status = 'active'
		 RETURNING `+coordinatorHeadColumns,
		c.namespace,
		request.TenantID,
		request.ExpectedRevision,
		request.ExpectedGeneration,
		request.ExpectedContext,
		request.WriteContextKey,
		request.WriteContextHash,
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
	if err := tx.Commit(ctx); err != nil {
		return c.resolveAmbiguousWriteContext(head)
	}
	return head, true, nil
}
