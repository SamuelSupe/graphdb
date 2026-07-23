package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) CompleteNoop(
	ctx context.Context,
	request HeadPublishRequest,
) (bool, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return false, coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := c.requirePostgresModeTx(ctx, tx); err != nil {
		return false, err
	}
	head, err := scanCoordinationHead(tx.QueryRow(ctx,
		`SELECT `+coordinatorHeadColumns+` FROM `+c.table("tenant_heads")+`
		 WHERE namespace = $1 AND tenant_id = $2
		 FOR UPDATE`,
		c.namespace, request.TenantID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, coordinatorUnavailable(err)
	}
	if head.Revision != request.ExpectedRevision ||
		head.Generation != request.ExpectedGeneration ||
		head.WriteContextRevision != request.ExpectedWriteContextRevision ||
		head.Status != TenantStatusActive {
		return false, nil
	}
	if err := c.completeIdempotencyTx(ctx, tx, request); err != nil {
		return false, err
	}
	if err := c.upsertCollectorStateTx(ctx, tx, request.TenantID, request.CollectorState, head.GraphVersion); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		if request.IdempotencyKey != "" {
			resolveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reservation, loadErr := c.loadCommitReservation(
				resolveCtx, request.TenantID, request.IdempotencyKey,
			)
			if loadErr == nil && reservation.Committed && reservation.RequestHash == request.RequestHash {
				return true, nil
			}
		}
		return false, coordinatorUnavailable(err)
	}
	return true, nil
}

func (c *PostgresCoordinator) completeIdempotencyTx(
	ctx context.Context,
	tx pgx.Tx,
	request HeadPublishRequest,
) error {
	if request.IdempotencyKey == "" {
		return nil
	}
	resultJSON := []byte(request.Result)
	if len(resultJSON) == 0 {
		resultJSON = []byte(`{}`)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE `+c.table("commit_idempotency")+`
		 SET status = 'committed', result_json = $6::jsonb,
		     candidate_commit_id = $7, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3
		   AND request_hash = $4 AND owner_token = $5 AND status = 'pending'`,
		c.namespace,
		request.TenantID,
		request.IdempotencyKey,
		request.RequestHash,
		request.OwnerToken,
		string(resultJSON),
		request.CommitID,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrIdempotencyInProgress
	}
	return nil
}

func (c *PostgresCoordinator) upsertCollectorStateTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
	update *CollectorStateUpdate,
	version int64,
) error {
	if update == nil {
		return nil
	}
	if update.Version > 0 {
		version = update.Version
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO `+c.table("collector_state")+` AS current (
			namespace, tenant_id, source, collector_id,
			last_batch_id, last_cursor, last_version, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,now())
		ON CONFLICT (namespace, tenant_id, source, collector_id) DO UPDATE
		SET last_batch_id = EXCLUDED.last_batch_id,
		    last_cursor = EXCLUDED.last_cursor,
		    last_version = EXCLUDED.last_version,
		    updated_at = EXCLUDED.updated_at
		WHERE EXCLUDED.last_version >= current.last_version`,
		c.namespace,
		tenantID,
		update.Source,
		update.CollectorID,
		update.BatchID,
		update.Cursor,
		version,
	)
	return coordinatorUnavailable(err)
}

func (c *PostgresCoordinator) CollectorState(
	ctx context.Context,
	tenantID string,
	source string,
	collectorID string,
) (CollectorStateUpdate, bool, error) {
	var state CollectorStateUpdate
	err := c.pool.QueryRow(ctx,
		`SELECT source, collector_id, last_batch_id, last_cursor, last_version
		 FROM `+c.table("collector_state")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND source = $3 AND collector_id = $4`,
		c.namespace, tenantID, source, collectorID,
	).Scan(&state.Source, &state.CollectorID, &state.BatchID, &state.Cursor, &state.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return CollectorStateUpdate{}, false, nil
	}
	if err != nil {
		return CollectorStateUpdate{}, false, coordinatorUnavailable(err)
	}
	return state, true, nil
}
