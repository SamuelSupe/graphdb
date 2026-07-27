package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) ReserveCommit(
	ctx context.Context,
	tenantID, key, requestHash, ownerToken string,
	pendingTTL time.Duration,
) (CommitReservation, error) {
	if key == "" {
		return CommitReservation{}, nil
	}
	tag, err := c.pool.Exec(ctx,
		`INSERT INTO `+c.table("commit_idempotency")+` (
			namespace, tenant_id, idempotency_key, request_hash, owner_token,
			status, started_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,'pending',now(),now())
		ON CONFLICT (namespace, tenant_id, idempotency_key) DO NOTHING`,
		c.namespace, tenantID, key, requestHash, ownerToken,
	)
	if err != nil {
		return CommitReservation{}, coordinatorUnavailable(err)
	}
	if tag.RowsAffected() == 1 {
		return CommitReservation{Key: key, RequestHash: requestHash, OwnerToken: ownerToken}, nil
	}

	reservation, err := c.loadCommitReservation(ctx, tenantID, key)
	if err != nil {
		return CommitReservation{}, err
	}
	if reservation.RequestHash != requestHash {
		return CommitReservation{}, fmt.Errorf("commit idempotency conflict for key %q: stored request differs from incoming request", key)
	}
	if reservation.Committed {
		return reservation, nil
	}
	if pendingTTL <= 0 {
		pendingTTL = coordinatorPendingReservationTTL
	}
	tag, err = c.pool.Exec(ctx,
		`UPDATE `+c.table("commit_idempotency")+`
		 SET owner_token = $5, started_at = now(), updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3
		   AND request_hash = $4 AND status = 'pending'
		   AND updated_at < now() - $6::interval`,
		c.namespace, tenantID, key, requestHash, ownerToken, postgresInterval(pendingTTL),
	)
	if err != nil {
		return CommitReservation{}, coordinatorUnavailable(err)
	}
	if tag.RowsAffected() == 1 {
		return CommitReservation{Key: key, RequestHash: requestHash, OwnerToken: ownerToken}, nil
	}
	return CommitReservation{}, fmt.Errorf("%w: commit idempotency key %q", ErrIdempotencyInProgress, key)
}

func (c *PostgresCoordinator) AbortCommit(
	ctx context.Context,
	tenantID, key, requestHash, ownerToken string,
) error {
	if key == "" {
		return nil
	}
	_, err := c.pool.Exec(ctx,
		`DELETE FROM `+c.table("commit_idempotency")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3
		   AND request_hash = $4 AND owner_token = $5 AND status = 'pending'`,
		c.namespace, tenantID, key, requestHash, ownerToken,
	)
	return coordinatorUnavailable(err)
}

func (c *PostgresCoordinator) RenewCommit(
	ctx context.Context,
	tenantID, key, requestHash, ownerToken string,
) (bool, error) {
	if key == "" {
		return true, nil
	}
	tag, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("commit_idempotency")+`
		 SET updated_at = CASE WHEN status = 'pending' THEN now() ELSE updated_at END
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3
		   AND request_hash = $4 AND owner_token = $5
		   AND status IN ('pending', 'committed')`,
		c.namespace, tenantID, key, requestHash, ownerToken,
	)
	if err != nil {
		return false, coordinatorUnavailable(err)
	}
	return tag.RowsAffected() == 1, nil
}

func (c *PostgresCoordinator) loadCommitReservation(ctx context.Context, tenantID, key string) (CommitReservation, error) {
	var reservation CommitReservation
	var status string
	var result []byte
	err := c.pool.QueryRow(ctx,
		`SELECT idempotency_key, request_hash, owner_token, status, result_json
		 FROM `+c.table("commit_idempotency")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3`,
		c.namespace, tenantID, key,
	).Scan(&reservation.Key, &reservation.RequestHash, &reservation.OwnerToken, &status, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		return CommitReservation{}, ErrNotFound
	}
	if err != nil {
		return CommitReservation{}, coordinatorUnavailable(err)
	}
	reservation.Committed = status == "committed"
	if len(result) > 0 {
		reservation.Result = json.RawMessage(append([]byte(nil), result...))
	}
	return reservation, nil
}
