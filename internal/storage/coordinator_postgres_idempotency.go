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
		return CommitReservation{}, fmt.Errorf(
			"%w for key %q: stored request differs from incoming request",
			ErrIdempotencyConflict,
			key,
		)
	}
	if reservation.Committed {
		return reservation, nil
	}
	if reservation.OwnerToken == ownerToken {
		ok, err := c.RenewCommit(ctx, tenantID, key, requestHash, ownerToken)
		if err != nil {
			return CommitReservation{}, err
		}
		if ok {
			return reservation, nil
		}
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

func (c *PostgresCoordinator) ReserveCommitBatch(
	ctx context.Context,
	tenantID string,
	requests []CommitReservationRequest,
	pendingTTL time.Duration,
) ([]CommitReservationOutcome, error) {
	outcomes := make([]CommitReservationOutcome, len(requests))
	if len(requests) == 0 {
		return outcomes, nil
	}
	keys := make([]string, 0, len(requests))
	hashes := make([]string, 0, len(requests))
	owners := make([]string, 0, len(requests))
	indexes := make([]int, 0, len(requests))
	seen := make(map[string]struct{}, len(requests))
	for index, request := range requests {
		if request.Key == "" {
			continue
		}
		if _, exists := seen[request.Key]; exists {
			return nil, fmt.Errorf("duplicate commit idempotency key %q in batch reservation", request.Key)
		}
		seen[request.Key] = struct{}{}
		keys = append(keys, request.Key)
		hashes = append(hashes, request.RequestHash)
		owners = append(owners, request.OwnerToken)
		indexes = append(indexes, index)
	}
	if len(keys) == 0 {
		return outcomes, nil
	}
	// Keep the insert, takeover and read as separate statements so each gets a
	// fresh READ COMMITTED snapshot after waiting on a concurrent same-key
	// writer. SendBatch still pipelines them in one client/server round trip.
	batch := &pgx.Batch{}
	batch.Queue(
		`INSERT INTO `+c.table("commit_idempotency")+` (
			namespace, tenant_id, idempotency_key, request_hash, owner_token,
			status, started_at, updated_at
		)
		SELECT $1, $2, input.idempotency_key, input.request_hash, input.owner_token,
		       'pending', now(), now()
		FROM unnest($3::text[], $4::text[], $5::text[])
		     AS input(idempotency_key, request_hash, owner_token)
		ON CONFLICT (namespace, tenant_id, idempotency_key) DO NOTHING`,
		c.namespace, tenantID, keys, hashes, owners,
	)
	if pendingTTL <= 0 {
		pendingTTL = coordinatorPendingReservationTTL
	}
	batch.Queue(
		`UPDATE `+c.table("commit_idempotency")+` AS current
		 SET owner_token = input.owner_token,
		     started_at = CASE WHEN current.owner_token = input.owner_token THEN current.started_at ELSE now() END,
		     updated_at = now()
		 FROM unnest($3::text[], $4::text[], $5::text[])
		      AS input(idempotency_key, request_hash, owner_token)
		 WHERE current.namespace = $1 AND current.tenant_id = $2
		   AND current.idempotency_key = input.idempotency_key
		   AND current.request_hash = input.request_hash
		   AND current.status = 'pending'
		   AND (current.owner_token = input.owner_token OR current.updated_at < now() - $6::interval)`,
		c.namespace, tenantID, keys, hashes, owners, postgresInterval(pendingTTL),
	)
	batch.Queue(
		`SELECT input.ordinality, current.idempotency_key, current.request_hash,
		        current.owner_token, current.status, current.result_json
		 FROM unnest($3::text[]) WITH ORDINALITY AS input(idempotency_key, ordinality)
		 JOIN `+c.table("commit_idempotency")+` AS current
		   ON current.namespace = $1 AND current.tenant_id = $2
		  AND current.idempotency_key = input.idempotency_key
		 ORDER BY input.ordinality`,
		c.namespace, tenantID, keys,
	)
	batchResults := c.pool.SendBatch(ctx, batch)
	closeBatch := func() {
		_ = batchResults.Close()
	}
	if _, err := batchResults.Exec(); err != nil {
		closeBatch()
		return nil, coordinatorUnavailable(err)
	}
	if _, err := batchResults.Exec(); err != nil {
		closeBatch()
		return nil, coordinatorUnavailable(err)
	}
	rows, err := batchResults.Query()
	if err != nil {
		closeBatch()
		return nil, coordinatorUnavailable(err)
	}
	found := make([]bool, len(keys))
	for rows.Next() {
		var (
			ordinal int
			status  string
			result  []byte
			stored  CommitReservation
		)
		if err := rows.Scan(&ordinal, &stored.Key, &stored.RequestHash, &stored.OwnerToken, &status, &result); err != nil {
			rows.Close()
			closeBatch()
			return nil, coordinatorUnavailable(err)
		}
		ordinal--
		if ordinal < 0 || ordinal >= len(keys) {
			rows.Close()
			closeBatch()
			return nil, coordinatorUnavailable(fmt.Errorf("invalid batch reservation ordinal %d", ordinal+1))
		}
		found[ordinal] = true
		stored.Committed = status == "committed"
		if len(result) > 0 {
			stored.Result = json.RawMessage(append([]byte(nil), result...))
		}
		request := requests[indexes[ordinal]]
		outcome := CommitReservationOutcome{Reservation: stored}
		switch {
		case stored.RequestHash != request.RequestHash:
			outcome.Err = fmt.Errorf("%w for key %q: stored request differs from incoming request", ErrIdempotencyConflict, request.Key)
		case stored.Committed:
		case stored.OwnerToken != request.OwnerToken:
			outcome.Err = fmt.Errorf("%w: commit idempotency key %q", ErrIdempotencyInProgress, request.Key)
		}
		outcomes[indexes[ordinal]] = outcome
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		closeBatch()
		return nil, coordinatorUnavailable(err)
	}
	rows.Close()
	if err := batchResults.Close(); err != nil {
		return nil, coordinatorUnavailable(err)
	}
	for index, ok := range found {
		if !ok {
			return nil, coordinatorUnavailable(fmt.Errorf("commit idempotency key %q disappeared during batch reservation", keys[index]))
		}
	}
	return outcomes, nil
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

func (c *PostgresCoordinator) AbortCommitBatch(
	ctx context.Context,
	tenantID string,
	requests []CommitReservationRequest,
) error {
	keys := make([]string, 0, len(requests))
	hashes := make([]string, 0, len(requests))
	owners := make([]string, 0, len(requests))
	for _, request := range requests {
		if request.Key == "" {
			continue
		}
		keys = append(keys, request.Key)
		hashes = append(hashes, request.RequestHash)
		owners = append(owners, request.OwnerToken)
	}
	if len(keys) == 0 {
		return nil
	}
	_, err := c.pool.Exec(ctx,
		`DELETE FROM `+c.table("commit_idempotency")+` AS current
		 USING unnest($3::text[], $4::text[], $5::text[])
		       AS input(idempotency_key, request_hash, owner_token)
		 WHERE current.namespace = $1 AND current.tenant_id = $2
		   AND current.idempotency_key = input.idempotency_key
		   AND current.request_hash = input.request_hash
		   AND current.owner_token = input.owner_token
		   AND current.status = 'pending'`,
		c.namespace, tenantID, keys, hashes, owners,
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
