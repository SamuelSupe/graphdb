package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) resolveAmbiguousPublish(
	_ context.Context,
	request HeadPublishRequest,
	candidate CoordinationHead,
) (CoordinationHead, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	idempotencyCommitted := false
	if request.IdempotencyKey != "" {
		reservation, err := c.loadCommitReservation(ctx, request.TenantID, request.IdempotencyKey)
		if err == nil {
			idempotencyCommitted = reservation.Committed &&
				reservation.RequestHash == request.RequestHash
		} else if !errors.Is(err, ErrNotFound) {
			return CoordinationHead{}, false, err
		}
	}
	current, exists, err := c.Head(ctx, request.TenantID)
	if err != nil {
		return CoordinationHead{}, false, err
	}
	if exists &&
		current.Revision == candidate.Revision &&
		current.CommitID == candidate.CommitID &&
		current.ManifestHash == candidate.ManifestHash {
		return current, true, nil
	}

	var revision int64
	var graphVersion int64
	err = c.pool.QueryRow(ctx,
		`SELECT head_revision, graph_version
		 FROM `+c.table("legacy_manifest_outbox")+`
		 WHERE namespace = $1 AND tenant_id = $2
		   AND head_revision = $3 AND graph_version = $4
		   AND manifest_key = $5 AND manifest_hash = $6 AND commit_id = $7`,
		c.namespace,
		request.TenantID,
		candidate.Revision,
		candidate.GraphVersion,
		candidate.ManifestKey,
		candidate.ManifestHash,
		candidate.CommitID,
	).Scan(&revision, &graphVersion)
	if err == nil {
		if revision != candidate.Revision || graphVersion != candidate.GraphVersion {
			return CoordinationHead{}, false, fmt.Errorf(
				"%w: PostgreSQL candidate commit metadata does not match",
				ErrCoordinatorUnavailable,
			)
		}
		return candidate, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CoordinationHead{}, false, coordinatorUnavailable(err)
	}
	if idempotencyCommitted {
		return CoordinationHead{}, false, fmt.Errorf(
			"%w: committed idempotency result has no candidate commit record",
			ErrCoordinatorUnavailable,
		)
	}
	return CoordinationHead{}, false, fmt.Errorf(
		"%w: PostgreSQL commit outcome is unknown",
		ErrCoordinatorUnavailable,
	)
}
