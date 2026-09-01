package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) PublishIngestBatch(
	ctx context.Context,
	request IngestBatchPublishRequest,
) (CoordinationHead, bool, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return CoordinationHead{}, false, coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := c.requirePostgresModeTx(ctx, tx); err != nil {
		return CoordinationHead{}, false, err
	}

	head, published, err := c.publishHeadTx(ctx, tx, request.Head)
	if err != nil {
		return CoordinationHead{}, false, err
	}
	if !published {
		_ = tx.Rollback(ctx)
		current, _, headErr := c.Head(ctx, request.Head.TenantID)
		return current, false, headErr
	}
	if err := c.completeIngestBatchItemsTx(ctx, tx, request.Head.TenantID, request.Items, head.GraphVersion); err != nil {
		return CoordinationHead{}, false, err
	}
	if err := c.insertLegacyManifestJob(
		ctx, tx, request.Head.TenantID, head.Revision, head.GraphVersion,
		head.ManifestKey, head.ManifestHash, head.CommitID,
	); err != nil {
		return CoordinationHead{}, false, err
	}
	if err := c.enqueueDerivedIndexes(ctx, tx, head.TenantID, head.GraphVersion); err != nil {
		return CoordinationHead{}, false, err
	}
	if request.PublishLease != nil {
		if err := c.releaseIngestPublishLeaseTx(
			ctx, tx, request.Head.TenantID, *request.PublishLease,
		); err != nil {
			return CoordinationHead{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return c.resolveAmbiguousIngestBatchPublish(request, head)
	}
	return head, true, nil
}

func (c *PostgresCoordinator) resolveAmbiguousIngestBatchPublish(
	request IngestBatchPublishRequest,
	expected CoordinationHead,
) (CoordinationHead, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	head, published, err := c.resolveAmbiguousPublish(ctx, request.Head, expected)
	if err != nil || !published {
		return head, published, err
	}
	for _, item := range request.Items {
		reservation, loadErr := c.loadCommitReservation(ctx, request.Head.TenantID, item.IdempotencyKey)
		if loadErr != nil || !reservation.Committed || reservation.RequestHash != item.RequestHash {
			return CoordinationHead{}, false, fmt.Errorf(
				"%w: PostgreSQL ingest batch publish outcome is unknown",
				ErrCoordinatorUnavailable,
			)
		}
	}
	return head, true, nil
}

func (c *PostgresCoordinator) CompleteIngestBatch(
	ctx context.Context,
	request IngestBatchPublishRequest,
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
		c.namespace, request.Head.TenantID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, coordinatorUnavailable(err)
	}
	switch head.Status {
	case TenantStatusDisabled:
		return false, ErrTenantDisabled
	case TenantStatusDeleted:
		return false, ErrTenantDeleted
	}
	if head.Revision != request.Head.ExpectedRevision ||
		head.Generation != request.Head.ExpectedGeneration ||
		head.WriteContextRevision != request.Head.ExpectedWriteContextRevision {
		return false, nil
	}
	if err := c.completeIngestBatchItemsTx(ctx, tx, request.Head.TenantID, request.Items, head.GraphVersion); err != nil {
		return false, err
	}
	if request.PublishLease != nil {
		if err := c.releaseIngestPublishLeaseTx(
			ctx, tx, request.Head.TenantID, *request.PublishLease,
		); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return c.resolveAmbiguousIngestBatchCompletion(request)
	}
	return true, nil
}

func (c *PostgresCoordinator) releaseIngestPublishLeaseTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
	lease CoordinatorTaskLease,
) error {
	if lease.TenantID != tenantID || lease.TaskType != coordinatorIngestPublishTaskType {
		return fmt.Errorf("ingest publish lease does not match tenant %q", tenantID)
	}
	return c.releaseTaskLeaseTx(ctx, tx, lease)
}

func (c *PostgresCoordinator) completeIngestBatchItemsTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
	items []IngestBatchCompletion,
	defaultVersion int64,
) error {
	for _, item := range items {
		request := HeadPublishRequest{
			TenantID:       tenantID,
			CommitID:       item.CommitID,
			IdempotencyKey: item.IdempotencyKey,
			RequestHash:    item.RequestHash,
			OwnerToken:     item.OwnerToken,
			Result:         item.Result,
			CollectorState: item.CollectorState,
		}
		if err := c.completeIdempotencyTx(ctx, tx, request); err != nil {
			return err
		}
		if err := c.upsertCollectorStateTx(ctx, tx, tenantID, item.CollectorState, defaultVersion); err != nil {
			return err
		}
	}
	return nil
}

func (c *PostgresCoordinator) resolveAmbiguousIngestBatchCompletion(
	request IngestBatchPublishRequest,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, item := range request.Items {
		reservation, err := c.loadCommitReservation(ctx, request.Head.TenantID, item.IdempotencyKey)
		if err != nil {
			return false, err
		}
		if !reservation.Committed || reservation.RequestHash != item.RequestHash {
			return false, fmt.Errorf("%w: PostgreSQL ingest batch completion outcome is unknown", ErrCoordinatorUnavailable)
		}
	}
	return true, nil
}
