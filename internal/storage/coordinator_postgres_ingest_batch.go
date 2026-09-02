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
	keys := make([]string, 0, len(items))
	hashes := make([]string, 0, len(items))
	owners := make([]string, 0, len(items))
	results := make([]string, 0, len(items))
	commitIDs := make([]string, 0, len(items))
	type collectorKey struct {
		source      string
		collectorID string
	}
	collectorUpdates := make(map[collectorKey]CollectorStateUpdate)
	collectorOrder := make([]collectorKey, 0, len(items))
	for _, item := range items {
		if item.IdempotencyKey != "" {
			result := string(item.Result)
			if result == "" {
				result = `{}`
			}
			keys = append(keys, item.IdempotencyKey)
			hashes = append(hashes, item.RequestHash)
			owners = append(owners, item.OwnerToken)
			results = append(results, result)
			commitIDs = append(commitIDs, item.CommitID)
		}
		if item.CollectorState == nil {
			continue
		}
		update := *item.CollectorState
		if update.Version <= 0 {
			update.Version = defaultVersion
		}
		key := collectorKey{source: update.Source, collectorID: update.CollectorID}
		current, exists := collectorUpdates[key]
		if !exists {
			collectorOrder = append(collectorOrder, key)
		}
		if !exists || update.Version >= current.Version {
			collectorUpdates[key] = update
		}
	}
	if len(keys) == 0 && len(collectorOrder) == 0 {
		return nil
	}
	sources := make([]string, 0, len(collectorOrder))
	collectors := make([]string, 0, len(collectorOrder))
	batchIDs := make([]string, 0, len(collectorOrder))
	cursors := make([]string, 0, len(collectorOrder))
	versions := make([]int64, 0, len(collectorOrder))
	for _, key := range collectorOrder {
		update := collectorUpdates[key]
		sources = append(sources, update.Source)
		collectors = append(collectors, update.CollectorID)
		batchIDs = append(batchIDs, update.BatchID)
		cursors = append(cursors, update.Cursor)
		versions = append(versions, update.Version)
	}
	var completed int64
	err := tx.QueryRow(ctx,
		`WITH completed_idempotency AS (
			UPDATE `+c.table("commit_idempotency")+` AS current
			SET status = 'committed', result_json = input.result_json::jsonb,
			    candidate_commit_id = input.commit_id, updated_at = now()
			FROM unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
			     AS input(idempotency_key, request_hash, owner_token, result_json, commit_id)
			WHERE current.namespace = $1 AND current.tenant_id = $2
			  AND current.idempotency_key = input.idempotency_key
			  AND current.request_hash = input.request_hash
			  AND current.owner_token = input.owner_token
			  AND current.status = 'pending'
			RETURNING 1
		), upserted_collectors AS (
			INSERT INTO `+c.table("collector_state")+` AS current (
				namespace, tenant_id, source, collector_id,
				last_batch_id, last_cursor, last_version, updated_at
			)
			SELECT $1, $2, input.source, input.collector_id,
			       input.batch_id, input.cursor, input.version, now()
			FROM unnest($8::text[], $9::text[], $10::text[], $11::text[], $12::bigint[])
			     AS input(source, collector_id, batch_id, cursor, version)
			ON CONFLICT (namespace, tenant_id, source, collector_id) DO UPDATE
			SET last_batch_id = EXCLUDED.last_batch_id,
			    last_cursor = EXCLUDED.last_cursor,
			    last_version = EXCLUDED.last_version,
			    updated_at = EXCLUDED.updated_at
			WHERE EXCLUDED.last_version >= current.last_version
			RETURNING 1
		)
		SELECT count(*) FROM completed_idempotency`,
		c.namespace, tenantID, keys, hashes, owners, results, commitIDs,
		sources, collectors, batchIDs, cursors, versions,
	).Scan(&completed)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if completed != int64(len(keys)) {
		return ErrIdempotencyInProgress
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
