package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type coordinatorHeadScanner interface {
	Scan(...any) error
}

const coordinatorHeadColumns = `tenant_id, generation, status, head_revision, graph_version,
	manifest_key, manifest_hash, commit_id, write_context_revision,
	write_context_key, write_context_hash,
	legacy_manifest_revision, updated_at`

func scanCoordinationHead(row coordinatorHeadScanner) (CoordinationHead, error) {
	var head CoordinationHead
	err := row.Scan(
		&head.TenantID,
		&head.Generation,
		&head.Status,
		&head.Revision,
		&head.GraphVersion,
		&head.ManifestKey,
		&head.ManifestHash,
		&head.CommitID,
		&head.WriteContextRevision,
		&head.WriteContextKey,
		&head.WriteContextHash,
		&head.LegacyManifestRevision,
		&head.UpdatedAt,
	)
	return head, err
}

func (c *PostgresCoordinator) Head(ctx context.Context, tenantID string) (CoordinationHead, bool, error) {
	head, err := scanCoordinationHead(c.pool.QueryRow(ctx,
		`SELECT `+coordinatorHeadColumns+` FROM `+c.table("tenant_heads")+`
		 WHERE namespace = $1 AND tenant_id = $2`,
		c.namespace, tenantID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinationHead{}, false, nil
	}
	if err != nil {
		return CoordinationHead{}, false, coordinatorUnavailable(err)
	}
	return head, true, nil
}

func (c *PostgresCoordinator) BootstrapHead(ctx context.Context, head CoordinationHead, legacyMirrored bool) error {
	if head.TenantID == "" || head.ManifestKey == "" || head.ManifestHash == "" {
		return fmt.Errorf("tenant id, manifest key, and manifest hash are required")
	}
	if head.Generation <= 0 {
		head.Generation = 1
	}
	if head.Status == "" {
		head.Status = "active"
	}
	if head.Revision <= 0 {
		head.Revision = 1
	}
	if head.UpdatedAt.IsZero() {
		head.UpdatedAt = time.Now().UTC()
	}
	mirrorRevision := int64(0)
	if legacyMirrored {
		mirrorRevision = head.Revision
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	tag, err := tx.Exec(ctx,
		`INSERT INTO `+c.table("tenant_heads")+` (
			namespace, tenant_id, generation, status, head_revision, graph_version,
			manifest_key, manifest_hash, commit_id, write_context_revision,
			write_context_key, write_context_hash, legacy_manifest_revision, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (namespace, tenant_id) DO NOTHING`,
		c.namespace,
		head.TenantID,
		head.Generation,
		head.Status,
		head.Revision,
		head.GraphVersion,
		head.ManifestKey,
		head.ManifestHash,
		head.CommitID,
		head.WriteContextRevision,
		head.WriteContextKey,
		head.WriteContextHash,
		mirrorRevision,
		head.UpdatedAt,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		current, err := scanCoordinationHead(tx.QueryRow(ctx,
			`SELECT `+coordinatorHeadColumns+` FROM `+c.table("tenant_heads")+`
			 WHERE namespace = $1 AND tenant_id = $2`,
			c.namespace, head.TenantID,
		))
		if err != nil {
			return coordinatorUnavailable(err)
		}
		if current.GraphVersion != head.GraphVersion ||
			current.ManifestHash != head.ManifestHash ||
			current.ManifestKey != head.ManifestKey {
			return fmt.Errorf("%w: tenant %q already has a different coordinator head", ErrConflict, head.TenantID)
		}
		return coordinatorUnavailable(tx.Commit(ctx))
	}
	if !legacyMirrored {
		if err := c.insertLegacyManifestJob(
			ctx, tx, head.TenantID, head.Revision, head.GraphVersion,
			head.ManifestKey, head.ManifestHash, head.CommitID,
		); err != nil {
			return err
		}
	}
	if err := c.enqueueDerivedIndexes(ctx, tx, head.TenantID, head.GraphVersion); err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO `+c.table("cluster_modes")+` (namespace, mode, updated_at)
		 VALUES ($1,$2,now())
		 ON CONFLICT (namespace) DO NOTHING`,
		c.namespace, CoordinationPostgres,
	)
	if err != nil {
		return coordinatorUnavailable(err)
	}
	return coordinatorUnavailable(tx.Commit(ctx))
}

func (c *PostgresCoordinator) PublishHead(ctx context.Context, request HeadPublishRequest) (CoordinationHead, bool, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return CoordinationHead{}, false, coordinatorUnavailable(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := c.requirePostgresModeTx(ctx, tx); err != nil {
		return CoordinationHead{}, false, err
	}

	head, published, err := c.publishHeadTx(ctx, tx, request)
	if err != nil {
		return CoordinationHead{}, false, err
	}
	if !published {
		_ = tx.Rollback(ctx)
		current, _, headErr := c.Head(ctx, request.TenantID)
		return current, false, headErr
	}
	if err := c.completeIdempotencyTx(ctx, tx, request); err != nil {
		return CoordinationHead{}, false, err
	}
	if err := c.upsertCollectorStateTx(ctx, tx, request.TenantID, request.CollectorState, head.GraphVersion); err != nil {
		return CoordinationHead{}, false, err
	}
	if err := c.insertLegacyManifestJob(
		ctx, tx, request.TenantID, head.Revision, head.GraphVersion,
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

func (c *PostgresCoordinator) publishHeadTx(ctx context.Context, tx pgx.Tx, request HeadPublishRequest) (CoordinationHead, bool, error) {
	now := time.Now().UTC()
	if request.ExpectedRevision == 0 {
		contextRevision, err := initialHeadWriteContextRevision(request)
		if err != nil {
			return CoordinationHead{}, false, err
		}
		head, err := scanCoordinationHead(tx.QueryRow(ctx,
			`INSERT INTO `+c.table("tenant_heads")+` (
				namespace, tenant_id, generation, status, head_revision, graph_version,
				manifest_key, manifest_hash, commit_id, write_context_revision,
				write_context_key, write_context_hash, legacy_manifest_revision, updated_at
			) VALUES ($1,$2,1,'active',1,$3,$4,$5,$6,$7,$8,$9,0,$10)
			ON CONFLICT (namespace, tenant_id) DO NOTHING
			RETURNING `+coordinatorHeadColumns,
			c.namespace,
			request.TenantID,
			request.GraphVersion,
			request.ManifestKey,
			request.ManifestHash,
			request.CommitID,
			contextRevision,
			request.InitialWriteContextKey,
			request.InitialWriteContextHash,
			now,
		))
		if errors.Is(err, pgx.ErrNoRows) {
			return CoordinationHead{}, false, nil
		}
		if err != nil {
			return CoordinationHead{}, false, coordinatorUnavailable(err)
		}
		return head, true, nil
	}
	if request.InitialWriteContextKey != "" ||
		request.InitialWriteContextHash != "" {
		return CoordinationHead{}, false, fmt.Errorf(
			"initial write-context is only valid for a new tenant head",
		)
	}

	head, err := scanCoordinationHead(tx.QueryRow(ctx,
		`UPDATE `+c.table("tenant_heads")+`
		 SET head_revision = head_revision + 1,
		     graph_version = $6,
		     manifest_key = $7,
		     manifest_hash = $8,
		     commit_id = $9,
		     updated_at = $10
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
		request.ExpectedWriteContextRevision,
		request.GraphVersion,
		request.ManifestKey,
		request.ManifestHash,
		request.CommitID,
		now,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinationHead{}, false, nil
	}
	if err != nil {
		return CoordinationHead{}, false, coordinatorUnavailable(err)
	}
	return head, true, nil
}

func initialHeadWriteContextRevision(
	request HeadPublishRequest,
) (int64, error) {
	hasKey := request.InitialWriteContextKey != ""
	hasHash := request.InitialWriteContextHash != ""
	if hasKey != hasHash {
		return 0, fmt.Errorf(
			"initial write-context key and hash must be provided together",
		)
	}
	if hasKey {
		return 1, nil
	}
	return 0, nil
}

func (c *PostgresCoordinator) insertLegacyManifestJob(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
	revision int64,
	graphVersion int64,
	key string,
	hash string,
	commitID string,
) error {
	query := `INSERT INTO ` + c.table("legacy_manifest_outbox") + ` (
		namespace, tenant_id, head_revision, graph_version, manifest_key, manifest_hash, commit_id
	) VALUES ($1,$2,$3,$4,$5,$6,$7)
	ON CONFLICT (namespace, tenant_id, head_revision) DO NOTHING`
	var err error
	if tx != nil {
		_, err = tx.Exec(ctx, query, c.namespace, tenantID, revision, graphVersion, key, hash, commitID)
	} else {
		_, err = c.pool.Exec(ctx, query, c.namespace, tenantID, revision, graphVersion, key, hash, commitID)
	}
	return coordinatorUnavailable(err)
}

func marshalCommitResult(result CommitResult) (json.RawMessage, error) {
	data, err := json.Marshal(result)
	return json.RawMessage(data), err
}
