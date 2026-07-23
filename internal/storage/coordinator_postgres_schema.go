package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) Migrate(ctx context.Context) error {
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS "` + c.schema + `"`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("schema_version") + ` (
			version integer PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("tenant_heads") + ` (
			namespace text NOT NULL,
			tenant_id text NOT NULL,
			generation bigint NOT NULL,
			status text NOT NULL,
			head_revision bigint NOT NULL,
			graph_version bigint NOT NULL,
			manifest_key text NOT NULL,
			manifest_hash text NOT NULL,
			commit_id text NOT NULL DEFAULT '',
			write_context_revision bigint NOT NULL DEFAULT 0,
			write_context_key text NOT NULL DEFAULT '',
			write_context_hash text NOT NULL DEFAULT '',
			legacy_manifest_revision bigint NOT NULL DEFAULT 0,
			updated_at timestamptz NOT NULL,
			PRIMARY KEY (namespace, tenant_id),
			CHECK (generation > 0),
			CHECK (head_revision > 0),
			CHECK (graph_version >= 0),
			CHECK (write_context_revision >= 0),
			CHECK (legacy_manifest_revision >= 0 AND legacy_manifest_revision <= head_revision)
		)`,
		`ALTER TABLE ` + c.table("tenant_heads") + `
			ADD COLUMN IF NOT EXISTS write_context_key text NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + c.table("tenant_heads") + `
			ADD COLUMN IF NOT EXISTS write_context_hash text NOT NULL DEFAULT ''`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("commit_idempotency") + ` (
			namespace text NOT NULL,
			tenant_id text NOT NULL,
			idempotency_key text NOT NULL,
			request_hash text NOT NULL,
			owner_token text NOT NULL,
			status text NOT NULL,
			result_json jsonb,
			candidate_commit_id text NOT NULL DEFAULT '',
			started_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			PRIMARY KEY (namespace, tenant_id, idempotency_key)
		)`,
		`CREATE INDEX IF NOT EXISTS graphdb_commit_idempotency_retention
			ON ` + c.table("commit_idempotency") + ` (namespace, status, updated_at)
			WHERE status IN ('committed', 'pending')`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("collector_state") + ` (
			namespace text NOT NULL,
			tenant_id text NOT NULL,
			source text NOT NULL,
			collector_id text NOT NULL,
			last_batch_id text NOT NULL DEFAULT '',
			last_cursor text NOT NULL DEFAULT '',
			last_version bigint NOT NULL DEFAULT 0,
			updated_at timestamptz NOT NULL,
			PRIMARY KEY (namespace, tenant_id, source, collector_id)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("task_leases") + ` (
			namespace text NOT NULL,
			tenant_id text NOT NULL,
			task_type text NOT NULL,
			owner_token text NOT NULL,
			fence_epoch bigint NOT NULL,
			expires_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			PRIMARY KEY (namespace, tenant_id, task_type)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("derived_tasks") + ` (
			namespace text NOT NULL,
			tenant_id text NOT NULL,
			task_type text NOT NULL,
			target_version bigint NOT NULL,
			processed_version bigint NOT NULL DEFAULT 0,
			status text NOT NULL DEFAULT 'pending',
			owner_token text NOT NULL DEFAULT '',
			attempts integer NOT NULL DEFAULT 0,
			lease_until timestamptz,
			next_attempt_at timestamptz NOT NULL DEFAULT now(),
			last_error text NOT NULL DEFAULT '',
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (namespace, tenant_id, task_type)
		)`,
		`CREATE INDEX IF NOT EXISTS graphdb_derived_tasks_ready
			ON ` + c.table("derived_tasks") + ` (namespace, status, next_attempt_at, tenant_id)`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("legacy_manifest_outbox") + ` (
			namespace text NOT NULL,
			tenant_id text NOT NULL,
			head_revision bigint NOT NULL,
			graph_version bigint NOT NULL,
			manifest_key text NOT NULL,
			manifest_hash text NOT NULL,
			commit_id text NOT NULL DEFAULT '',
			status text NOT NULL DEFAULT 'pending',
			owner_token text NOT NULL DEFAULT '',
			attempts integer NOT NULL DEFAULT 0,
			lease_until timestamptz,
			next_attempt_at timestamptz NOT NULL DEFAULT now(),
			last_error text NOT NULL DEFAULT '',
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (namespace, tenant_id, head_revision)
		)`,
		`ALTER TABLE ` + c.table("legacy_manifest_outbox") + `
			ADD COLUMN IF NOT EXISTS commit_id text NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS graphdb_legacy_manifest_outbox_ready
			ON ` + c.table("legacy_manifest_outbox") + ` (namespace, status, next_attempt_at, tenant_id, head_revision)`,
		`CREATE INDEX IF NOT EXISTS graphdb_legacy_manifest_outbox_cleanup
			ON ` + c.table("legacy_manifest_outbox") + ` (namespace, updated_at, tenant_id, head_revision)
			WHERE status = 'done'`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("cluster_modes") + ` (
			namespace text PRIMARY KEY,
			mode text NOT NULL,
			updated_at timestamptz NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := c.pool.Exec(ctx, statement); err != nil {
			return coordinatorUnavailable(err)
		}
	}
	if _, err := c.pool.Exec(ctx,
		`INSERT INTO `+c.table("schema_version")+` (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`,
		coordinatorSchemaVersion,
	); err != nil {
		return coordinatorUnavailable(err)
	}
	if _, err := c.pool.Exec(ctx,
		`INSERT INTO `+c.table("cluster_modes")+` (namespace, mode, updated_at)
		 VALUES ($1,$2,now()) ON CONFLICT (namespace) DO NOTHING`,
		c.namespace, CoordinationPostgres,
	); err != nil {
		return coordinatorUnavailable(err)
	}
	return nil
}

func (c *PostgresCoordinator) CheckSchema(ctx context.Context) error {
	var version int
	err := c.pool.QueryRow(ctx,
		`SELECT COALESCE(max(version), 0) FROM `+c.table("schema_version"),
	).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCoordinatorSchemaRequired
	}
	if err != nil {
		if isPostgresUndefinedTable(err) {
			return ErrCoordinatorSchemaRequired
		}
		return coordinatorUnavailable(err)
	}
	if version == 0 {
		return ErrCoordinatorSchemaRequired
	}
	if version != coordinatorSchemaVersion {
		return fmt.Errorf("%w: found version %d, need %d", ErrCoordinatorSchemaRequired, version, coordinatorSchemaVersion)
	}
	return nil
}

func isPostgresUndefinedTable(err error) bool {
	type sqlState interface {
		SQLState() string
	}
	var state sqlState
	return errors.As(err, &state) && state.SQLState() == "42P01"
}
