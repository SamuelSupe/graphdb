package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) Reachability(
	ctx context.Context,
	tenantID string,
) (CoordinatorReachability, error) {
	head, exists, err := c.Head(ctx, tenantID)
	if err != nil {
		return CoordinatorReachability{}, err
	}
	if !exists {
		return CoordinatorReachability{}, ErrCoordinatorHeadMissing
	}
	roots := CoordinatorReachability{
		Head:             head,
		ManifestKeys:     map[string]struct{}{head.ManifestKey: {}},
		WriteContextKeys: map[string]struct{}{},
	}
	if head.WriteContextKey != "" {
		roots.WriteContextKeys[head.WriteContextKey] = struct{}{}
	}
	rows, err := c.pool.Query(ctx,
		`SELECT manifest_key
		 FROM `+c.table("legacy_manifest_outbox")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND status <> 'done'`,
		c.namespace, tenantID,
	)
	if err != nil {
		return CoordinatorReachability{}, coordinatorUnavailable(err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return CoordinatorReachability{}, coordinatorUnavailable(err)
		}
		roots.ManifestKeys[key] = struct{}{}
		roots.PendingLegacy++
	}
	if err := rows.Err(); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return CoordinatorReachability{}, coordinatorUnavailable(err)
	}
	return roots, nil
}
