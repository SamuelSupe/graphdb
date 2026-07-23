package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (c *PostgresCoordinator) CoordinationMode(ctx context.Context) (string, error) {
	var mode string
	err := c.pool.QueryRow(ctx,
		`SELECT mode FROM `+c.table("cluster_modes")+` WHERE namespace = $1`,
		c.namespace,
	).Scan(&mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrCoordinatorSchemaRequired
	}
	if err != nil {
		return "", coordinatorUnavailable(err)
	}
	return mode, nil
}

func (c *PostgresCoordinator) CompareAndSwapCoordinationMode(
	ctx context.Context,
	expected string,
	next string,
) (bool, error) {
	if !validCoordinationMode(expected) || !validCoordinationMode(next) {
		return false, fmt.Errorf("invalid coordination mode transition %q -> %q", expected, next)
	}
	tag, err := c.pool.Exec(ctx,
		`UPDATE `+c.table("cluster_modes")+`
		 SET mode = $3, updated_at = now()
		 WHERE namespace = $1 AND mode = $2`,
		c.namespace, expected, next,
	)
	if err != nil {
		return false, coordinatorUnavailable(err)
	}
	return tag.RowsAffected() == 1, nil
}

func (c *PostgresCoordinator) ListHeads(ctx context.Context) ([]CoordinationHead, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT `+coordinatorHeadColumns+` FROM `+c.table("tenant_heads")+`
		 WHERE namespace = $1 ORDER BY tenant_id`,
		c.namespace,
	)
	if err != nil {
		return nil, coordinatorUnavailable(err)
	}
	defer rows.Close()
	heads := []CoordinationHead{}
	for rows.Next() {
		head, err := scanCoordinationHead(rows)
		if err != nil {
			return nil, coordinatorUnavailable(err)
		}
		heads = append(heads, head)
	}
	if err := rows.Err(); err != nil {
		return nil, coordinatorUnavailable(err)
	}
	return heads, nil
}

func (c *PostgresCoordinator) requirePostgresModeTx(ctx context.Context, tx pgx.Tx) error {
	var mode string
	err := tx.QueryRow(ctx,
		`SELECT mode FROM `+c.table("cluster_modes")+`
		 WHERE namespace = $1 FOR SHARE`,
		c.namespace,
	).Scan(&mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCoordinatorFenced
	}
	if err != nil {
		return coordinatorUnavailable(err)
	}
	if mode != CoordinationPostgres {
		return fmt.Errorf("%w: namespace %q is in %q mode", ErrCoordinatorFenced, c.namespace, mode)
	}
	return nil
}

func validCoordinationMode(mode string) bool {
	switch mode {
	case CoordinationLocal, CoordinationPostgres, CoordinationDraining:
		return true
	default:
		return false
	}
}
