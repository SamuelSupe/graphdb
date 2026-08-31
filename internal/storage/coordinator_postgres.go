package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const coordinatorSchemaVersion = 5

var postgresIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type PostgresCoordinator struct {
	pool      *pgxpool.Pool
	schema    string
	namespace string
}

func NewPostgresCoordinator(ctx context.Context, dsn, schema, namespace string) (*PostgresCoordinator, error) {
	coordinator, err := newPostgresCoordinator(ctx, dsn, schema, namespace)
	if err != nil {
		return nil, err
	}
	if err := coordinator.pool.Ping(ctx); err != nil {
		coordinator.pool.Close()
		return nil, coordinatorUnavailable(err)
	}
	return coordinator, nil
}

// NewPostgresCoordinatorLazy constructs a coordinator without requiring an
// initial connection. WAL writers use it so recovered owner routes and local
// admission can start while PostgreSQL is temporarily unavailable.
func NewPostgresCoordinatorLazy(ctx context.Context, dsn, schema, namespace string) (*PostgresCoordinator, error) {
	return newPostgresCoordinator(ctx, dsn, schema, namespace)
}

func newPostgresCoordinator(ctx context.Context, dsn, schema, namespace string) (*PostgresCoordinator, error) {
	dsn = strings.TrimSpace(dsn)
	namespace = strings.TrimSpace(namespace)
	if dsn == "" {
		return nil, fmt.Errorf("GRAPHDB_POSTGRES_DSN is required")
	}
	if namespace == "" {
		return nil, fmt.Errorf("GRAPHDB_COORDINATOR_NAMESPACE is required")
	}
	if schema == "" {
		schema = "graphdb_coordination"
	}
	if !postgresIdentifierPattern.MatchString(schema) {
		return nil, fmt.Errorf("invalid PostgreSQL schema %q", schema)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, coordinatorUnavailable(err)
	}
	return &PostgresCoordinator{pool: pool, schema: schema, namespace: namespace}, nil
}

func (c *PostgresCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c *PostgresCoordinator) Namespace() string {
	return c.namespace
}

func (c *PostgresCoordinator) Close() {
	if c != nil && c.pool != nil {
		c.pool.Close()
	}
}

func (c *PostgresCoordinator) table(name string) string {
	return `"` + c.schema + `"."` + name + `"`
}

func coordinatorUnavailable(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrCoordinatorUnavailable, err)
}
