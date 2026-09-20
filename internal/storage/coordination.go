package storage

import (
	"context"
	"errors"
	"time"
)

const (
	CoordinationLocal    = "local"
	CoordinationPostgres = "postgres"
)

var ErrWriteConflict = errors.New("write conflict")
var ErrVersionConflict = errors.New("version conflict")
var ErrIdempotencyConflict = errors.New("idempotency conflict")
var ErrIdempotencyInProgress = errors.New("idempotency request is in progress")

type CoordinatorStatus struct {
	Backend        string    `json:"backend"`
	Available      bool      `json:"available"`
	SchemaVersion  int       `json:"schema_version"`
	Namespace      string    `json:"namespace,omitempty"`
	Tenants        int64     `json:"tenants"`
	OutboxBacklog  int64     `json:"outbox_backlog"`
	DerivedBacklog int64     `json:"derived_backlog"`
	MaxMirrorLag   int64     `json:"max_legacy_mirror_lag"`
	CheckedAt      time.Time `json:"checked_at"`
	LastError      string    `json:"last_error,omitempty"`
}

type CollectorStateUpdate struct {
	Source      string
	CollectorID string
	BatchID     string
	Cursor      string
	Version     int64
}

func (s *TenantStore) CoordinationBackend() string { return CoordinationLocal }
func (s *TenantStore) CoordinatorStatus(context.Context) CoordinatorStatus {
	return localCoordinatorStatus()
}
func (s *TenantStore) CachedCoordinatorStatus() CoordinatorStatus { return localCoordinatorStatus() }
func localCoordinatorStatus() CoordinatorStatus {
	return CoordinatorStatus{Backend: CoordinationLocal, Available: true, CheckedAt: time.Now().UTC()}
}
