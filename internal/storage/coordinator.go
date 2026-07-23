package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	CoordinationLocal                = "local"
	CoordinationPostgres             = "postgres"
	CoordinationDraining             = "draining"
	coordinatorPendingReservationTTL = 3 * time.Minute
	derivedTaskDebounce              = 250 * time.Millisecond
)

var ErrCoordinatorUnavailable = errors.New("coordinator unavailable")
var ErrCoordinatorSchemaRequired = errors.New("coordinator schema is not initialized")
var ErrCoordinatorHeadMissing = errors.New("coordinator tenant head is missing")
var ErrCoordinatorFenced = errors.New("coordinator is fenced")
var ErrWriteConflict = errors.New("write conflict")
var ErrVersionConflict = errors.New("version conflict")
var ErrIdempotencyInProgress = errors.New("idempotency request is in progress")
var ErrTaskLeaseHeld = errors.New("coordinator task lease is held")

type CoordinationHead struct {
	TenantID               string    `json:"tenant_id"`
	Generation             int64     `json:"generation"`
	Status                 string    `json:"status"`
	Revision               int64     `json:"head_revision"`
	GraphVersion           int64     `json:"graph_version"`
	ManifestKey            string    `json:"manifest_key"`
	ManifestHash           string    `json:"manifest_hash"`
	CommitID               string    `json:"commit_id,omitempty"`
	WriteContextRevision   int64     `json:"write_context_revision"`
	WriteContextKey        string    `json:"write_context_key,omitempty"`
	WriteContextHash       string    `json:"write_context_hash,omitempty"`
	LegacyManifestRevision int64     `json:"legacy_manifest_revision"`
	UpdatedAt              time.Time `json:"updated_at"`
}

type CommitReservation struct {
	Key         string
	RequestHash string
	OwnerToken  string
	Result      json.RawMessage
	Committed   bool
}

type HeadPublishRequest struct {
	TenantID                     string
	ExpectedRevision             int64
	ExpectedGeneration           int64
	ExpectedWriteContextRevision int64
	GraphVersion                 int64
	ManifestKey                  string
	ManifestHash                 string
	CommitID                     string
	IdempotencyKey               string
	RequestHash                  string
	OwnerToken                   string
	Result                       json.RawMessage
	CollectorState               *CollectorStateUpdate
}

type WriteContextPublishRequest struct {
	TenantID           string
	ExpectedRevision   int64
	ExpectedGeneration int64
	ExpectedContext    int64
	WriteContextKey    string
	WriteContextHash   string
}

type LegacyManifestJob struct {
	TenantID     string
	Generation   int64
	HeadRevision int64
	GraphVersion int64
	ManifestKey  string
	ManifestHash string
	OwnerToken   string
	Attempts     int
}

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

type CoordinatorCleanupConfig struct {
	IdempotencyRetention  time.Duration
	PendingReservationTTL time.Duration
	OutboxRetention       time.Duration
	Interval              time.Duration
	BatchSize             int
}

type CoordinatorCleanupReport struct {
	IdempotencyDeleted int64 `json:"idempotency_deleted"`
	OutboxDeleted      int64 `json:"outbox_deleted"`
}

func DefaultCoordinatorCleanupConfig() CoordinatorCleanupConfig {
	return CoordinatorCleanupConfig{
		IdempotencyRetention:  24 * time.Hour,
		PendingReservationTTL: coordinatorPendingReservationTTL,
		OutboxRetention:       time.Hour,
		Interval:              time.Minute,
		BatchSize:             5000,
	}
}

type CoordinatorReachability struct {
	Head             CoordinationHead
	ManifestKeys     map[string]struct{}
	WriteContextKeys map[string]struct{}
	PendingLegacy    int64
}

type DerivedTaskJob struct {
	TenantID      string
	TaskType      string
	TargetVersion int64
	OwnerToken    string
	Attempts      int
}

type CollectorStateUpdate struct {
	Source      string
	CollectorID string
	BatchID     string
	Cursor      string
	Version     int64
}

type CoordinatorObserver interface {
	RecordCoordinatorCAS(tenantID string, status string)
	RecordCoordinatorHeadRevision(tenantID string, revision int64)
	RecordCoordinatorCleanup(status string, idempotencyDeleted, outboxDeleted int64)
}

type CoordinatorTaskLease struct {
	TenantID   string
	TaskType   string
	OwnerToken string
	FenceEpoch int64
	ExpiresAt  time.Time
}

type WriteCoordinator interface {
	Backend() string
	Namespace() string
	CheckSchema(context.Context) error
	Head(context.Context, string) (CoordinationHead, bool, error)
	BootstrapHead(context.Context, CoordinationHead, bool) error
	ReserveCommit(context.Context, string, string, string, string, time.Duration) (CommitReservation, error)
	RenewCommit(context.Context, string, string, string, string) (bool, error)
	AbortCommit(context.Context, string, string, string, string) error
	PublishHead(context.Context, HeadPublishRequest) (CoordinationHead, bool, error)
	CompleteNoop(context.Context, HeadPublishRequest) (bool, error)
	PublishWriteContext(context.Context, WriteContextPublishRequest) (CoordinationHead, bool, error)
	TransitionTenant(context.Context, string, string, bool) (CoordinationHead, error)
	ActivateTenantHead(context.Context, HeadPublishRequest) (CoordinationHead, bool, error)
	FinalizeTenantPurge(context.Context, string, int64) error
	AcquireTaskLease(context.Context, string, string, string, time.Duration) (CoordinatorTaskLease, bool, error)
	RenewTaskLease(context.Context, CoordinatorTaskLease, time.Duration) (CoordinatorTaskLease, bool, error)
	ReleaseTaskLease(context.Context, CoordinatorTaskLease) error
	ClaimDerivedTask(context.Context, string, time.Duration) (DerivedTaskJob, bool, error)
	RenewDerivedTask(context.Context, DerivedTaskJob, time.Duration) (bool, error)
	CompleteDerivedTask(context.Context, DerivedTaskJob, int64) error
	FailDerivedTask(context.Context, DerivedTaskJob, error) error
	CollectorState(context.Context, string, string, string) (CollectorStateUpdate, bool, error)
	ClaimLegacyManifest(context.Context, string, time.Duration) (LegacyManifestJob, bool, error)
	CompleteLegacyManifest(context.Context, LegacyManifestJob) error
	FailLegacyManifest(context.Context, LegacyManifestJob, error) error
	Cleanup(context.Context, CoordinatorCleanupConfig) (CoordinatorCleanupReport, error)
	Reachability(context.Context, string) (CoordinatorReachability, error)
	Status(context.Context) (CoordinatorStatus, error)
	Close()
}

// CoordinatorModeController provides the namespace fence used by an
// operational rollback from PostgreSQL coordination to local coordination.
type CoordinatorModeController interface {
	CoordinationMode(context.Context) (string, error)
	CompareAndSwapCoordinationMode(context.Context, string, string) (bool, error)
	ListHeads(context.Context) ([]CoordinationHead, error)
}
