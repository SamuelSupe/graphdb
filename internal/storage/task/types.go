// Package task defines the async and replicated maintenance task contracts,
// status lifecycle predicates, and type identifiers.
package task

import "time"

const (
	TypeCompact            = "compact"
	TypeGC                 = "gc"
	TypeRepair             = "repair"
	TypeExportSnapshot     = "export_snapshot"
	TypeReplayDeadLetter   = "replay_deadletters"
	TypeIndexRebuild       = "index_rebuild"
	TypeTenantBackup       = "tenant_backup"
	TypeTenantRestore      = "tenant_restore"
	TypeTenantRestoreDrill = "tenant_restore_drill"
)

const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusCanceled  = "canceled"
	StatusFailed    = "failed"
	StatusSucceeded = "succeeded"
)

type Task struct {
	ID                string         `json:"id"`
	TenantID          string         `json:"tenant_id"`
	Type              string         `json:"type"`
	Status            string         `json:"status"`
	Phase             string         `json:"phase,omitempty"`
	ProgressCompleted int            `json:"progress_completed,omitempty"`
	ProgressTotal     int            `json:"progress_total,omitempty"`
	OwnerID           string         `json:"owner_id,omitempty"`
	Params            map[string]any `json:"params,omitempty"`
	Checkpoint        map[string]any `json:"checkpoint,omitempty"`
	Result            map[string]any `json:"result,omitempty"`
	ResultKey         string         `json:"result_key,omitempty"`
	Error             string         `json:"error,omitempty"`
	StartedAt         time.Time      `json:"started_at"`
	UpdatedAt         time.Time      `json:"updated_at,omitempty"`
	FinishedAt        time.Time      `json:"finished_at,omitempty"`
}

type ListOptions struct {
	Type   string
	Status string
	Limit  int
}

func IsTerminal(status string) bool {
	switch status {
	case StatusCanceled, StatusFailed, StatusSucceeded:
		return true
	default:
		return false
	}
}
