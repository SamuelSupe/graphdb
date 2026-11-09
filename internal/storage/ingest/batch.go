package ingest

import (
	"context"
	"errors"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

var (
	ErrIngestRepairRequired   = errors.New("ingest WAL repair required")
	ErrIngestGenerationFenced = errors.New("ingest WAL tenant generation fenced")
)

const LegacyUnboundIngestGeneration int64 = -1

type IngestBatchEntry struct {
	Request            IngestRequest
	AcceptedAt         time.Time
	AcceptedGeneration int64
	Prepared           *IngestPreparedRequest
}

type IngestPreparedRequest struct {
	FlushID                  string        `json:"flush_id"`
	BaseVersion              int64         `json:"base_version"`
	BaseHeadCommitID         string        `json:"base_head_commit_id,omitempty"`
	BaseHeadRevision         int64         `json:"base_head_revision,omitempty"`
	BaseGeneration           int64         `json:"base_generation,omitempty"`
	BaseWriteContextRevision int64         `json:"base_write_context_revision,omitempty"`
	FinalVersion             int64         `json:"final_version"`
	FinalHeadCommitID        string        `json:"final_head_commit_id,omitempty"`
	Result                   IngestResult  `json:"result"`
	Commit                   *graph.Commit `json:"commit,omitempty"`
	DataHash                 string        `json:"data_hash,omitempty"`
	StartedAt                time.Time     `json:"started_at"`
}

type IngestBatchHooks struct {
	Prepared  func(context.Context, []*IngestPreparedRequest) error
	Published func()
	Stats     func(IngestBatchStats)
}

type IngestBatchStats struct {
	LogicalCommits    int
	Segments          int
	ManifestPublishes int
	ExactDedup        int
	CASMerged         int
	Fallback          bool
}
