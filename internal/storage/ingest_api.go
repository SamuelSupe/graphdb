package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"

type (
	CollectorStatus       = ingest.CollectorStatus
	IngestBatchEntry      = ingest.IngestBatchEntry
	IngestBatchHooks      = ingest.IngestBatchHooks
	IngestBatchRecord     = ingest.IngestBatchRecord
	IngestBatchStats      = ingest.IngestBatchStats
	IngestConflict        = ingest.IngestConflict
	IngestFailure         = ingest.IngestFailure
	IngestItem            = ingest.IngestItem
	IngestPrecondition    = ingest.IngestPrecondition
	IngestPreparedRequest = ingest.IngestPreparedRequest
	IngestRequest         = ingest.IngestRequest
	IngestResult          = ingest.IngestResult
)

const (
	IngestErrorAtomicSuppressed      = ingest.IngestErrorAtomicSuppressed
	IngestErrorAtomicValidation      = ingest.IngestErrorAtomicValidation
	IngestErrorIdempotencyConflict   = ingest.IngestErrorIdempotencyConflict
	IngestErrorPreconditionFailed    = ingest.IngestErrorPreconditionFailed
	IngestErrorVersionConflict       = ingest.IngestErrorVersionConflict
	IngestFailureModeAtomic          = ingest.IngestFailureModeAtomic
	IngestFailureModeBestEffort      = ingest.IngestFailureModeBestEffort
	IngestSkipReasonIdempotentReplay = ingest.IngestSkipReasonIdempotentReplay
	IngestSkipReasonLogicalNoop      = ingest.IngestSkipReasonLogicalNoop
	legacyUnboundIngestGeneration    = ingest.LegacyUnboundIngestGeneration
)

var (
	ErrIdempotencyConflict      = ingest.ErrIdempotencyConflict
	ErrIngestAtomicSuppressed   = ingest.ErrIngestAtomicSuppressed
	ErrIngestAtomicValidation   = ingest.ErrIngestAtomicValidation
	ErrIngestPreconditionFailed = ingest.ErrIngestPreconditionFailed
	ErrIngestRepairRequired     = ingest.ErrIngestRepairRequired
	ErrVersionConflict          = ingest.ErrVersionConflict
	errIngestGenerationFenced   = ingest.ErrIngestGenerationFenced
)

func PrepareIngestRequest(tenantID string, request IngestRequest) (IngestRequest, error) {
	return ingest.PrepareIngestRequest(tenantID, request)
}

// ValidateIngestStorageIdentifiers checks escaped path component limits,
// including the suffixes used by batch and collector records.
func ValidateIngestStorageIdentifiers(request IngestRequest) error {
	return ingest.ValidateIngestStorageIdentifiers(request)
}
