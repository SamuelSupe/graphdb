package ingest

import "errors"

var ErrVersionConflict = errors.New("version conflict")

var ErrIdempotencyConflict = errors.New("idempotency conflict")

var ErrIngestIdentityConflict = errors.New("ingest identity conflict")

// ErrInvalidArgument identifies invalid ingestion input.
var ErrInvalidArgument = errors.New("invalid argument")
