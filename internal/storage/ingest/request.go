package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/identity"
)

func PrepareIngestRequest(tenantID string, request IngestRequest) (IngestRequest, error) {
	return PrepareRequestWithPathValidation(tenantID, request, true)
}

// PrepareRequestWithPathValidation retains legacy replay validation when validatePaths
// is false. New submissions must validate their eventual storage paths.
func PrepareRequestWithPathValidation(tenantID string, request IngestRequest, validatePaths bool) (IngestRequest, error) {
	if err := identity.ValidateTenantID(tenantID); err != nil {
		return IngestRequest{}, err
	}
	request = normalizeIngestRequest(request)
	if request.Source == "" || request.CollectorID == "" {
		return IngestRequest{}, fmt.Errorf("%w: source and collector_id are required", ErrInvalidArgument)
	}
	if err := normalizeIngestTransactionalOptions(&request); err != nil {
		return IngestRequest{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if request.BatchID == "" {
		batchID, err := defaultIngestBatchID(request)
		if err != nil {
			return IngestRequest{}, fmt.Errorf("generate ingest batch_id: %w", err)
		}
		request.BatchID = batchID
	}
	for _, identifier := range []struct {
		name  string
		value string
	}{
		{name: "source", value: request.Source},
		{name: "collector_id", value: request.CollectorID},
		{name: "batch_id", value: request.BatchID},
	} {
		if !validatePaths {
			if identifier.value == "." || identifier.value == ".." {
				return IngestRequest{}, fmt.Errorf("%w: %s must not be a dot path segment", ErrInvalidArgument, identifier.name)
			}
			continue
		}
		if err := ValidateStatusPathIdentifier(identifier.name, identifier.value); err != nil {
			return IngestRequest{}, err
		}
	}
	if validatePaths && request.IdempotencyKey != "" {
		if err := ValidateStatusPathIdentifier("idempotency_key", request.IdempotencyKey); err != nil {
			return IngestRequest{}, err
		}
	}
	if _, err := json.Marshal(request); err != nil {
		return IngestRequest{}, fmt.Errorf("encode ingest request: %w", err)
	}
	return request, nil
}

func ValidateStatusPathIdentifier(name string, value string) error {
	if name != "idempotency_key" && (value == "." || value == "..") {
		return fmt.Errorf("%w: %s must not be a dot path segment", ErrInvalidArgument, name)
	}
	limit := 255
	if name == "collector_id" || name == "batch_id" || name == "idempotency_key" {
		limit -= len(".parquet")
	}
	if strings.ContainsRune(value, 0) || len(identity.ObjectSegment(value)) > limit {
		return fmt.Errorf("%w: %s must contain no NUL and fit in %d escaped path bytes", ErrInvalidArgument, name, limit)
	}
	return nil
}

// ValidateIngestStorageIdentifiers checks path component limits for normalized
// identifiers, including the suffixes used by batch and collector records.
func ValidateIngestStorageIdentifiers(request IngestRequest) error {
	for _, id := range []struct{ name, value string }{
		{"source", request.Source}, {"collector_id", request.CollectorID},
		{"batch_id", request.BatchID}, {"idempotency_key", request.IdempotencyKey},
	} {
		if err := ValidateStatusPathIdentifier(id.name, id.value); err != nil {
			return err
		}
	}
	return nil
}

func normalizeIngestRequest(request IngestRequest) IngestRequest {
	request.Source = strings.TrimSpace(request.Source)
	request.CollectorID = strings.TrimSpace(request.CollectorID)
	request.BatchID = strings.TrimSpace(request.BatchID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.StaleAction = strings.TrimSpace(request.StaleAction)
	request.StaleKind = strings.TrimSpace(request.StaleKind)
	return request
}

func defaultIngestBatchID(request IngestRequest) (string, error) {
	if request.IdempotencyKey != "" {
		sum := sha256.Sum256([]byte(request.Source + "\x00" + request.CollectorID + "\x00" + request.IdempotencyKey))
		return request.Source + "-" + request.CollectorID + "-" + hex.EncodeToString(sum[:])[:16], nil
	}
	nonce, err := identity.NewCommitID()
	if err != nil {
		return "", err
	}
	return request.Source + "-" + request.CollectorID + "-" + nonce, nil
}
