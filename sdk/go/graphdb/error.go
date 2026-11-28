package graphdb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	ErrorCodeBadRequest              = "bad_request"
	ErrorCodeInvalidTenant           = "invalid_tenant"
	ErrorCodeTenantRequired          = "tenant_required"
	ErrorCodeTenantDisabled          = "tenant_disabled"
	ErrorCodeTenantDeleted           = "tenant_deleted"
	ErrorCodeOperationDisabled       = "operation_disabled"
	ErrorCodeNotFound                = "not_found"
	ErrorCodeMethodNotAllowed        = "method_not_allowed"
	ErrorCodeRequestTooLarge         = "request_too_large"
	ErrorCodeTooManyRequests         = "too_many_requests"
	ErrorCodeInternal                = "internal_error"
	ErrorCodeInvalidJSON             = "invalid_json"
	ErrorCodeInvalidQuery            = "invalid_query"
	ErrorCodeQueryLimitExceeded      = "query_limit_exceeded"
	ErrorCodeMutationBudget          = "mutation_budget_exceeded"
	ErrorCodeIndexStale              = "index_stale"
	ErrorCodeReaderNotFresh          = "reader_not_fresh"
	ErrorCodeQuotaExceeded           = "quota_exceeded"
	ErrorCodeLeaseHeld               = "lease_held"
	ErrorCodeManifestCASConflict     = "manifest_cas_conflict"
	ErrorCodeObjectWriteConflict     = "object_write_conflict"
	ErrorCodeObjectStoreUnavailable  = "object_store_unavailable"
	ErrorCodeTaskConflict            = "task_conflict"
	ErrorCodeRepairRequired          = "repair_required"
	ErrorCodeVersionConflict         = "version_conflict"
	ErrorCodePreconditionFailed      = "precondition_failed"
	ErrorCodeAtomicValidationFailed  = "atomic_validation_failed"
	ErrorCodeAtomicSuppressed        = "atomic_suppressed"
	ErrorCodeIdempotencyConflict     = "idempotency_conflict"
	ErrorCodeIdempotencyInProgress   = "idempotency_in_progress"
	ErrorCodeWriteConflict           = "write_conflict"
	ErrorCodeCoordinatorUnavailable  = "coordinator_unavailable"
	ErrorCodeCommitTailTooLong       = "commit_tail_too_long"
	ErrorCodeIndexRebuildRunning     = "index_rebuild_running"
	ErrorCodeMaintenanceTaskRunning  = "maintenance_task_running"
	ErrorCodeWriteAdmissionTimeout   = "write_admission_queue_timeout"
	ErrorCodeWriteBackpressure       = "write_backpressure"
	ErrorCodeRequestTimeout          = "request_timeout"
	ErrorCodeRaftUnavailable         = "raft_unavailable"
	ErrorCodeWriteOutcomeUnknown     = "write_outcome_unknown"
	ErrorCodeTenantGenerationChanged = "tenant_generation_changed"
	ErrorCodeIngestQueueFull         = "ingest_queue_full"
	ErrorCodeRequestCanceled         = "request_canceled"
)

type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	Detail     map[string]any
	RawDetail  any
	Reasons    []map[string]any
	Body       []byte
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code != "" && e.Message != "" {
		return fmt.Sprintf("graphdb: status=%d code=%s message=%s", e.StatusCode, e.Code, e.Message)
	}
	if e.Message != "" {
		return fmt.Sprintf("graphdb: status=%d message=%s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("graphdb: status=%d", e.StatusCode)
}

func parseAPIError(resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	apiErr := apiErrorFromBody(resp.StatusCode, data)
	if apiErr.RetryAfter <= 0 {
		if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After")); retryAfter > 0 {
			apiErr.RetryAfter = retryAfter
		}
	}
	return apiErr
}

func apiErrorFromBody(status int, data []byte) *APIError {
	envelope := decodeErrorEnvelope(data)
	detailMap, _ := envelope.Detail.(map[string]any)
	apiErr := &APIError{
		StatusCode: status,
		Code:       envelope.Code,
		Message:    envelope.Message,
		Retryable:  envelope.Retryable,
		Detail:     detailMap,
		RawDetail:  envelope.Detail,
		Reasons:    envelope.Reasons,
		Body:       data,
	}
	if envelope.RetryAfterMS > 0 {
		apiErr.RetryAfter = time.Duration(envelope.RetryAfterMS) * time.Millisecond
	}
	return apiErr
}

type errorEnvelope struct {
	Error        string           `json:"error"`
	Code         string           `json:"code"`
	Message      string           `json:"message"`
	Retryable    bool             `json:"retryable"`
	RetryAfterMS int64            `json:"retry_after_ms"`
	Detail       any              `json:"detail"`
	Reasons      []map[string]any `json:"reasons"`
}

func decodeErrorEnvelope(data []byte) errorEnvelope {
	var envelope errorEnvelope
	_ = json.Unmarshal(data, &envelope)
	if envelope.Message == "" {
		envelope.Message = envelope.Error
	}
	if envelope.Message == "" {
		envelope.Message = strings.TrimSpace(string(data))
	}
	return envelope
}

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(raw); err == nil {
		delay := time.Until(when)
		if delay > 0 {
			return delay
		}
	}
	return 0
}
