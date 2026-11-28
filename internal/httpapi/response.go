package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"go.opentelemetry.io/otel/attribute"
)

const (
	ErrorCodeBadRequest              ErrorCode = "bad_request"
	ErrorCodeInvalidTenant           ErrorCode = "invalid_tenant"
	ErrorCodeTenantRequired          ErrorCode = "tenant_required"
	ErrorCodeTenantDisabled          ErrorCode = "tenant_disabled"
	ErrorCodeTenantDeleted           ErrorCode = "tenant_deleted"
	ErrorCodeOperationDisabled       ErrorCode = "operation_disabled"
	ErrorCodeNotFound                ErrorCode = "not_found"
	ErrorCodeMethodNotAllowed        ErrorCode = "method_not_allowed"
	ErrorCodeRequestTooLarge         ErrorCode = "request_too_large"
	ErrorCodeTooManyRequests         ErrorCode = "too_many_requests"
	ErrorCodeInternal                ErrorCode = "internal_error"
	ErrorCodeInvalidJSON             ErrorCode = "invalid_json"
	ErrorCodeInvalidQuery            ErrorCode = "invalid_query"
	ErrorCodeQueryLimitExceeded      ErrorCode = "query_limit_exceeded"
	ErrorCodeMutationBudget          ErrorCode = storage.IngestErrorMutationBudget
	ErrorCodeIndexStale              ErrorCode = "index_stale"
	ErrorCodeReaderNotFresh          ErrorCode = "reader_not_fresh"
	ErrorCodeQuotaExceeded           ErrorCode = "quota_exceeded"
	ErrorCodeLeaseHeld               ErrorCode = "lease_held"
	ErrorCodeManifestCASConflict     ErrorCode = "manifest_cas_conflict"
	ErrorCodeObjectWriteConflict     ErrorCode = "object_write_conflict"
	ErrorCodeObjectStoreUnavailable  ErrorCode = "object_store_unavailable"
	ErrorCodeTaskConflict            ErrorCode = "task_conflict"
	ErrorCodeRepairRequired          ErrorCode = "repair_required"
	ErrorCodeVersionConflict         ErrorCode = "version_conflict"
	ErrorCodePreconditionFailed      ErrorCode = "precondition_failed"
	ErrorCodeAtomicValidationFailed  ErrorCode = "atomic_validation_failed"
	ErrorCodeAtomicSuppressed        ErrorCode = "atomic_suppressed"
	ErrorCodeIdempotencyConflict     ErrorCode = "idempotency_conflict"
	ErrorCodeIdempotencyInProgress   ErrorCode = "idempotency_in_progress"
	ErrorCodeWriteConflict           ErrorCode = "write_conflict"
	ErrorCodeCoordinatorUnavailable  ErrorCode = "coordinator_unavailable"
	ErrorCodeCommitTailTooLong       ErrorCode = "commit_tail_too_long"
	ErrorCodeIndexRebuildRunning     ErrorCode = "index_rebuild_running"
	ErrorCodeMaintenanceTaskRunning  ErrorCode = "maintenance_task_running"
	ErrorCodeWriteAdmissionTimeout   ErrorCode = "write_admission_queue_timeout"
	ErrorCodeWriteBackpressure       ErrorCode = "write_backpressure"
	ErrorCodeRequestTimeout          ErrorCode = "request_timeout"
	ErrorCodeRaftUnavailable         ErrorCode = "raft_unavailable"
	ErrorCodeWriteOutcomeUnknown     ErrorCode = "write_outcome_unknown"
	ErrorCodeTenantGenerationChanged ErrorCode = "tenant_generation_changed"
	ErrorCodeIngestQueueFull         ErrorCode = "ingest_queue_full"
	ErrorCodeRequestCanceled         ErrorCode = "request_canceled"
)

type ErrorCode string

var stableErrorCodes = []ErrorCode{
	ErrorCodeBadRequest,
	ErrorCodeInvalidTenant,
	ErrorCodeTenantRequired,
	ErrorCodeTenantDisabled,
	ErrorCodeTenantDeleted,
	ErrorCodeOperationDisabled,
	ErrorCodeNotFound,
	ErrorCodeMethodNotAllowed,
	ErrorCodeRequestTooLarge,
	ErrorCodeTooManyRequests,
	ErrorCodeInternal,
	ErrorCodeInvalidJSON,
	ErrorCodeInvalidQuery,
	ErrorCodeQueryLimitExceeded,
	ErrorCodeMutationBudget,
	ErrorCodeIndexStale,
	ErrorCodeReaderNotFresh,
	ErrorCodeQuotaExceeded,
	ErrorCodeLeaseHeld,
	ErrorCodeManifestCASConflict,
	ErrorCodeObjectWriteConflict,
	ErrorCodeObjectStoreUnavailable,
	ErrorCodeTaskConflict,
	ErrorCodeRepairRequired,
	ErrorCodeVersionConflict,
	ErrorCodePreconditionFailed,
	ErrorCodeAtomicValidationFailed,
	ErrorCodeAtomicSuppressed,
	ErrorCodeIdempotencyConflict,
	ErrorCodeIdempotencyInProgress,
	ErrorCodeWriteConflict,
	ErrorCodeCoordinatorUnavailable,
	ErrorCodeCommitTailTooLong,
	ErrorCodeIndexRebuildRunning,
	ErrorCodeMaintenanceTaskRunning,
	ErrorCodeWriteAdmissionTimeout,
	ErrorCodeWriteBackpressure,
	ErrorCodeRequestTimeout,
	ErrorCodeRaftUnavailable,
	ErrorCodeWriteOutcomeUnknown,
	ErrorCodeTenantGenerationChanged,
	ErrorCodeIngestQueueFull,
	ErrorCodeRequestCanceled,
}

var stableErrorCodeSet = func() map[ErrorCode]struct{} {
	out := make(map[ErrorCode]struct{}, len(stableErrorCodes))
	for _, code := range stableErrorCodes {
		out[code] = struct{}{}
	}
	return out
}()

type ErrorResponse struct {
	Error     string    `json:"error"`
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
	Detail    any       `json:"detail,omitempty"`
}

type StreamErrorResponse struct {
	ErrorResponse
	Done bool `json:"done"`
}

func tenantFromRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	_, span := startAPIPhase(r.Context(), "resolve_tenant")
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		if span != nil {
			span.SetAttributes(attribute.String("graphdb.tenant.resolve_result", "missing"))
		}
		endHTTPSpan(span, traceError("X-Tenant-ID header is required"))
		writeError(w, http.StatusBadRequest, "X-Tenant-ID header is required")
		return "", false
	}
	if err := storage.ValidateTenantID(tenantID); err != nil {
		if span != nil {
			span.SetAttributes(attribute.String("graphdb.tenant.resolve_result", "invalid"))
		}
		endHTTPSpan(span, err)
		writeError(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	setAPITraceTenant(r.Context(), tenantID)
	if span != nil {
		span.SetAttributes(
			attribute.String("graphdb.tenant", tenantID),
			attribute.String("graphdb.tenant.resolve_result", "ok"),
		)
	}
	endHTTPSpan(span, nil)
	return tenantID, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(responseJSONValue(value))
	if err != nil {
		status = http.StatusInternalServerError
		data, _ = json.Marshal(buildErrorResponse(ErrorCodeInternal, "response encoding failed", false, nil))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(data, '\n'))
}

func writeError(w http.ResponseWriter, status int, message string) {
	response := errorResponseFor(status, nil, message, nil)
	writeJSON(w, status, response)
}

func writeErrorErr(w http.ResponseWriter, status int, err error) {
	response := errorResponseFor(status, err, "", nil)
	writeJSON(w, status, response)
}

func writeStorageError(w http.ResponseWriter, err error) {
	writeStorageErrorDefault(w, http.StatusInternalServerError, err)
}

func writeStorageErrorDefault(w http.ResponseWriter, defaultStatus int, err error) {
	var pressure *storage.BackpressureError
	if errors.As(err, &pressure) && pressure.RetryAfter > 0 {
		seconds := int((pressure.RetryAfter + time.Second - 1) / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
	response := errorResponseFor(defaultStatus, err, "", nil)
	if response.Code == ErrorCodeInternal {
		response = buildErrorResponse(ErrorCodeInternal, "internal storage error", false, nil)
	}
	writeJSON(w, storageErrorStatus(response.Code), response)
}

type errorCodeContract struct {
	Status             int
	Retryable          bool
	BackpressureHeader bool
}

// errorCodeContracts is the single authoritative HTTP status and retryability
// table for every stable ErrorCode. Exhaustiveness is enforced by
// TestErrorCodeContractTableIsExhaustive.
var errorCodeContracts = map[ErrorCode]errorCodeContract{
	ErrorCodeBadRequest:              {Status: http.StatusBadRequest, Retryable: false},
	ErrorCodeInvalidTenant:           {Status: http.StatusBadRequest, Retryable: false},
	ErrorCodeTenantRequired:          {Status: http.StatusBadRequest, Retryable: false},
	ErrorCodeTenantDisabled:          {Status: http.StatusForbidden, Retryable: false},
	ErrorCodeTenantDeleted:           {Status: http.StatusGone, Retryable: false},
	ErrorCodeOperationDisabled:       {Status: http.StatusMethodNotAllowed, Retryable: false},
	ErrorCodeNotFound:                {Status: http.StatusNotFound, Retryable: false},
	ErrorCodeMethodNotAllowed:        {Status: http.StatusMethodNotAllowed, Retryable: false},
	ErrorCodeRequestTooLarge:         {Status: http.StatusRequestEntityTooLarge, Retryable: false},
	ErrorCodeTooManyRequests:         {Status: http.StatusTooManyRequests, Retryable: true, BackpressureHeader: true},
	ErrorCodeInternal:                {Status: http.StatusInternalServerError, Retryable: false},
	ErrorCodeInvalidJSON:             {Status: http.StatusBadRequest, Retryable: false},
	ErrorCodeInvalidQuery:            {Status: http.StatusBadRequest, Retryable: false},
	ErrorCodeQueryLimitExceeded:      {Status: http.StatusTooManyRequests, Retryable: true, BackpressureHeader: true},
	ErrorCodeMutationBudget:          {Status: http.StatusUnprocessableEntity, Retryable: false},
	ErrorCodeIndexStale:              {Status: http.StatusBadRequest, Retryable: false},
	ErrorCodeReaderNotFresh:          {Status: http.StatusServiceUnavailable, Retryable: true},
	ErrorCodeQuotaExceeded:           {Status: http.StatusTooManyRequests, Retryable: false, BackpressureHeader: true},
	ErrorCodeLeaseHeld:               {Status: http.StatusConflict, Retryable: true},
	ErrorCodeManifestCASConflict:     {Status: http.StatusConflict, Retryable: true},
	ErrorCodeObjectWriteConflict:     {Status: http.StatusConflict, Retryable: true},
	ErrorCodeObjectStoreUnavailable:  {Status: http.StatusServiceUnavailable, Retryable: true},
	ErrorCodeTaskConflict:            {Status: http.StatusConflict, Retryable: false},
	ErrorCodeRepairRequired:          {Status: http.StatusConflict, Retryable: false},
	ErrorCodeVersionConflict:         {Status: http.StatusConflict, Retryable: false},
	ErrorCodePreconditionFailed:      {Status: http.StatusPreconditionFailed, Retryable: false},
	ErrorCodeAtomicValidationFailed:  {Status: http.StatusUnprocessableEntity, Retryable: false},
	ErrorCodeAtomicSuppressed:        {Status: http.StatusConflict, Retryable: false},
	ErrorCodeIdempotencyConflict:     {Status: http.StatusConflict, Retryable: false},
	ErrorCodeIdempotencyInProgress:   {Status: http.StatusConflict, Retryable: true},
	ErrorCodeWriteConflict:           {Status: http.StatusConflict, Retryable: true},
	ErrorCodeCoordinatorUnavailable:  {Status: http.StatusServiceUnavailable, Retryable: true},
	ErrorCodeCommitTailTooLong:       {Status: http.StatusTooManyRequests, Retryable: true, BackpressureHeader: true},
	ErrorCodeIndexRebuildRunning:     {Status: http.StatusTooManyRequests, Retryable: true, BackpressureHeader: true},
	ErrorCodeMaintenanceTaskRunning:  {Status: http.StatusTooManyRequests, Retryable: true, BackpressureHeader: true},
	ErrorCodeWriteAdmissionTimeout:   {Status: http.StatusTooManyRequests, Retryable: true, BackpressureHeader: true},
	ErrorCodeWriteBackpressure:       {Status: http.StatusTooManyRequests, Retryable: true, BackpressureHeader: true},
	ErrorCodeRequestTimeout:          {Status: http.StatusGatewayTimeout, Retryable: true},
	ErrorCodeRaftUnavailable:         {Status: http.StatusServiceUnavailable, Retryable: true},
	ErrorCodeWriteOutcomeUnknown:     {Status: http.StatusServiceUnavailable, Retryable: false},
	ErrorCodeTenantGenerationChanged: {Status: http.StatusConflict, Retryable: false},
	ErrorCodeIngestQueueFull:         {Status: http.StatusServiceUnavailable, Retryable: true, BackpressureHeader: true},
	ErrorCodeRequestCanceled:         {Status: 499, Retryable: false},
}

func storageErrorStatus(code ErrorCode) int {
	if contract, ok := errorCodeContracts[code]; ok {
		return contract.Status
	}
	return http.StatusInternalServerError
}

func writeErrorDetail(w http.ResponseWriter, status int, code ErrorCode, message string, retryable bool, detail any) {
	if isStableErrorCode(code) {
		w.Header().Set("X-GraphDB-Error-Code", string(code))
	}
	writeJSON(w, status, buildErrorResponse(code, message, retryable, detail))
}

func buildErrorResponse(code ErrorCode, message string, retryable bool, detail any) ErrorResponse {
	if !isStableErrorCode(code) {
		code = ErrorCodeInternal
		retryable = false
	}
	return ErrorResponse{
		Error:     message,
		Code:      code,
		Message:   message,
		Retryable: retryable,
		Detail:    detail,
	}
}

func isStableErrorCode(code ErrorCode) bool {
	_, ok := stableErrorCodeSet[code]
	return ok
}

func streamErrorResponse(response ErrorResponse) StreamErrorResponse {
	return StreamErrorResponse{ErrorResponse: response, Done: true}
}

func defaultErrorCode(status int) ErrorCode {
	switch status {
	case http.StatusBadRequest:
		return ErrorCodeBadRequest
	case http.StatusNotFound:
		return ErrorCodeNotFound
	case http.StatusMethodNotAllowed:
		return ErrorCodeMethodNotAllowed
	case http.StatusForbidden:
		return ErrorCodeTenantDisabled
	case http.StatusGone:
		return ErrorCodeTenantDeleted
	case http.StatusRequestEntityTooLarge:
		return ErrorCodeRequestTooLarge
	case http.StatusTooManyRequests:
		return ErrorCodeTooManyRequests
	case http.StatusInternalServerError:
		return ErrorCodeInternal
	default:
		return ErrorCodeInternal
	}
}

func defaultRetryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

func errorResponseFor(status int, err error, message string, detail any) ErrorResponse {
	if err != nil && message == "" {
		if errors.Is(err, storage.ErrReceiptStoreUnavailable) {
			message = storage.ErrReceiptStoreUnavailable.Error()
		} else {
			message = err.Error()
		}
	}
	code := defaultErrorCode(status)
	retryable := defaultRetryable(status)
	if err != nil {
		code, retryable = classifyError(err, code, retryable)
	} else {
		code, retryable = classifyMessage(message, code, retryable)
	}
	return buildErrorResponse(code, message, retryable, detail)
}

func classifyError(err error, fallback ErrorCode, retryable bool) (ErrorCode, bool) {
	var notFresh *readerNotFreshError
	if errors.As(err, &notFresh) {
		return ErrorCodeReaderNotFresh, true
	}
	var pressure *storage.BackpressureError
	if errors.As(err, &pressure) {
		return backpressureErrorContract(pressure.Reasons)
	}
	switch {
	case errors.Is(err, storage.ErrTaskConflict):
		return ErrorCodeTaskConflict, false
	case errors.Is(err, storage.ErrLeaseHeld):
		return ErrorCodeLeaseHeld, true
	case errors.Is(err, storage.ErrObjectStoreUnavailable):
		return ErrorCodeObjectStoreUnavailable, true
	case errors.Is(err, storage.ErrWriteConflict):
		return ErrorCodeWriteConflict, true
	case errors.Is(err, storage.ErrVersionConflict):
		return ErrorCodeVersionConflict, false
	case errors.Is(err, storage.ErrIdempotencyInProgress):
		return ErrorCodeIdempotencyInProgress, true
	case errors.Is(err, storage.ErrIdempotencyConflict), errors.Is(err, storage.ErrIngestIdentityConflict):
		return ErrorCodeIdempotencyConflict, false
	case errors.Is(err, storage.ErrMaintenanceBusy):
		return ErrorCodeMaintenanceTaskRunning, true
	case errors.Is(err, storage.ErrIngestRepairRequired), errors.Is(err, storage.ErrTenantMetadataMissing), errors.Is(err, storage.ErrReceiptStoreUnavailable):
		return ErrorCodeRepairRequired, false
	case errors.Is(err, storage.ErrTenantDisabled):
		return ErrorCodeTenantDisabled, false
	case errors.Is(err, storage.ErrTenantDeleted):
		return ErrorCodeTenantDeleted, false
	case errors.Is(err, storage.ErrConflict):
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "manifest") {
			return ErrorCodeManifestCASConflict, true
		}
		return ErrorCodeObjectWriteConflict, true
	case errors.Is(err, storage.ErrNotFound):
		return ErrorCodeNotFound, false
	case errors.Is(err, storage.ErrInvalidArgument):
		return ErrorCodeBadRequest, false
	case errors.Is(err, query.ErrInvalid):
		return ErrorCodeInvalidQuery, false
	case errors.Is(err, query.ErrLimitExceeded):
		return ErrorCodeQueryLimitExceeded, true
	case errors.Is(err, query.ErrIndexUnavailable):
		return ErrorCodeIndexStale, false
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorCodeRequestTimeout, true
	case errors.Is(err, context.Canceled):
		return ErrorCodeRequestCanceled, false
	default:
		return fallback, retryable
	}
}

func classifyMessage(message string, fallback ErrorCode, retryable bool) (ErrorCode, bool) {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "x-tenant-id header is required"):
		return ErrorCodeTenantRequired, false
	case strings.Contains(lower, "invalid tenant id"):
		return ErrorCodeInvalidTenant, false
	case strings.Contains(lower, "tenant disabled"):
		return ErrorCodeTenantDisabled, false
	case strings.Contains(lower, "tenant deleted"):
		return ErrorCodeTenantDeleted, false
	case strings.Contains(lower, "disabled in reader mode"):
		return ErrorCodeOperationDisabled, false
	case strings.Contains(lower, "writer lease is held"):
		return ErrorCodeLeaseHeld, true
	case strings.Contains(lower, "object store unavailable"):
		return ErrorCodeObjectStoreUnavailable, true
	case strings.Contains(lower, "persisted index unavailable"):
		return ErrorCodeIndexStale, false
	case strings.Contains(lower, "reader not fresh") || strings.Contains(lower, "reader is not fresh") || strings.Contains(lower, "readers are not fresh"):
		return ErrorCodeReaderNotFresh, true
	case strings.Contains(lower, "task") && (strings.Contains(lower, "still running") || strings.Contains(lower, "still queued") || strings.Contains(lower, "can be retried") || strings.Contains(lower, "cancellation is not supported")):
		return ErrorCodeTaskConflict, false
	case strings.Contains(lower, "repair required") || strings.Contains(lower, "requires repair"):
		return ErrorCodeRepairRequired, false
	case strings.Contains(lower, "quota") && strings.Contains(lower, "exceeded"):
		return ErrorCodeQuotaExceeded, false
	case strings.Contains(lower, "expected version"):
		return ErrorCodeVersionConflict, false
	case strings.Contains(lower, "idempotency conflict"):
		return ErrorCodeIdempotencyConflict, false
	case strings.Contains(lower, "manifest") && strings.Contains(lower, "changed"):
		return ErrorCodeManifestCASConflict, true
	case strings.Contains(lower, "object write conflict"):
		return ErrorCodeObjectWriteConflict, true
	default:
		return fallback, retryable
	}
}
