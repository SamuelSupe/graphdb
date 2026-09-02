package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const (
	IngestFailureModeBestEffort = "best_effort"
	IngestFailureModeAtomic     = "atomic"

	IngestErrorVersionConflict     = "version_conflict"
	IngestErrorPreconditionFailed  = "precondition_failed"
	IngestErrorAtomicValidation    = "atomic_validation_failed"
	IngestErrorAtomicSuppressed    = "atomic_suppressed"
	IngestErrorIdempotencyConflict = "idempotency_conflict"

	maxIngestPreconditions = 256
)

var (
	ErrIngestPreconditionFailed = errors.New("ingest precondition failed")
	ErrIngestAtomicValidation   = errors.New("atomic ingest validation failed")
	ErrIngestAtomicSuppressed   = errors.New("atomic ingest mutation was suppressed")
)

type IngestPrecondition struct {
	ResourceType string `json:"resource_type"`
	ID           string `json:"id"`
	Field        string `json:"field,omitempty"`
	Op           string `json:"op"`
	Value        any    `json:"value,omitempty"`
	ValueFrom    string `json:"value_from,omitempty"`
}

type ingestPreconditionError struct {
	Index        int
	Precondition IngestPrecondition
	Actual       any
	Expected     any
	Reason       string
}

func (e *ingestPreconditionError) Error() string {
	target := e.Precondition.ResourceType + " " + e.Precondition.ID
	if e.Precondition.Field != "" {
		target += " field " + e.Precondition.Field
	}
	return fmt.Sprintf("%s: condition %d on %s with op %s: %s", ErrIngestPreconditionFailed, e.Index, target, e.Precondition.Op, e.Reason)
}

func (e *ingestPreconditionError) Unwrap() error {
	return ErrIngestPreconditionFailed
}

func normalizeIngestTransactionalOptions(request *IngestRequest) error {
	request.FailureMode = strings.ToLower(strings.TrimSpace(request.FailureMode))
	if request.FailureMode == "" {
		request.FailureMode = IngestFailureModeBestEffort
	}
	if request.FailureMode != IngestFailureModeBestEffort && request.FailureMode != IngestFailureModeAtomic {
		return fmt.Errorf("failure_mode must be %q or %q", IngestFailureModeBestEffort, IngestFailureModeAtomic)
	}
	if request.ExpectedVersion != nil && *request.ExpectedVersion < 0 {
		return fmt.Errorf("expected_version must be >= 0")
	}
	if len(request.Preconditions) > maxIngestPreconditions {
		return fmt.Errorf("preconditions must contain at most %d entries", maxIngestPreconditions)
	}
	for index := range request.Preconditions {
		condition := &request.Preconditions[index]
		condition.ResourceType = strings.ToLower(strings.TrimSpace(condition.ResourceType))
		condition.ID = strings.TrimSpace(condition.ID)
		condition.Field = strings.TrimSpace(condition.Field)
		condition.Op = strings.ToLower(strings.TrimSpace(condition.Op))
		condition.ValueFrom = strings.ToLower(strings.TrimSpace(condition.ValueFrom))
		if err := validateIngestPrecondition(*condition); err != nil {
			return fmt.Errorf("preconditions[%d]: %w", index, err)
		}
	}
	return nil
}

func validateIngestPrecondition(condition IngestPrecondition) error {
	if condition.ResourceType != "entity" && condition.ResourceType != "edge" {
		return fmt.Errorf("resource_type must be %q or %q", "entity", "edge")
	}
	if condition.ID == "" {
		return fmt.Errorf("id is required")
	}
	switch condition.Op {
	case "exists", "not_exists":
		if condition.Value != nil || condition.ValueFrom != "" {
			return fmt.Errorf("op %q does not accept value or value_from", condition.Op)
		}
		return nil
	case "eq", "ne", "lt", "lte", "gt", "gte":
		if condition.Field == "" {
			return fmt.Errorf("field is required for op %q", condition.Op)
		}
	default:
		return fmt.Errorf("unsupported op %q", condition.Op)
	}
	if condition.ValueFrom != "" && condition.ValueFrom != "accepted_at" {
		return fmt.Errorf("value_from must be %q", "accepted_at")
	}
	if condition.ValueFrom != "" && condition.Value != nil {
		return fmt.Errorf("value and value_from are mutually exclusive")
	}
	if condition.ValueFrom == "" && condition.Value == nil {
		return fmt.Errorf("value or value_from is required for op %q", condition.Op)
	}
	if _, err := json.Marshal(condition.Value); err != nil {
		return fmt.Errorf("value is not JSON encodable: %w", err)
	}
	return nil
}

func ingestRequestAtomic(request IngestRequest) bool {
	return request.FailureMode == IngestFailureModeAtomic
}

func ingestRequestNeedsIsolatedApply(request IngestRequest) bool {
	return request.ExpectedVersion != nil || len(request.Preconditions) > 0 || ingestRequestAtomic(request)
}

func evaluateIngestPreconditions(g *graph.Graph, conditions []IngestPrecondition, acceptedAt time.Time) error {
	for index, condition := range conditions {
		actual, exists := ingestPreconditionValue(g, condition)
		expected := condition.Value
		if condition.ValueFrom == "accepted_at" {
			expected = acceptedAt
		}
		matched, reason := ingestPreconditionMatches(condition.Op, actual, expected, exists)
		if !matched {
			return &ingestPreconditionError{
				Index:        index,
				Precondition: condition,
				Actual:       actual,
				Expected:     expected,
				Reason:       reason,
			}
		}
	}
	return nil
}

func ingestPreconditionValue(g *graph.Graph, condition IngestPrecondition) (any, bool) {
	if condition.ResourceType == "entity" {
		entity, ok := g.Entities[condition.ID]
		if condition.Field == "" {
			return nil, ok
		}
		if !ok {
			return nil, false
		}
		value, exists := entity.Fields[condition.Field]
		return value, exists
	}
	edge, ok := g.Edges[condition.ID]
	if condition.Field == "" {
		return nil, ok
	}
	if !ok {
		return nil, false
	}
	value, exists := edge.Fields[condition.Field]
	return value, exists
}

func ingestPreconditionMatches(op string, actual any, expected any, exists bool) (bool, string) {
	switch op {
	case "exists":
		if exists {
			return true, ""
		}
		return false, "value does not exist"
	case "not_exists":
		if !exists {
			return true, ""
		}
		return false, "value exists"
	}
	if !exists {
		return false, "value does not exist"
	}
	if op == "eq" {
		if ingestValuesEqual(actual, expected) {
			return true, ""
		}
		return false, "value is not equal"
	}
	if op == "ne" {
		if !ingestValuesEqual(actual, expected) {
			return true, ""
		}
		return false, "value is equal"
	}
	comparison, ok := compareIngestValues(actual, expected)
	if !ok {
		return false, "values are not comparable numbers or timestamps"
	}
	switch op {
	case "lt":
		return comparison < 0, "value is not less than expected"
	case "lte":
		return comparison <= 0, "value is greater than expected"
	case "gt":
		return comparison > 0, "value is not greater than expected"
	case "gte":
		return comparison >= 0, "value is less than expected"
	default:
		return false, "unsupported comparison"
	}
}

func ingestValuesEqual(left any, right any) bool {
	if leftNumber, ok := ingestNumber(left); ok {
		if rightNumber, rightOK := ingestNumber(right); rightOK {
			return leftNumber == rightNumber
		}
	}
	if leftTime, ok := ingestTime(left); ok {
		if rightTime, rightOK := ingestTime(right); rightOK {
			return leftTime.Equal(rightTime)
		}
	}
	return reflect.DeepEqual(left, right)
}

func compareIngestValues(left any, right any) (int, bool) {
	if leftNumber, ok := ingestNumber(left); ok {
		if rightNumber, rightOK := ingestNumber(right); rightOK {
			switch {
			case leftNumber < rightNumber:
				return -1, true
			case leftNumber > rightNumber:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	if leftTime, ok := ingestTime(left); ok {
		if rightTime, rightOK := ingestTime(right); rightOK {
			switch {
			case leftTime.Before(rightTime):
				return -1, true
			case leftTime.After(rightTime):
				return 1, true
			default:
				return 0, true
			}
		}
	}
	return 0, false
}

func ingestNumber(value any) (float64, bool) {
	var number float64
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		number = parsed
	case float64:
		number = typed
	case float32:
		number = float64(typed)
	case int:
		number = float64(typed)
	case int8:
		number = float64(typed)
	case int16:
		number = float64(typed)
	case int32:
		number = float64(typed)
	case int64:
		number = float64(typed)
	case uint:
		number = float64(typed)
	case uint8:
		number = float64(typed)
	case uint16:
		number = float64(typed)
	case uint32:
		number = float64(typed)
	case uint64:
		number = float64(typed)
	default:
		return 0, false
	}
	return number, !math.IsNaN(number)
}

func ingestTime(value any) (time.Time, bool) {
	switch typed := value.(type) {
	case time.Time:
		return typed, !typed.IsZero()
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		return parsed, err == nil
	default:
		return time.Time{}, false
	}
}

func ingestErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrVersionConflict):
		return IngestErrorVersionConflict
	case errors.Is(err, ErrIngestPreconditionFailed):
		return IngestErrorPreconditionFailed
	case errors.Is(err, ErrIngestAtomicValidation):
		return IngestErrorAtomicValidation
	case errors.Is(err, ErrIngestAtomicSuppressed):
		return IngestErrorAtomicSuppressed
	case errors.Is(err, ErrIngestIdentityConflict), errors.Is(err, ErrIdempotencyConflict):
		return IngestErrorIdempotencyConflict
	default:
		return ""
	}
}

func ingestConflictFromError(err error) IngestConflict {
	conflict := IngestConflict{Message: err.Error()}
	var conditionErr *ingestPreconditionError
	if !errors.As(err, &conditionErr) {
		return conflict
	}
	conflict.ResourceType = conditionErr.Precondition.ResourceType
	if conflict.ResourceType == "entity" {
		conflict.EntityID = conditionErr.Precondition.ID
	} else {
		conflict.EdgeID = conditionErr.Precondition.ID
	}
	conflict.Field = conditionErr.Precondition.Field
	conflict.ExistingValue = conditionErr.Actual
	conflict.IncomingValue = conditionErr.Expected
	return conflict
}

func markIngestResultFailure(result *IngestResult, request IngestRequest, appliedIndices []int, failure error) {
	pendingApplied := result.Applied
	result.Failed += pendingApplied
	result.Applied = 0
	if result.Failed == 0 {
		result.Failed = 1
	}
	if code := ingestErrorCode(failure); code != "" {
		result.ErrorCode = code
	}
	for _, index := range appliedIndices {
		result.Failures = append(result.Failures, IngestFailure{
			Index:      index,
			ExternalID: request.Items[index].ExternalID,
			Error:      failure.Error(),
		})
	}
	result.Conflicts = append(result.Conflicts, ingestConflictFromError(failure))
}
