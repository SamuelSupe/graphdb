package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
)

// IngestInput binds execution to the original durable acceptance time, including
// after leadership changes and retries of accepted_at preconditions.
type IngestInput struct {
	Request    storage.IngestRequest `json:"request"`
	AcceptedAt time.Time             `json:"accepted_at"`
}

func (input IngestInput) Encode() ([]byte, error) {
	if len(input.Request.Items) > MaxChanges {
		return nil, invalid("ingest item count exceeds the graph transaction budget")
	}
	cursor, err := json.Marshal(input.Request.Cursor)
	if err != nil {
		return nil, err
	}
	if len(cursor) > MaxValueBytes {
		return nil, invalid("ingest cursor exceeds the receipt byte budget")
	}
	data, err := json.Marshal(transactionInput{Ingest: &input})
	if err == nil && len(data) > MaxChangeBytes {
		err = invalid("ingest batch exceeds the graph transaction input budget")
	}
	return data, err
}

// SubmitIngest uses the acceptance identity as the transaction fence. The
// ingest result is published with the graph decision, before queue completion.
func (e Engine) SubmitIngest(ctx context.Context, identity string, input IngestInput) (storage.IngestResult, error) {
	if !strings.HasPrefix(identity, "ingest:") || input.AcceptedAt.IsZero() || e.Generation <= 0 {
		return storage.IngestResult{}, invalid("ingest requires a durable identity, generation and acceptance time")
	}
	data, err := input.Encode()
	if err != nil {
		return storage.IngestResult{}, err
	}
	_, submitErr := e.submitInput(ctx, data, identity, nil, "ingest")
	txn, err := e.Remote.Transaction(ctx, e.Home, e.Tenant, e.Generation, identity)
	if err != nil {
		return storage.IngestResult{}, errors.Join(submitErr, err)
	}
	if txn.Digest != fmt.Sprintf("%x", sha256.Sum256(data)) {
		return storage.IngestResult{}, conflict("idempotency key was used with different content")
	}
	if (txn.State == "committed" || txn.State == "aborted") && txn.IngestResult != nil && len(txn.Inputs) == 0 && len(txn.Participants) == 0 {
		return *txn.IngestResult, nil
	}
	if submitErr != nil {
		return storage.IngestResult{}, submitErr
	}
	return storage.IngestResult{}, fmt.Errorf("ingest transaction has no durable result")
}

func buildIngestPlan(ctx context.Context, view View, input IngestInput) (Plan, error) {
	request := input.Request
	result, mutations, indices := ingest.BuildIngestMutations(request)
	result.BatchID, result.Cursor = request.BatchID, request.Cursor
	var failure error
	switch {
	case request.FullSync:
		failure = invalid("full_sync requires a partitioned reconciliation task")
	case ingest.IngestRequestAtomic(request) && result.Failed > 0:
		failure = ingest.ErrIngestAtomicValidation
	case result.Applied > 0 && request.ExpectedVersion != nil && *request.ExpectedVersion != view.Graph.Version:
		failure = ingest.ErrVersionConflict
	}
	var plan Plan
	if failure == nil && result.Applied > 0 {
		var err error
		plan, err = buildPlan(ctx, view, CommitRequest{Mutations: mutations}, input.AcceptedAt, request.Preconditions, ingest.IngestRequestAtomic(request))
		if err != nil {
			if !errors.Is(err, storage.ErrInvalidArgument) && !errors.Is(err, ErrReadBudget) && !errors.Is(err, ErrQuotaExceeded) &&
				!errors.Is(err, ingest.ErrIngestPreconditionFailed) && !errors.Is(err, ingest.ErrIngestAtomicSuppressed) {
				return Plan{}, err
			}
			failure = err
		} else {
			result.Suppressed = len(plan.Report.Suppressed)
			result.Conflicts = ingest.IngestConflicts(request, plan.Report.Suppressed)
			result.Skipped = !plan.Report.Changed
			if result.Skipped {
				result.SkipReason = storage.IngestSkipReasonLogicalNoop
			}
		}
	}
	if failure != nil {
		ingest.MarkIngestResultFailure(&result, request, indices, failure)
	}
	if failure != nil || result.Applied == 0 {
		plan = Plan{CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes, EntityCount: view.Graph.EntityCount, EdgeCount: view.Graph.EdgeCount}
	}
	plan.IngestResult = boundIngestResult(result)
	return plan, nil
}

func failedIngestResult(input *IngestInput, failure error) *storage.IngestResult {
	if input == nil {
		return nil
	}
	result, _, indices := ingest.BuildIngestMutations(input.Request)
	result.BatchID, result.Cursor = input.Request.BatchID, input.Request.Cursor
	ingest.MarkIngestResultFailure(&result, input.Request, indices, failure)
	return boundIngestResult(result)
}

// An accepted request must always fit its terminal decision into a Raft
// action. Bound diagnostic details separately from the counts and cursor.
func boundIngestResult(result storage.IngestResult) *storage.IngestResult {
	budget := MaxValueBytes
	failures := boundedIngestDetails(result.Failures, &budget)
	conflicts := boundedIngestDetails(result.Conflicts, &budget)
	result.DetailsTruncated = len(failures) != len(result.Failures) || len(conflicts) != len(result.Conflicts)
	result.Failures, result.Conflicts = failures, conflicts
	return &result
}

func boundedIngestDetails[T any](details []T, budget *int) []T {
	for i, detail := range details {
		data, err := json.Marshal(detail)
		if err != nil || len(data)+1 > *budget {
			return details[:i]
		}
		*budget -= len(data) + 1
	}
	return details
}
