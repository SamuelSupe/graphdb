package partition

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"golang.org/x/sync/errgroup"
)

type Engine struct {
	Remote         Remote
	Home           Group
	Tenant         string
	Generation     int64
	BatchFragments bool
	Metrics        *observability.OperationMetrics
	// PublishAfter permits immutable planning while a maintenance step runs.
	PublishAfter <-chan struct{}
}

func (e Engine) waitToPublish(ctx context.Context) error {
	if e.PublishAfter == nil {
		return nil
	}
	finish := e.Metrics.Start("partition_txn_wait_gc")
	select {
	case <-e.PublishAfter:
	case <-ctx.Done():
	}
	err := ctx.Err()
	finish(err)
	return err
}

type OutcomeUnknownError struct {
	Txn   string
	Cause error
}

func (e *OutcomeUnknownError) Error() string {
	return fmt.Sprintf("graph transaction %s outcome is unknown: %v", e.Txn, e.Cause)
}
func (e *OutcomeUnknownError) Unwrap() error { return e.Cause }

func waitWriter(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func transactionID() (string, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return hex.EncodeToString(id[:]), err
}

func (e Engine) control(ctx context.Context, g Graph, a Action, result any) error {
	a.Tenant = e.Tenant
	a.Generation = g.Generation
	finish := e.Metrics.Start("partition_txn_" + a.Operation)
	err := e.Remote.Action(ctx, e.Home, a, result)
	finish(err)
	return err
}

func (e Engine) updateTransaction(ctx context.Context, g Graph, a Action, txn *Transaction) error {
	// Describe results may be shared by concurrent callers. Decode into a new
	// value so JSON decoding cannot reuse a cached transaction's slices or maps.
	var next Transaction
	if err := e.control(ctx, g, a, &next); err != nil {
		return err
	}
	*txn = next
	return nil
}

func (e Engine) Submit(ctx context.Context, request CommitRequest) (storage.CommitResult, error) {
	canonical := storage.NormalizeDirectCommitRequest(storage.DirectCommitRequest{Mutations: request.Mutations, ExpectedVersion: request.ExpectedVersion, IdempotencyKey: request.IdempotencyKey})
	request.Mutations = canonical.Mutations
	request.IdempotencyKey = canonical.IdempotencyKey
	data, err := json.Marshal(transactionInput{Commit: &request})
	if err != nil {
		return storage.CommitResult{}, invalid(err.Error())
	}
	identity := ""
	if request.IdempotencyKey != "" {
		identity = "direct:" + request.IdempotencyKey
	}
	return e.submitInput(ctx, data, identity, request.ExpectedVersion, "commit", "", nil)
}

func (e Engine) submitInput(ctx context.Context, data []byte, identity string, expected *int64, purpose, acceptance string, finished *Transaction) (storage.CommitResult, error) {
	if len(data) > MaxChangeBytes {
		return storage.CommitResult{}, invalid("graph transaction exceeds its input budget")
	}
	id, err := transactionID()
	if err != nil {
		return storage.CommitResult{}, err
	}
	if identity == "" {
		identity = "request:" + id
	}
	if len(identity) > 512 {
		return storage.CommitResult{}, invalid("idempotency key exceeds its byte budget")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	var inputs []Input
	var g Graph
	var txn Transaction
	var planned *Plan
	var stagedGeneration int64
	for {
		g, err = e.Remote.Describe(ctx, e.Home, e.Tenant)
		if err != nil {
			return storage.CommitResult{}, err
		}
		if e.Generation > 0 && e.Generation != g.Generation || stagedGeneration > 0 && stagedGeneration != g.Generation {
			return storage.CommitResult{}, conflict("tenant generation changed")
		}
		previous, lookupErr := e.Remote.Transaction(ctx, e.Home, e.Tenant, g.Generation, identity)
		if lookupErr == nil {
			if previous.Digest != digest {
				return storage.CommitResult{}, conflict("idempotency key was used with different content")
			}
			if previous.State != "retryable" {
				if err := e.waitToPublish(ctx); err != nil {
					return storage.CommitResult{}, err
				}
				txn = previous
				break
			}
		}
		if lookupErr != nil && !errors.Is(lookupErr, storage.ErrNotFound) {
			return storage.CommitResult{}, lookupErr
		}
		if g.GC.BlocksWrites() {
			return storage.CommitResult{}, fmt.Errorf("%w: garbage collection is installing its retention boundary", ErrGraphBusy)
		}
		if g.Move.BlocksWrites() {
			return storage.CommitResult{}, fmt.Errorf("%w: partition move is completing its cutover", ErrGraphBusy)
		}
		if expected != nil && *expected != g.Version {
			return storage.CommitResult{}, conflict("expected graph version does not match")
		}
		if g.Active != nil {
			if (purpose == "commit" || purpose == "ingest") && len(inputs) == 0 {
				return storage.CommitResult{}, fmt.Errorf("%w: another graph transaction is active", ErrGraphBusy)
			}
			if err := waitWriter(ctx); err != nil {
				return storage.CommitResult{}, err
			}
			continue
		}
		if len(inputs) == 0 {
			if acceptance != "" {
				inputs = []Input{{Stage: id, Digest: digest, Bytes: len(data), Acceptance: acceptance}}
			} else {
				for offset := 0; offset < len(data); offset += MaxValueBytes {
					chunk := data[offset:min(offset+MaxValueBytes, len(data))]
					var input Input
					if err := e.control(ctx, g, Action{Operation: "stage_input", Txn: id, Data: chunk, Digest: fmt.Sprintf("%x", sha256.Sum256(chunk))}, &input); err != nil {
						return storage.CommitResult{}, err
					}
					inputs = append(inputs, input)
				}
			}
			stagedGeneration = g.Generation
		}
		begin := Action{Operation: "begin", Purpose: purpose, Txn: id, Identity: identity, Digest: digest, Inputs: inputs, At: time.Now().UTC(), BaseVersion: g.Version, ExpectedVersion: expected}
		if e.BatchFragments && acceptance != "" {
			var input transactionInput
			if json.Unmarshal(data, &input) == nil && input.Ingest != nil {
				// Leave time to establish the durable active transaction even
				// when a data group cannot answer the speculative reads.
				planning, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
				finish := e.Metrics.Start("partition_txn_plan")
				plan, planErr := buildIngestPlan(planning, View{Graph: g, Remote: e.Remote}, *input.Ingest)
				finish(planErr)
				cancel()
				if planErr == nil {
					begin.Participants = planParticipants(plan)
					begin.LayoutRevision = g.LayoutRevision
					planned = &plan
				}
			}
		}
		if err := e.waitToPublish(ctx); err != nil {
			return storage.CommitResult{}, err
		}
		err = e.control(ctx, g, begin, &txn)
		if err == nil {
			break
		}
		planned = nil
		if errors.Is(err, ErrInputExpired) {
			inputs = nil
			continue
		}
		if acceptance != "" && errors.Is(err, storage.ErrConflict) {
			// The WAL driver retries from the current queue head, which may have
			// changed while this coordinator was waiting on Raft.
			return storage.CommitResult{}, err
		}
		if !errors.Is(err, storage.ErrConflict) {
			return storage.CommitResult{}, &OutcomeUnknownError{Txn: id, Cause: err}
		}
		if err := waitWriter(ctx); err != nil {
			return storage.CommitResult{}, err
		}
	}
	replay := txn.ID != id
	for txn.State != "committed" && txn.State != "aborted" {
		var resolved Transaction
		var resumeErr error
		if planned != nil && !replay {
			resolved, resumeErr = e.resume(ctx, g, txn, data, planned)
			planned = nil
		} else {
			resolved, resumeErr = e.Resume(ctx)
		}
		if resumeErr == nil && resolved.ID == txn.ID {
			txn = resolved
			break
		}
		latest, lookupErr := e.Remote.Transaction(ctx, e.Home, e.Tenant, g.Generation, identity)
		if lookupErr == nil {
			txn = latest
			if txn.Digest != digest {
				return storage.CommitResult{}, fmt.Errorf("durable transaction digest changed")
			}
			if txn.State == "committed" || txn.State == "aborted" {
				break
			}
		}
		if resumeErr != nil && !errors.Is(resumeErr, storage.ErrConflict) {
			return storage.CommitResult{}, &OutcomeUnknownError{Txn: txn.ID, Cause: resumeErr}
		}
		if lookupErr != nil {
			return storage.CommitResult{}, &OutcomeUnknownError{Txn: txn.ID, Cause: lookupErr}
		}
		if err := waitWriter(ctx); err != nil {
			return storage.CommitResult{}, &OutcomeUnknownError{Txn: txn.ID, Cause: err}
		}
	}
	if len(txn.Inputs) > 0 || len(txn.Participants) > 0 {
		completed, finishErr := e.finish(ctx, g, txn)
		if finishErr != nil {
			latest, lookupErr := e.Remote.Transaction(ctx, e.Home, e.Tenant, g.Generation, identity)
			if lookupErr != nil || latest.ID != txn.ID || len(latest.Inputs) > 0 || len(latest.Participants) > 0 {
				return storage.CommitResult{}, &OutcomeUnknownError{Txn: txn.ID, Cause: finishErr}
			}
			completed = latest
		}
		txn = completed
	}
	if finished != nil {
		*finished = txn
	}
	if txn.State == "aborted" {
		if txn.ErrorCode == "quota_exceeded" {
			return storage.CommitResult{}, fmt.Errorf("%w: %s", ErrQuotaExceeded, txn.Error)
		}
		return storage.CommitResult{}, invalid(txn.Error)
	}
	result := txn.Result
	result.IdempotentReplay = replay
	result.Skipped = result.Skipped || replay
	return result, nil
}

// Resume reconstructs all work from replicated input references and prepare
// receipts. It is safe for a newly elected leader and repeated RPC outcomes.
func (e Engine) Resume(ctx context.Context) (Transaction, error) {
	g, err := e.Remote.Describe(ctx, e.Home, e.Tenant)
	if err != nil {
		return Transaction{}, err
	}
	if e.Generation > 0 && e.Generation != g.Generation {
		return Transaction{}, conflict("tenant generation changed")
	}
	if g.Active == nil {
		return Transaction{}, conflict("graph has no active transaction")
	}
	return e.resume(ctx, g, *g.Active, nil, nil)
}

func (e Engine) resume(ctx context.Context, g Graph, txn Transaction, encoded []byte, planned *Plan) (Transaction, error) {
	if txn.State == "preparing" {
		if encoded == nil {
			for _, input := range txn.Inputs {
				chunk, err := e.Remote.Input(ctx, e.Home, e.Tenant, g.Generation, input)
				if err != nil {
					return txn, err
				}
				if len(chunk) != input.Bytes || fmt.Sprintf("%x", sha256.Sum256(chunk)) != input.Digest {
					return txn, fmt.Errorf("graph transaction input checksum mismatch")
				}
				encoded = append(encoded, chunk...)
				if len(encoded) > MaxChangeBytes {
					return txn, fmt.Errorf("durable graph input exceeds its budget")
				}
			}
		}
		if fmt.Sprintf("%x", sha256.Sum256(encoded)) != txn.Digest {
			return txn, fmt.Errorf("graph transaction input digest mismatch")
		}
		var input transactionInput
		if err := json.Unmarshal(encoded, &input); err != nil {
			return txn, err
		}
		var plan Plan
		var err error
		if planned != nil {
			plan = *planned
		} else if input.Ingest != nil && input.Commit == nil && input.Import == nil && txn.Purpose == "ingest" {
			plan, err = buildIngestPlan(ctx, View{Graph: g, Remote: e.Remote}, *input.Ingest)
		} else if input.Commit != nil && input.Import == nil && input.Ingest == nil {
			plan, err = BuildPlan(ctx, View{Graph: g, Remote: e.Remote}, *input.Commit, txn.At)
		} else if input.Import != nil && input.Commit == nil && input.Ingest == nil && txn.Purpose == "build" {
			plan, err = BuildImportPlan(ctx, View{Graph: g, Remote: e.Remote}, *input.Import)
		} else {
			err = invalid("invalid durable graph transaction input")
		}

		if err != nil {
			if !errors.Is(err, storage.ErrInvalidArgument) && !errors.Is(err, ErrReadBudget) && !errors.Is(err, ErrQuotaExceeded) {
				return txn, err
			}
			code := ""
			if errors.Is(err, ErrQuotaExceeded) {
				code = "quota_exceeded"
			}
			if abortErr := e.updateTransaction(ctx, g, Action{Operation: "abort", Txn: txn.ID, Sequence: txn.Sequence, Error: err.Error(), ErrorCode: code, IngestResult: failedIngestResult(input.Ingest, err)}, &txn); abortErr != nil {
				return txn, abortErr
			}
			return e.finish(ctx, g, txn)
		}
		participants := planParticipants(plan)
		if len(txn.Participants) == 0 {
			if err := e.updateTransaction(ctx, g, Action{Operation: "participants", Txn: txn.ID, Sequence: txn.Sequence, Participants: participants}, &txn); err != nil {
				return txn, err
			}
		} else if !slices.Equal(txn.Participants, participants) {
			return txn, conflict("participant set is immutable")
		}
		var actions []Action
		for _, id := range participants {
			if _, recorded := txn.Prepared[id]; recorded {
				continue
			}
			changes := plan.Changes.Rows(id)
			payload, _ := json.Marshal(changes)
			actions = append(actions, Action{Operation: "prepare", Tenant: e.Tenant, Generation: g.Generation, Partition: id, Epoch: g.Partitions[id].Epoch, Txn: txn.ID, Sequence: txn.Sequence, Base: g.Roots[id], Changes: changes, Digest: fmt.Sprintf("%x", sha256.Sum256(payload))})
		}
		var preparedBatch map[uint8]Prepared
		if e.BatchFragments {
			var prepared []Prepared
			finish := e.Metrics.Start("partition_txn_prepare")
			prepared, err = ExecuteFragmentActions[Prepared](ctx, e.Remote, g, actions, true)
			finish(err)
			if err == nil {
				preparedBatch = make(map[uint8]Prepared, len(prepared))
				for i, p := range prepared {
					preparedBatch[actions[i].Partition] = p
				}
			}
		} else {
			err = e.prepareLegacy(ctx, g, actions)
		}
		if err != nil {
			if errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, storage.ErrConflict) {
				if abortErr := e.updateTransaction(ctx, g, Action{Operation: "abort", Txn: txn.ID, Sequence: txn.Sequence, Error: err.Error(), IngestResult: failedIngestResult(input.Ingest, err)}, &txn); abortErr != nil {
					return txn, abortErr
				}
				return e.finish(ctx, g, txn)
			}
			return txn, err
		}
		if !e.BatchFragments && len(txn.Prepared) != len(participants) {
			txn, err = e.Remote.Transaction(ctx, e.Home, e.Tenant, g.Generation, txn.Identity)
			if err != nil {
				return txn, err
			}
		}
		if txn.State != "preparing" {
			return e.finish(ctx, g, txn)
		}
		version := g.Version
		if plan.Report.Changed && txn.Purpose != "build" {
			version++
		}
		buckets := g.HashBuckets
		for _, id := range participants {
			p, ok := preparedBatch[id]
			if !ok {
				p = txn.Prepared[id]
			}
			for index, digest := range p.HashBuckets {
				if index < 0 || index >= len(buckets) {
					return txn, fmt.Errorf("invalid prepare logical bucket")
				}
				buckets[index] = digest
			}
		}
		digest, err := graph.ContentHashFromBuckets(buckets)
		if err != nil {
			return txn, err
		}
		result := storage.CommitResult{Manifest: storage.Manifest{TenantID: e.Tenant, Version: version, HeadCommitID: txn.ID, UpdatedAt: txn.At}, ReadableVersion: version, DataHash: digest, Skipped: !plan.Report.Changed, Suppressed: plan.Report.Suppressed, CanonicalEntities: plan.Report.CanonicalEntities, CanonicalEdges: plan.Report.CanonicalEdges}
		if plan.IngestResult != nil {
			// The bounded ingest result already carries the public diagnostics.
			result.Suppressed, result.CanonicalEntities, result.CanonicalEdges = nil, nil, nil
			if plan.IngestResult.Applied > 0 {
				plan.IngestResult.Version = version
			}
		}
		if err := e.updateTransaction(ctx, g, Action{Operation: "commit", Txn: txn.ID, Sequence: txn.Sequence, PreparedBatch: preparedBatch, Result: &result, IngestResult: plan.IngestResult, CITypes: plan.CITypes, RelationTypes: plan.RelationTypes}, &txn); err != nil {
			return txn, err
		}
	}
	return e.finish(ctx, g, txn)
}

func planParticipants(plan Plan) []uint8 {
	if ids := plan.Changes.Participants(); len(ids) > 0 {
		return ids
	}
	return []uint8{0}
}

func (e Engine) prepareLegacy(ctx context.Context, g Graph, actions []Action) error {
	workers, workCtx := errgroup.WithContext(ctx)
	workers.SetLimit(4)
	for _, a := range actions {
		workers.Go(func() error {
			group, err := g.GroupFor(a.Partition)
			if err != nil {
				return err
			}
			var prepared Prepared
			if err := e.Remote.Action(workCtx, group, a, &prepared); err != nil {
				return err
			}
			return e.control(workCtx, g, Action{Operation: "prepared", Txn: a.Txn, Sequence: a.Sequence, Partition: a.Partition, Prepared: &prepared}, nil)
		})
	}
	return workers.Wait()
}

func (e Engine) finish(ctx context.Context, g Graph, txn Transaction) (Transaction, error) {
	if txn.State != "committed" && txn.State != "aborted" {
		return txn, conflict("graph transaction is undecided")
	}
	operation := "discard"
	if txn.State == "committed" {
		operation = "finalize"
	}
	actions := make([]Action, 0, len(txn.Participants))
	for _, id := range txn.Participants {
		a := Action{Operation: operation, Tenant: e.Tenant, Generation: g.Generation, Partition: id, Epoch: g.Partitions[id].Epoch, Txn: txn.ID, Sequence: txn.Sequence}
		if prepared, ok := txn.Prepared[id]; ok {
			a.Prepared = &prepared
		}
		actions = append(actions, a)
	}
	finish := e.Metrics.Start("partition_txn_finalize")
	_, err := ExecuteFragmentActions[struct{}](ctx, e.Remote, g, actions, e.BatchFragments)
	finish(err)
	if err != nil {
		return txn, err
	}
	var completed Transaction
	complete := Action{Operation: "complete", Txn: txn.ID, Sequence: txn.Sequence}
	if e.BatchFragments && txn.Purpose == "ingest" && len(txn.Inputs) == 1 {
		complete.ReceiptRef = txn.Inputs[0].Acceptance
	}
	if err := e.control(ctx, g, complete, &completed); err != nil {
		return txn, err
	}
	return completed, nil
}
