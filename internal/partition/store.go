package partition

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (s Store) namespace(tenant string, generation int64) string {
	return path.Join(s.Prefix, "control/graphs", tenant, strconv.FormatInt(generation, 10))
}
func (s Store) graphKey(tenant string) string {
	return path.Join(s.Prefix, "control/graphs", tenant, "graph.json")
}
func (s Store) dataKey(tenant string, generation int64, bucket uint8) string {
	return path.Join(s.namespace(tenant, generation), fmt.Sprintf("partitions/%02x/state.json", bucket))
}
func (s Store) Tree(tenant string, generation int64, bucket uint8) Tree {
	return Tree{Objects: s.Objects, Prefix: path.Join(s.namespace(tenant, generation), fmt.Sprintf("partitions/%02x/pages", bucket))}
}
func (s Store) inputKey(tenant string, generation int64, stage, digest string) string {
	return path.Join(s.namespace(tenant, generation), "inputs", stage, digest)
}
func (s Store) txnKey(tenant string, generation int64, id string) string {
	return path.Join(s.namespace(tenant, generation), "transactions", fmt.Sprintf("%x", sha256.Sum256([]byte(id)))) + ".json"
}

func (s Store) GetGraph(ctx context.Context, tenant string) (Graph, error) {
	var g Graph
	err := s.load(ctx, s.graphKey(tenant), &g)
	if err == nil && (g.Format != FormatVersion || g.Tenant != tenant || g.Generation <= 0) {
		err = fmt.Errorf("unsupported or invalid partitioned graph")
	}
	return g, err
}
func (s Store) Data(ctx context.Context, tenant string, generation int64, bucket uint8) (DataState, error) {
	var state DataState
	err := s.load(ctx, s.dataKey(tenant, generation, bucket), &state)
	if err == nil && (state.Tenant != tenant || state.Generation != generation || state.Partition != bucket || state.Epoch == 0) {
		err = fmt.Errorf("partition metadata scope mismatch")
	}
	return state, err
}
func (s Store) Transaction(ctx context.Context, tenant string, generation int64, identity string) (Transaction, error) {
	var txn Transaction
	err := s.load(ctx, s.txnKey(tenant, generation, identity), &txn)
	if err == nil && (txn.Tenant != tenant || txn.Generation != generation || txn.Identity != identity) {
		err = fmt.Errorf("transaction metadata scope mismatch")
	}
	return txn, err
}
func (s Store) Input(ctx context.Context, tenant string, generation int64, input Input) ([]byte, error) {
	if !validTokenID(input.Stage) || !validDigest(input.Digest) || input.Bytes <= 0 {
		return nil, invalid("invalid transaction input reference")
	}
	var data []byte
	var err error
	if input.Acceptance != "" {
		if !validDigest(input.Acceptance) || input.Bytes > MaxChangeBytes || s.ReadIngestInput == nil {
			return nil, invalid("invalid ingest acceptance input reference")
		}
		data, err = s.ReadIngestInput(ctx, tenant, generation, input.Acceptance)
	} else {
		if input.Bytes > MaxValueBytes {
			return nil, fmt.Errorf("invalid transaction input reference")
		}
		data, err = s.Objects.Get(ctx, s.inputKey(tenant, generation, input.Stage, input.Digest))
	}
	if errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("%w: durable graph input %s is missing", errInputMissing, input.Digest)
	}
	if err == nil && (len(data) != input.Bytes || fmt.Sprintf("%x", sha256.Sum256(data)) != input.Digest) {
		if input.Acceptance != "" {
			return nil, invalid("graph input differs from its acceptance")
		}
		err = fmt.Errorf("durable graph input checksum mismatch")
	}
	return data, err
}

// RetireIngestDecision is called in the same Raft application that saves the
// terminal acceptance, once every participant has finished the transaction.
func (s Store) RetireIngestDecision(ctx context.Context, txn Transaction) error {
	return s.Objects.Delete(ctx, s.txnKey(txn.Tenant, txn.Generation, txn.Identity))
}

func invalid(message string) error  { return fmt.Errorf("%w: %s", storage.ErrInvalidArgument, message) }
func conflict(message string) error { return fmt.Errorf("%w: %s", storage.ErrConflict, message) }

func (s Store) Apply(ctx context.Context, a Action) (any, error) {
	if storage.ValidateTenantID(a.Tenant) != nil || a.Generation <= 0 {
		return nil, invalid("valid tenant and graph generation are required")
	}
	if strings.HasPrefix(a.Operation, "data_gc_") {
		return s.applyDataGC(ctx, a)
	}
	if strings.HasPrefix(a.Operation, "data_move_") {
		return s.applyDataMove(ctx, a)
	}
	switch a.Operation {
	case "reserve_partitions":
		if len(a.Participants) == 0 || len(a.Participants) > 256 {
			return nil, invalid("invalid partition reservation set")
		}
		for _, id := range a.Participants {
			state, err := s.Data(ctx, a.Tenant, a.Generation, id)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			if err == nil && (state.Epoch != a.Epoch || state.State != "active") {
				return nil, conflict("partition reservation conflicts with ownership")
			}
		}
		for _, id := range a.Participants {
			next := a
			next.Operation = "reserve"
			next.Partition = id
			if _, err := s.applyData(ctx, next); err != nil {
				return nil, err
			}
		}
		return map[string]any{"reserved": len(a.Participants)}, nil
	case "stage_input":
		if !validTokenID(a.Txn) || len(a.Data) == 0 || len(a.Data) > MaxValueBytes || !validDigest(a.Digest) || fmt.Sprintf("%x", sha256.Sum256(a.Data)) != a.Digest {
			return nil, invalid("invalid graph input chunk")
		}
		key := s.inputKey(a.Tenant, a.Generation, a.Txn, a.Digest)
		_, err := s.Objects.PutConditional(ctx, key, a.Data, storage.PutCondition{IfNoneMatch: true})
		if errors.Is(err, storage.ErrConflict) {
			existing, readErr := s.Objects.Get(ctx, key)
			if readErr != nil {
				return nil, readErr
			}
			if string(existing) != string(a.Data) {
				return nil, fmt.Errorf("durable graph input changed")
			}
			err = nil
		}
		return Input{Stage: a.Txn, Digest: a.Digest, Bytes: len(a.Data)}, err
	case "reserve", "prepare", "finalize", "discard", "fence":
		return s.applyData(ctx, a)
	default:
		return s.applyControl(ctx, a)
	}
}
