package partition

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"golang.org/x/sync/errgroup"
)

const MaxFragmentBatch = 32
const MaxFragmentBatchBytes = 4 << 20

func fragmentBatchLimit(operation string) int {
	if operation == "data_gc_step" {
		return MaxFragmentBatch / 2
	}
	return MaxFragmentBatch
}

type batchActor interface {
	FragmentBatch(context.Context, Group, []Action) ([]json.RawMessage, error)
}

// ExecuteFragmentActions runs independent fragment actions in bounded waves.
// Results retain input order so the coordinator can identify each partition.
func ExecuteFragmentActions[T any](ctx context.Context, remote Remote, g Graph, actions []Action, batch bool) ([]T, error) {
	results := make([]T, len(actions))
	groups := make(map[string][]int)
	batcher, supportsBatch := remote.(batchActor)
	workers, workCtx := errgroup.WithContext(ctx)
	workers.SetLimit(4)
	for i, action := range actions {
		group, err := g.GroupFor(action.Partition)
		if err != nil {
			_ = workers.Wait()
			return nil, err
		}
		if batch && supportsBatch {
			groups[group.ID] = append(groups[group.ID], i)
			continue
		}
		workers.Go(func() error {
			return remote.Action(workCtx, group, action, &results[i])
		})
	}
	for id, indices := range groups {
		group := g.Groups[id]
		workers.Go(func() error {
			limit := fragmentBatchLimit(actions[indices[0]].Operation)
			for start := 0; start < len(indices); start += limit {
				chunk := indices[start:min(len(indices), start+limit)]
				payload := make([]Action, 0, len(chunk))
				for _, index := range chunk {
					payload = append(payload, actions[index])
				}
				replies, err := batcher.FragmentBatch(workCtx, group, payload)
				if err != nil {
					return err
				}
				if len(replies) != len(chunk) {
					return fmt.Errorf("invalid fragment batch response length")
				}
				for i, reply := range replies {
					if err := json.Unmarshal(reply, &results[chunk[i]]); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return results, workers.Wait()
}

func ValidateFragmentBatch(actions []Action) error {
	if len(actions) == 0 || len(actions) > fragmentBatchLimit(actions[0].Operation) {
		return invalid("invalid fragment batch length")
	}
	first := actions[0]
	switch first.Operation {
	case "prepare", "finalize", "discard", "data_gc_begin", "data_gc_step":
	default:
		return invalid("unsupported fragment batch operation")
	}
	var seen [256]bool
	for _, a := range actions {
		if a.Operation != first.Operation || a.Tenant != first.Tenant || a.Generation != first.Generation ||
			a.Txn != first.Txn || a.Sequence != first.Sequence || a.Purpose != first.Purpose || seen[a.Partition] {
			return invalid("fragment batch crosses a transaction or repeats a partition")
		}
		seen[a.Partition] = true
	}
	return nil
}

// ApplyFragment loads the partition once inside the application journal. The
// caller owns its partition and must discard buffered writes before retrying.
func (s Store) ApplyFragment(ctx context.Context, a Action) (any, error) {
	if storage.ValidateTenantID(a.Tenant) != nil || a.Generation <= 0 {
		return nil, invalid("valid tenant and graph generation are required")
	}
	state, err := s.Data(ctx, a.Tenant, a.Generation, a.Partition)
	if err != nil {
		return nil, err
	}
	switch a.Operation {
	case "prepare", "finalize", "discard":
		return s.applyDataState(ctx, a, state)
	case "data_gc_begin", "data_gc_step":
		return s.applyDataGCState(ctx, a, state)
	default:
		return nil, invalid("unsupported fragment batch operation")
	}
}
