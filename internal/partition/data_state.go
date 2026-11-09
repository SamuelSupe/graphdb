package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (s Store) applyData(ctx context.Context, a Action) (any, error) {
	state, err := s.Data(ctx, a.Tenant, a.Generation, a.Partition)
	if a.Operation == "reserve" {
		if a.Epoch == 0 {
			return nil, invalid("partition epoch is required")
		}
		if err == nil {
			if state.Epoch == a.Epoch && state.State == "active" {
				return state, nil
			}
			return nil, conflict("partition already reserved")
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		state = DataState{Tenant: a.Tenant, Generation: a.Generation, Partition: a.Partition, Epoch: a.Epoch, State: "active"}
		return state, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
	}
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("required partition ownership is missing")
		}
		return nil, err
	}
	if state.Epoch != a.Epoch || state.State != "active" {
		return nil, conflict("partition ownership changed")
	}
	if a.Txn == "" || a.Sequence == 0 {
		return nil, invalid("transaction identity is required")
	}
	switch a.Operation {
	case "prepare":
		encoded, err := json.Marshal(a.Changes)
		if err != nil {
			return nil, err
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(encoded))
		if a.Digest != digest {
			return nil, invalid("prepare digest does not match changes")
		}
		if state.Prepared != nil {
			p := state.Prepared
			if p.Txn == a.Txn && p.Sequence == a.Sequence && p.Base == a.Base && p.Digest == a.Digest {
				return *p, nil
			}
			return nil, conflict("partition has an unresolved transaction")
		}
		if a.Sequence <= state.Sequence || state.Root != a.Base {
			return nil, conflict("partition base or transaction sequence changed")
		}
		tree := s.Tree(a.Tenant, a.Generation, a.Partition)
		var captured []Row
		if state.Capture != "" || state.GC != nil && state.GC.Phase != "cleanup" {
			tree.saved = func(_ context.Context, hash string) error {
				captured = append(captured, Row{Key: hash, Value: json.RawMessage(`true`)})
				return nil
			}
		}
		root, err := tree.Update(ctx, a.Base, a.Changes)
		if err != nil {
			return nil, err
		}
		if err := s.recordNewPages(ctx, &state, uniqueHashRows(captured)); err != nil {
			return nil, err
		}
		prepared := Prepared{EdgeTypeDelta: map[string]int64{}, Txn: a.Txn, Sequence: a.Sequence, Base: a.Base, Root: root, Digest: a.Digest}
		for _, row := range a.Changes {
			entity := strings.HasPrefix(row.Key, Key("entity"))
			edge := strings.HasPrefix(row.Key, Key("edge"))
			if !entity && !edge {
				continue
			}
			previous, found, err := s.Tree(a.Tenant, a.Generation, a.Partition).Get(ctx, a.Base, row.Key)
			if err != nil {
				return nil, err
			}
			delta := int64(0)
			if found {
				delta--
			}
			if len(row.Value) > 0 {
				delta++
			}
			if entity {
				prepared.EntityDelta += delta
			} else {
				prepared.EdgeDelta += delta
				if found {
					var record graph.Edge
					if err := json.Unmarshal(previous, &record); err != nil {
						return nil, err
					}
					prepared.EdgeTypeDelta[record.Type]--
				}
				if len(row.Value) > 0 {
					var record graph.Edge
					if err := json.Unmarshal(row.Value, &record); err != nil {
						return nil, err
					}
					prepared.EdgeTypeDelta[record.Type]++
				}
			}
		}
		prepared.HashBuckets, err = s.Tree(a.Tenant, a.Generation, a.Partition).LogicalBuckets(ctx, root, a.Changes)
		if err != nil {
			return nil, err
		}
		state.Prepared = &prepared
		return prepared, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
	case "finalize", "discard":
		if state.Sequence == a.Sequence && state.LastTxn == a.Txn && state.Prepared == nil {
			return state, nil
		}
		if state.Sequence >= a.Sequence {
			return nil, conflict("transaction already superseded")
		}
		if state.Prepared != nil && (state.Prepared.Txn != a.Txn || state.Prepared.Sequence != a.Sequence) {
			return nil, conflict("different transaction is prepared")
		}
		if a.Operation == "finalize" {
			if state.Prepared == nil || a.Prepared == nil || !samePrepared(*a.Prepared, *state.Prepared) {
				return nil, conflict("committed prepare result does not match")
			}
			state.Root = state.Prepared.Root
		}
		state.Prepared = nil
		state.Sequence = a.Sequence
		state.LastTxn = a.Txn
	case "fence":
		if state.Prepared != nil || a.Sequence <= state.Sequence {
			return nil, conflict("resolve transactions before fencing")
		}
		state.State = "frozen"
		state.Sequence = a.Sequence
	}
	return state, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
}

func samePrepared(a, b Prepared) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}
