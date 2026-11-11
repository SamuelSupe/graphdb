package partition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

var errInputMissing = errors.New("graph input missing")

// ErrInputExpired asks a coordinator to restage a request that has not begun.
// Missing input in an active transaction still fails recovery and snapshots.
var ErrInputExpired = fmt.Errorf("%w: unclaimed graph input was reclaimed", storage.ErrConflict)

// InputGCPlan lists candidates before Raft application, keeping directory
// enumeration outside the application lock and off every follower's replay.
func (s Store) InputGCPlan(ctx context.Context, g Graph) (Action, error) {
	action := Action{Operation: "graph_gc_inputs", ExpectedCursor: g.GC.Cursor}
	prefix := path.Join(s.namespace(g.Tenant, g.Generation), "inputs") + "/"
	objects, next, err := s.objectPage(ctx, prefix, g.GC.Cursor, 32)
	if err != nil {
		return Action{}, err
	}
	var total int64
	cursor := g.GC.Cursor
	for _, object := range objects {
		stage, digest, found := strings.Cut(strings.TrimPrefix(object.Key, prefix), "/")
		if !found || !validTokenID(stage) || !validDigest(digest) || object.Size <= 0 || object.Size > MaxValueBytes {
			return Action{}, fmt.Errorf("invalid staged graph input object")
		}
		if total+object.Size > MaxChangeBytes {
			next = cursor
			break
		}
		action.Inputs = append(action.Inputs, Input{Stage: stage, Digest: digest, Bytes: int(object.Size)})
		total += object.Size
		cursor = object.Key
	}
	action.Cursor = next
	return action, nil
}

func (s Store) reclaimInputs(ctx context.Context, g Graph, action Action) error {
	if len(action.Inputs) > 32 {
		return invalid("graph input GC exceeds its object budget")
	}
	cursor := action.ExpectedCursor
	total := 0
	for _, input := range action.Inputs {
		key := s.inputKey(g.Tenant, g.Generation, input.Stage, input.Digest)
		total += input.Bytes
		if !validTokenID(input.Stage) || !validDigest(input.Digest) || input.Acceptance != "" ||
			input.Bytes <= 0 || input.Bytes > MaxValueBytes || total > MaxChangeBytes || key <= cursor {
			return invalid("invalid graph input GC candidate")
		}
		cursor = key
	}
	if action.Cursor != "" && (len(action.Inputs) == 0 || action.Cursor != cursor) {
		return invalid("graph input GC cursor made no progress")
	}
	for _, input := range action.Inputs {
		// Begin and this sweep share the Raft application order. A live
		// coordinator whose stage loses that race restages before retrying.
		if g.Active == nil || input.Stage != g.Active.ID {
			key := s.inputKey(g.Tenant, g.Generation, input.Stage, input.Digest)
			if err := s.Objects.Delete(ctx, key); err != nil {
				return err
			}
		}
	}
	return nil
}
