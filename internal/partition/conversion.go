package partition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (s Store) conversionCancellationKey(tenant string) string {
	return path.Join(s.Prefix, "control/graphs", tenant, "conversion-cancellation.json")
}

func (s Store) LastConversionCancellation(ctx context.Context, tenant string) (ConversionCancellation, error) {
	var cancelled ConversionCancellation
	err := s.load(ctx, s.conversionCancellationKey(tenant), &cancelled)
	if errors.Is(err, storage.ErrNotFound) {
		return ConversionCancellation{}, nil
	}
	if err == nil && (cancelled.Generation <= 0 || cancelled.Activated) {
		return ConversionCancellation{}, fmt.Errorf("invalid graph conversion cancellation")
	}
	return cancelled, err
}

func (s Store) abandonedKey(tenant string, generation int64) string {
	return s.namespace(tenant, generation) + ".abandoned"
}

func (s Store) isAbandoned(ctx context.Context, tenant string, generation int64) (bool, error) {
	_, err := s.Objects.Get(ctx, s.abandonedKey(tenant, generation))
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s Store) cancelConversion(ctx context.Context, a Action) (ConversionCancellation, error) {
	last, err := s.LastConversionCancellation(ctx, a.Tenant)
	if err != nil {
		return last, err
	}
	if last.Generation != 0 && a.ConversionAttempt <= last.Attempt {
		if a.ConversionAttempt == last.Attempt {
			return last, nil
		}
		return last, conflict("graph conversion attempt changed")
	}
	decision := ConversionCancellation{Attempt: a.ConversionAttempt, Generation: a.Generation}
	g, err := s.GetGraph(ctx, a.Tenant)
	hasGraph := false
	if err == nil {
		if a.Generation != g.Generation || (g.ConversionAttempt != 0 && g.ConversionAttempt != a.ConversionAttempt) {
			return decision, conflict("graph conversion attempt changed")
		}
		if g.State == "active" {
			decision.Activated = true
			return decision, nil
		}
		if g.State != "initializing" {
			return decision, conflict("graph is not awaiting activation")
		}
		hasGraph = true
	} else if !errors.Is(err, storage.ErrNotFound) {
		return decision, err
	}
	states, marked, err := s.collectAbandonableStates(ctx, a.Tenant, a.Generation)
	if err != nil {
		return decision, err
	}
	if hasGraph {
		tenants, err := s.GraphTenants(ctx)
		if err != nil {
			return decision, err
		}
		if err := s.save(ctx, path.Join(s.Prefix, "control/graphs/registry.json"), slices.DeleteFunc(tenants, func(id string) bool { return id == a.Tenant })); err != nil {
			return decision, err
		}
		if err := s.Objects.Delete(ctx, s.graphKey(a.Tenant)); err != nil {
			return decision, err
		}
	}
	if !marked {
		if err := s.deleteConversionObjects(ctx, states); err != nil {
			return decision, err
		}
		if err := s.Objects.Put(ctx, s.abandonedKey(a.Tenant, a.Generation), []byte("abandoned")); err != nil {
			return decision, err
		}
	}
	return decision, s.save(ctx, s.conversionCancellationKey(a.Tenant), decision)
}

func (s Store) collectAbandonableStates(ctx context.Context, tenant string, generation int64) ([]string, bool, error) {
	marked, err := s.isAbandoned(ctx, tenant, generation)
	if err != nil || marked {
		return nil, marked, err
	}
	// Remove ownership before reclaiming pages. A missing partition then
	// checks the durable marker and rejects late fragment and reserve commands.
	var states []string
	for i := range 256 {
		key := s.dataKey(tenant, generation, uint8(i))
		var state DataState
		err := s.load(ctx, key, &state)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		} else if err != nil {
			return nil, false, err
		}
		nonRecoveryGC := (state.GC != nil && state.GC.RecoverySequence == 0) || (state.LastGC != "" && !state.LastGCRecovery)
		if state.Epoch > 1 || nonRecoveryGC || state.Capture != "" || state.MoveVerify != nil {
			return nil, false, conflict("partition state shows post-conversion lifecycle activity")
		}
		states = append(states, key)
	}
	return states, false, nil
}

func (s Store) markAbandoned(ctx context.Context, tenant string, generation int64) error {
	states, marked, err := s.collectAbandonableStates(ctx, tenant, generation)
	if err != nil || marked {
		return err
	}
	if err := s.deleteConversionObjects(ctx, states); err != nil {
		return err
	}
	return s.Objects.Put(ctx, s.abandonedKey(tenant, generation), []byte("abandoned"))
}

func (s Store) reclaimConversion(ctx context.Context, a Action) (map[string]bool, error) {
	g, err := s.GetGraph(ctx, a.Tenant)
	if err == nil && g.Generation == a.Generation {
		return nil, conflict("published graph still owns this generation")
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	if err := s.markAbandoned(ctx, a.Tenant, a.Generation); err != nil {
		return nil, err
	}
	prefix := s.namespace(a.Tenant, a.Generation) + "/"
	const pageSize = 64
	var objects []storage.ObjectInfo
	var more bool
	if paged, ok := s.Objects.(interface {
		ListPage(context.Context, string, string, int) ([]storage.ObjectInfo, string, error)
	}); ok {
		var next string
		objects, next, err = paged.ListPage(ctx, prefix, "", pageSize)
		more = next != ""
	} else {
		objects, err = s.Objects.List(ctx, prefix)
		more = len(objects) > pageSize
		objects = objects[:min(pageSize, len(objects))]
	}
	if err != nil {
		return nil, err
	}
	var bytes int64
	keys := make([]string, 0, len(objects))
	for i, object := range objects {
		if i > 0 && bytes+object.Size > 4<<20 {
			more = true
			break
		}
		keys = append(keys, object.Key)
		bytes += object.Size
	}
	if err := s.deleteConversionObjects(ctx, keys); err != nil {
		return nil, err
	}
	return map[string]bool{"done": !more}, nil
}

func (s Store) deleteConversionObjects(ctx context.Context, keys []string) error {
	if journal, ok := s.Objects.(interface {
		JournalReplicatedObjects(context.Context, []string) error
	}); ok && len(keys) > 0 {
		if err := journal.JournalReplicatedObjects(ctx, keys); err != nil {
			return err
		}
	}
	for _, key := range keys {
		if err := s.Objects.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
