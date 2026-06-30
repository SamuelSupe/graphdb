package storage

import (
	"context"
	"errors"
	"sync"

	"graphdb/internal/graph"
)

const entityBatchLookupConcurrency = 16

func (l *PersistedIndexLookup) GetEntities(ctx context.Context, ids []string, fields []string) (map[string]graph.Entity, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return nil, false, nil
	}
	if len(ids) == 0 {
		return map[string]graph.Entity{}, true, nil
	}
	groups := map[string][]string{}
	for _, id := range ids {
		groups[entityShardID(id)] = append(groups[entityShardID(id)], id)
	}

	out := make(map[string]graph.Entity, len(ids))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, entityBatchLookupConcurrency)
	var firstErr error
	unavailable := false
	setUnavailable := func() {
		mu.Lock()
		unavailable = true
		mu.Unlock()
	}
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	for shard, shardIDs := range groups {
		if _, ok := l.catalogEntityPageSpec(shard); !ok {
			return nil, false, nil
		}
		shard := shard
		shardIDs := append([]string(nil), shardIDs...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				setErr(ctx.Err())
				return
			}
			page, err := l.loadEntityPage(ctx, shard)
			if errors.Is(err, ErrNotFound) {
				setUnavailable()
				return
			}
			if err != nil {
				setErr(err)
				return
			}
			local := map[string]graph.Entity{}
			for _, id := range shardIDs {
				entity, ok := l.entityFromCachedPage(shard, id, page)
				if !ok {
					setUnavailable()
					continue
				}
				local[id] = trimEntityFields(entity, fields)
			}
			mu.Lock()
			for id, entity := range local {
				out[id] = entity
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, false, firstErr
	}
	if unavailable {
		return nil, false, nil
	}
	return out, true, nil
}
