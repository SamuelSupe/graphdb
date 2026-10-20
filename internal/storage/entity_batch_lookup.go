package storage

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

const entityBatchLookupConcurrency = 16

func (l *PersistedIndexLookup) GetEntities(ctx context.Context, ids []string, fields []string) (map[string]graph.Entity, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return nil, false, nil
	}
	if len(ids) == 0 {
		return map[string]graph.Entity{}, true, nil
	}
	if err := l.compileCatalog(); err != nil {
		return nil, false, nil
	}
	groups := map[string][]string{}
	specs := map[string]EntityPageSpec{}
	parents := map[string]EntityPageSpec{}
	rangeIndexes := map[string]int{}
	for _, id := range ids {
		for _, shard := range indexShardIDCandidates(id) {
			spec, exists := l.catalogEntityPageSpec(shard)
			if !exists {
				continue
			}
			key := shard
			if hasIndexRanges(spec.Objects) {
				n := rangeObjectIndex(spec.Objects, id)
				object := spec.Objects[n]
				key = object.Key
				parents[key] = spec
				rangeIndexes[key] = n
				spec = entityRangeSpec(spec, object)
			}
			specs[key] = spec
			groups[key] = append(groups[key], id)
		}
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
		if _, ok := specs[shard]; !ok {
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
			var page EntityPageData
			var available bool
			var err error
			if parent, ranged := parents[shard]; ranged {
				page, available, err = l.Store.borrowEntityRange(ctx, l.TenantID, l.Version, parent, rangeIndexes[shard])
			} else {
				page, _, available, err = l.Store.borrowValidatedParquetEntityPageObject(ctx, l.TenantID, l.Version, specs[shard])
			}
			if !available && err == nil {
				err = ErrNotFound
			}
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
				n := sort.Search(len(page.Entities), func(n int) bool { return page.Entities[n].ID >= id })
				if n == len(page.Entities) || page.Entities[n].ID != id {
					continue
				}
				entity := page.Entities[n]
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
	for _, id := range ids {
		if _, exists := out[id]; !exists {
			return nil, false, nil
		}
	}
	return out, true, nil
}
