package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) writeEntityPages(ctx context.Context, tenantID string, g *graph.Graph, version int64) error {
	return s.writeParquetEntityPages(ctx, tenantID, buildEntityPages(g, version), version)
}

func (s *TenantStore) writeEntityPagesWithFormat(ctx context.Context, tenantID string, g *graph.Graph, version int64, pages []EntityPageData, format string) error {
	normalized, err := normalizeIndexFormat(format)
	if err != nil {
		return err
	}
	if normalized != IndexFormatParquet {
		return fmt.Errorf("unsupported index format %q", format)
	}
	return s.writeParquetEntityPages(ctx, tenantID, pages, version)
}

func buildEntityPages(g *graph.Graph, version int64) []EntityPageData {
	counts := entityPageCounts(g)
	now := time.Now().UTC()
	pages := newEntityPageBuckets(counts, version, now)
	for _, entity := range g.Entities {
		appendEntityPage(pages, entity)
	}
	return finishEntityPages(pages)
}

func buildEntityPagesFromEntities(entities []graph.Entity, version int64) []EntityPageData {
	counts := make(map[string]int, len(entities))
	for _, entity := range entities {
		counts[entityShardID(entity.ID)]++
	}
	now := time.Now().UTC()
	pages := newEntityPageBuckets(counts, version, now)
	for _, entity := range entities {
		appendEntityPage(pages, entity)
	}
	return finishEntityPages(pages)
}

func newEntityPageBuckets(counts map[string]int, version int64, now time.Time) map[string]EntityPageData {
	pages := make(map[string]EntityPageData, len(counts))
	for shardID, count := range counts {
		pages[shardID] = EntityPageData{
			LayoutVersion: CurrentObjectLayoutVersion,
			Shard:         shardID,
			Entities:      make([]graph.Entity, 0, count),
			Version:       version,
			UpdatedAt:     now,
			hashCanonical: true,
		}
	}
	return pages
}

func appendEntityPage(pages map[string]EntityPageData, entity graph.Entity) {
	shardID := entityShardID(entity.ID)
	page := pages[shardID]
	page.Entities = append(page.Entities, entity)
	page.hashCanonical = page.hashCanonical && graphEntityHashCanonical(entity)
	pages[shardID] = page
}

func finishEntityPages(pages map[string]EntityPageData) []EntityPageData {
	items := make([]EntityPageData, 0, len(pages))
	for _, page := range pages {
		sort.Slice(page.Entities, func(i, j int) bool { return page.Entities[i].ID < page.Entities[j].ID })
		items = append(items, page)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Shard < items[j].Shard })
	return items
}

func entityPagePackIDs(pages []EntityPageSpec, packPages bool, maxBytes int64) map[string]string {
	if !packPages {
		return nil
	}
	items := make([]indexPackItem, 0, len(pages))
	for _, page := range pages {
		items = append(items, indexPackItem{ID: page.Shard, Group: "entities", Rows: page.EntityCount, Bytes: page.estimatedBytes})
	}
	return indexPackMap(planIndexPacksWithMaxBytes(items, maxBytes))
}

func entityPageDataPackGroups(pages []EntityPageData, packPages bool, maxBytes int64) []entityPageDataPackGroup {
	if !packPages {
		out := make([]entityPageDataPackGroup, 0, len(pages))
		for _, page := range pages {
			out = append(out, entityPageDataPackGroup{ID: page.Shard, Pages: []EntityPageData{page}})
		}
		return out
	}
	items := make([]indexPackItem, 0, len(pages))
	byShard := map[string]EntityPageData{}
	for _, page := range pages {
		items = append(items, indexPackItem{ID: page.Shard, Group: "entities", Rows: len(page.Entities), Bytes: entityPagePackBytes(page)})
		byShard[page.Shard] = page
	}
	groups := planIndexPacksWithMaxBytes(items, maxBytes)
	out := make([]entityPageDataPackGroup, 0, len(groups))
	for _, group := range groups {
		packed := entityPageDataPackGroup{ID: group.ID}
		for _, item := range group.Items {
			packed.Pages = append(packed.Pages, byShard[item.ID])
		}
		out = append(out, packed)
	}
	return out
}

type entityPageDataPackGroup struct {
	ID    string
	Pages []EntityPageData
}

func mergeEntityPagePack(group entityPageDataPackGroup) EntityPageData {
	if len(group.Pages) == 1 {
		return group.Pages[0]
	}
	first := group.Pages[0]
	merged := EntityPageData{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      first.TenantID,
		Shard:         group.ID,
		Version:       first.Version,
		UpdatedAt:     first.UpdatedAt,
		hashCanonical: true,
	}
	for _, page := range group.Pages {
		merged.Entities = append(merged.Entities, page.Entities...)
		merged.hashCanonical = merged.hashCanonical && page.hashCanonical
	}
	sort.Slice(merged.Entities, func(i, j int) bool { return merged.Entities[i].ID < merged.Entities[j].ID })
	return merged
}

func entityPageCounts(g *graph.Graph) map[string]int {
	pages := map[string]int{}
	for _, entity := range g.Entities {
		pages[entityShardID(entity.ID)]++
	}
	return pages
}

func entityShardID(id string) string {
	return hashedIndexShardID(id)
}

func (s *TenantStore) tombstoneStaleEntityRecords(ctx context.Context, tenantID string, currentIDs map[string]struct{}, version int64) error {
	objects, err := s.Objects.List(ctx, s.entityRecordPrefix(tenantID))
	if err != nil {
		return err
	}
	for _, object := range objects {
		entityID, ok, err := s.entityIDFromRecordKey(tenantID, object.Key)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, current := currentIDs[entityID]; current {
			continue
		}
		meta, err := objectMeta(ctx, s.Objects, object.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if meta.ETag == "" {
			return fmt.Errorf("entity record %q missing etag for safe stale cleanup", object.Key)
		}
		record := EntityRecord{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			ID:            entityID,
			Page:          entityShardID(entityID),
			Deleted:       true,
			Version:       version,
			UpdatedAt:     time.Now().UTC(),
		}
		stampEntityRecordHash(&record)
		if err := s.putEntityRecordWithMeta(ctx, object.Key, record, meta); err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) entityIDFromRecordKey(tenantID string, key string) (string, bool, error) {
	prefix := s.entityRecordPrefix(tenantID)
	if !strings.HasPrefix(key, prefix) {
		return "", false, fmt.Errorf("entity record key %q is outside prefix %q", key, prefix)
	}
	rawName := strings.TrimPrefix(key, prefix)
	suffix := ""
	switch {
	case strings.HasSuffix(rawName, ".parquet"):
		suffix = ".parquet"
	default:
		return "", false, nil
	}
	name := strings.TrimSuffix(rawName, suffix)
	if path.Base(name) != name || name == "" {
		return "", false, nil
	}
	id, err := url.PathUnescape(name)
	if err != nil {
		return "", false, nil
	}
	if strings.TrimSpace(id) == "" {
		return "", false, nil
	}
	return id, true, nil
}

func (s *TenantStore) deleteListedObject(ctx context.Context, object ObjectInfo) error {
	if object.ETag != "" {
		err := s.Objects.DeleteConditional(ctx, object.Key, PutCondition{IfMatch: object.ETag})
		if err == nil || errors.Is(err, ErrNotFound) {
			return nil
		}
		if !errors.Is(err, ErrConditionalDeleteUnsupported) {
			return err
		}
	}
	return s.Objects.Delete(ctx, object.Key)
}
