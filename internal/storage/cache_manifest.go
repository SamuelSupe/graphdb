package storage

import "context"

func (c *ReaderCache) manifestForCacheEntry(
	ctx context.Context,
	tenantID string,
	entry cacheEntry,
	entryExists bool,
) (Manifest, ObjectMeta, error) {
	if !entryExists || !c.Store.coordinated() {
		return c.Store.getManifest(ctx, tenantID)
	}
	head, headExists, err := c.Store.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return Manifest{}, ObjectMeta{}, err
	}
	if !headExists {
		return c.Store.getManifest(ctx, tenantID)
	}
	if readerCacheEntryMatchesCoordinatorHead(entry, head) {
		return entry.manifest, entry.meta, nil
	}
	return c.Store.getCoordinatedManifestAtHead(ctx, tenantID, head)
}

func readerCacheEntryMatchesCoordinatorHead(
	entry cacheEntry,
	head CoordinationHead,
) bool {
	return entry.graph != nil &&
		entry.graph.Version == head.GraphVersion &&
		manifestMetaMatchesCoordinatorHead(entry.manifest, entry.meta, head)
}
