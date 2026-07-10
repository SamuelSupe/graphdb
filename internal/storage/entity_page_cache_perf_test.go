package storage

import (
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestEntityPageCacheUsesByteBound(t *testing.T) {
	cache := newEntityPageCache(10)
	cache.maxBytes = 64
	cache.put("large", cachedEntityPage{
		page: EntityPageData{Entities: []graph.Entity{{ID: "host:a", Kind: "host"}}},
	})
	if len(cache.data) != 0 || cache.bytes != 0 {
		t.Fatalf("oversized page retained: entries=%d bytes=%d", len(cache.data), cache.bytes)
	}
}

func TestEntityPageCacheRevalidationPolicy(t *testing.T) {
	strict := newEntityPageCache(1)
	entry := cachedEntityPage{validatedAt: time.Now()}
	if !strict.needsRevalidation(entry) {
		t.Fatal("default cache must revalidate every hit")
	}
	configured := newConfiguredEntityPageCache(1)
	if configured.needsRevalidation(entry) {
		t.Fatal("configured cache revalidated a fresh immutable object")
	}
	entry.validatedAt = time.Now().Add(-time.Minute)
	if !configured.needsRevalidation(entry) {
		t.Fatal("configured cache did not revalidate an expired object")
	}
}
