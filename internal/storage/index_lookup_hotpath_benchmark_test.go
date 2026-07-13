package storage

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

var (
	benchmarkCatalogSpec EdgeShard
	benchmarkEntityValue any
)

func BenchmarkPersistedIndexLookupEdgeCatalog(b *testing.B) {
	catalog := benchmarkEdgeCatalog()
	lookup := &PersistedIndexLookup{Version: 1, Catalog: catalog}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relations := lookup.relationTypesForShard("2a", nil)
		for _, relation := range relations {
			var ok bool
			benchmarkCatalogSpec, ok = lookup.catalogEdgeShardSpec(relation, "2a")
			if !ok {
				b.Fatal("catalog edge shard missing")
			}
		}
	}
}

func BenchmarkPersistedIndexLookupEdgeCatalogCold(b *testing.B) {
	catalog := benchmarkEdgeCatalog()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lookup := &PersistedIndexLookup{Version: 1, Catalog: catalog}
		relations := lookup.relationTypesForShard("2a", nil)
		for _, relation := range relations {
			benchmarkCatalogSpec, _ = lookup.catalogEdgeShardSpec(relation, "2a")
		}
	}
}

func benchmarkEdgeCatalog() IndexCatalog {
	catalog := IndexCatalog{Version: 1}
	for relation := 0; relation < 256; relation++ {
		for shard := 0; shard < indexShardBuckets; shard++ {
			catalog.EdgeShards = append(catalog.EdgeShards, EdgeShard{
				RelationType: fmt.Sprintf("relation-%03d", relation),
				Shard:        fmt.Sprintf("%02x", shard),
			})
		}
	}
	return catalog
}

func BenchmarkTrimEntityFieldsWideProjection(b *testing.B) {
	entity := graph.Entity{
		ID:              "host:wide",
		Kind:            "host",
		Fields:          graph.Fields{},
		FieldWriteModes: map[string]string{},
		FieldSources:    map[string]graph.FieldSource{},
		Identity:        map[string]any{"hostname": "wide"},
	}
	for i := 0; i < 512; i++ {
		name := fmt.Sprintf("field-%03d", i)
		entity.Fields[name] = map[string]any{"values": []any{i, name, true}}
		entity.FieldWriteModes[name] = "replace"
		entity.FieldSources[name] = graph.FieldSource{Source: "benchmark"}
	}
	projection := []string{"field-256"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		projected := trimEntityFields(entity, projection)
		benchmarkEntityValue = projected.Fields["field-256"]
	}
}

func TestPersistedIndexLookupCachesEdgeCatalogPerShard(t *testing.T) {
	lookup := &PersistedIndexLookup{Catalog: IndexCatalog{EdgeShards: []EdgeShard{
		{RelationType: "runs_on", Shard: "2a", ContentHash: "first"},
		{RelationType: "depends_on", Shard: "2a"},
		{RelationType: "runs_on", Shard: "2a", ContentHash: "duplicate"},
		{RelationType: "ignored", Shard: "2b"},
	}}}

	relations := lookup.relationTypesForShard("2a", nil)
	if len(relations) != 2 || relations[0] != "depends_on" || relations[1] != "runs_on" {
		t.Fatalf("relations = %#v", relations)
	}
	filtered := lookup.relationTypesForShard("2a", map[string]struct{}{"runs_on": {}})
	if len(filtered) != 1 || filtered[0] != "runs_on" {
		t.Fatalf("filtered relations = %#v", filtered)
	}
	spec, ok := lookup.catalogEdgeShardSpec("runs_on", "2a")
	if !ok || spec.ContentHash != "first" {
		t.Fatalf("edge spec = %#v, ok=%v", spec, ok)
	}
}

func TestTrimEntityFieldsKeepsProjectedValuesIsolated(t *testing.T) {
	entity := graph.Entity{
		Fields: graph.Fields{
			"keep": map[string]any{"nested": []any{"original"}},
			"drop": "unused",
		},
		FieldWriteModes: map[string]string{"keep": "replace", "drop": "merge"},
		FieldSources: map[string]graph.FieldSource{
			"keep": {Source: "collector-a"},
			"drop": {Source: "collector-b"},
		},
	}

	projected := trimEntityFields(entity, []string{"keep"})
	if len(projected.Fields) != 1 || len(projected.FieldSources) != 1 {
		t.Fatalf("projected entity = %#v", projected)
	}
	projected.Fields["keep"].(map[string]any)["nested"].([]any)[0] = "changed"
	projected.FieldWriteModes["keep"] = "changed"

	original := entity.Fields["keep"].(map[string]any)["nested"].([]any)[0]
	if original != "original" || entity.FieldWriteModes["keep"] != "replace" {
		t.Fatalf("projection aliases source entity: %#v", entity)
	}
}
