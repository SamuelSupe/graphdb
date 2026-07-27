package storage

import (
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type indexCatalogCoverage struct {
	fields map[string]string
	edges  map[string]string
	pages  map[string]string
}

func checkIndexCatalogCoverage(
	g *graph.Graph,
	catalog IndexCatalog,
	definitions []IndexDefinition,
	health *IndexHealth,
) error {
	expected, err := expectedIndexCatalogCoverage(g, definitions)
	if err != nil {
		return err
	}
	checkFieldIndexCoverage(expected.fields, catalog.Indexes, health)
	checkEdgeShardCoverage(expected.edges, catalog.EdgeShards, health)
	checkEntityPageCoverage(expected.pages, catalog.EntityPages, health)
	return nil
}

func expectedIndexCatalogCoverage(
	g *graph.Graph,
	definitions []IndexDefinition,
) (indexCatalogCoverage, error) {
	coverage := indexCatalogCoverage{
		fields: map[string]string{},
		edges:  map[string]string{},
		pages:  map[string]string{},
	}
	for _, ciType := range g.ListCITypes() {
		fields, err := g.EffectiveFields(ciType.Name)
		if err != nil {
			return indexCatalogCoverage{}, err
		}
		for field, spec := range fields {
			if spec.Indexed || spec.Unique {
				key := fieldIndexHealthKey(ciType.Name, field)
				coverage.fields[key] = ciType.Name + "." + field
			}
		}
	}
	for _, definition := range definitions {
		key := fieldIndexHealthKey(definition.Kind, definition.Field)
		coverage.fields[key] = definition.Kind + "." + definition.Field
	}
	for _, edge := range g.Edges {
		shard := edgeShardID(edge.From)
		key := edgeShardCatalogKey(edge.Type, shard)
		coverage.edges[key] = edge.Type + "/" + shard
	}
	for _, entity := range g.Entities {
		shard := entityShardID(entity.ID)
		coverage.pages[shard] = shard
	}
	return coverage, nil
}

func checkFieldIndexCoverage(
	expected map[string]string,
	actual []IndexSpec,
	health *IndexHealth,
) {
	seen := make(map[string]struct{}, len(actual))
	for _, spec := range actual {
		key := fieldIndexHealthKey(spec.Kind, spec.Field)
		label := "field index " + spec.Kind + "." + spec.Field
		if _, exists := expected[key]; !exists {
			health.Issues = append(health.Issues, label+" is not expected")
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			health.Issues = append(health.Issues, label+" is duplicated")
			continue
		}
		seen[key] = struct{}{}
		if spec.Status != "ready" {
			health.Issues = append(health.Issues, label+" is not ready")
		}
	}
	appendMissingCoverage(
		expected,
		seen,
		"field index ",
		" is missing from catalog",
		health,
	)
}

func checkEdgeShardCoverage(
	expected map[string]string,
	actual []EdgeShard,
	health *IndexHealth,
) {
	seen := make(map[string]struct{}, len(actual))
	for _, spec := range actual {
		key := edgeShardCatalogKey(spec.RelationType, spec.Shard)
		label := "edge shard " + spec.RelationType + "/" + spec.Shard
		if _, exists := expected[key]; !exists {
			health.Issues = append(health.Issues, label+" is not expected")
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			health.Issues = append(health.Issues, label+" is duplicated")
			continue
		}
		seen[key] = struct{}{}
	}
	appendMissingCoverage(
		expected,
		seen,
		"edge shard ",
		" is missing from catalog",
		health,
	)
}

func checkEntityPageCoverage(
	expected map[string]string,
	actual []EntityPageSpec,
	health *IndexHealth,
) {
	seen := make(map[string]struct{}, len(actual))
	for _, spec := range actual {
		label := "entity page " + spec.Shard
		if _, exists := expected[spec.Shard]; !exists {
			health.Issues = append(health.Issues, label+" is not expected")
			continue
		}
		if _, duplicate := seen[spec.Shard]; duplicate {
			health.Issues = append(health.Issues, label+" is duplicated")
			continue
		}
		seen[spec.Shard] = struct{}{}
	}
	appendMissingCoverage(
		expected,
		seen,
		"entity page ",
		" is missing from catalog",
		health,
	)
}

func appendMissingCoverage(
	expected map[string]string,
	seen map[string]struct{},
	prefix string,
	suffix string,
	health *IndexHealth,
) {
	missing := make([]string, 0)
	for key, label := range expected {
		if _, exists := seen[key]; !exists {
			missing = append(missing, prefix+label+suffix)
		}
	}
	sort.Strings(missing)
	health.Issues = append(health.Issues, missing...)
}

func fieldIndexHealthKey(kind string, field string) string {
	return kind + "\x00" + field
}

func edgeShardCatalogKey(relationType string, shard string) string {
	return relationType + "\x00" + shard
}
