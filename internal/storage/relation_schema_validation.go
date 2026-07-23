package storage

import (
	"context"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) prepareRelationSchemaMutations(ctx context.Context, tenantID string, mutations graph.Mutations) (graph.Mutations, RelationSchemaCatalog, ObjectMeta, error) {
	catalog, meta, err := s.getRelationSchemaCatalogWithMeta(ctx, tenantID)
	if err != nil || len(catalog.RelationSchemas) == 0 {
		return mutations, catalog, meta, err
	}
	mutations.UpsertEdges = append([]graph.Edge(nil), mutations.UpsertEdges...)
	for i, edge := range mutations.UpsertEdges {
		schema, ok := catalog.Schema(edge.Type)
		if !ok {
			continue
		}
		edge = graph.CopyEdge(edge)
		edge.Fields = graph.ApplyPropertyDefaults(edge.Fields, schema.Fields)
		mutations.UpsertEdges[i] = edge
	}
	return mutations, catalog, meta, nil
}

func (s *TenantStore) advanceRelationSchemaValidation(ctx context.Context, tenantID string, catalog RelationSchemaCatalog, meta ObjectMeta, graphVersion int64) error {
	if s.coordinated() {
		// The immutable write-context records the graph version at which the
		// schema was fully validated. Later commits can safely revalidate the
		// full graph without creating a context revision for every graph write.
		return nil
	}
	if len(catalog.RelationSchemas) == 0 || catalog.GraphVersion == graphVersion {
		return nil
	}
	catalog.GraphVersion = graphVersion
	catalog.UpdatedAt = time.Now().UTC()
	return s.putRelationSchemaCatalog(ctx, tenantID, catalog, meta)
}

func validateRelationSchemaGraph(g *graph.Graph, catalog RelationSchemaCatalog) error {
	if g == nil || len(catalog.RelationSchemas) == 0 {
		return nil
	}
	if err := validateRelationSchemaReferences(g, catalog); err != nil {
		return err
	}
	for _, edge := range g.Edges {
		if err := validateRelationSchemaEdge(edge, catalog); err != nil {
			return err
		}
	}
	return nil
}

func validateRelationSchemaCommit(g *graph.Graph, catalog RelationSchemaCatalog, affectedEdgeIDs []string) error {
	if g == nil || len(catalog.RelationSchemas) == 0 {
		return nil
	}
	if err := validateRelationSchemaReferences(g, catalog); err != nil {
		return err
	}
	for _, edgeID := range affectedEdgeIDs {
		edge, ok := g.Edges[edgeID]
		if !ok {
			continue
		}
		if err := validateRelationSchemaEdge(edge, catalog); err != nil {
			return err
		}
	}
	return nil
}

func validateRelationSchemaReferences(g *graph.Graph, catalog RelationSchemaCatalog) error {
	for _, schema := range catalog.RelationSchemas {
		if _, ok := g.RelationTypes[schema.RelationType]; !ok {
			return fmt.Errorf("relation schema references missing relation type %q", schema.RelationType)
		}
	}
	return nil
}

func validateRelationSchemaEdge(edge graph.Edge, catalog RelationSchemaCatalog) error {
	schema, ok := catalog.Schema(edge.Type)
	if !ok {
		return nil
	}
	resource := fmt.Sprintf("edge %q relation %q", edge.ID, edge.Type)
	return graph.ValidatePropertyFields(resource, edge.Fields, schema.Fields, schema.Strict)
}
