package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const relationSchemaLayoutVersion = 1

type RelationSchema struct {
	RelationType string                     `json:"relation_type"`
	Description  string                     `json:"description,omitempty"`
	Fields       map[string]graph.FieldSpec `json:"fields,omitempty"`
	Strict       bool                       `json:"strict,omitempty"`
}

type RelationSchemaCatalog struct {
	LayoutVersion   int              `json:"layout_version"`
	TenantID        string           `json:"tenant_id"`
	Revision        int64            `json:"revision"`
	GraphVersion    int64            `json:"graph_version"`
	UpdatedAt       time.Time        `json:"updated_at,omitempty"`
	RelationSchemas []RelationSchema `json:"relation_schemas"`
}

func (catalog RelationSchemaCatalog) Schema(relationType string) (RelationSchema, bool) {
	for _, schema := range catalog.RelationSchemas {
		if schema.RelationType == relationType {
			return schema, true
		}
	}
	return RelationSchema{}, false
}

func (s *TenantStore) GetRelationSchemas(ctx context.Context, tenantID string) (RelationSchemaCatalog, error) {
	catalog, _, err := s.getRelationSchemaCatalogWithMeta(ctx, tenantID)
	return catalog, err
}

func (s *TenantStore) PutRelationSchema(ctx context.Context, tenantID string, schema RelationSchema) (RelationSchemaCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RelationSchemaCatalog{}, err
	}
	normalized, err := normalizeRelationSchema(schema)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	if s.coordinated() {
		return s.putCoordinatedRelationSchema(ctx, tenantID, normalized)
	}
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	defer unlock()
	ctx, err = s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return RelationSchemaCatalog{}, err
	}
	loaded, err := s.loadForWriteLocked(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	if _, exists := loaded.Graph.RelationTypes[normalized.RelationType]; !exists {
		return RelationSchemaCatalog{}, fmt.Errorf("relation schema references missing relation type %q", normalized.RelationType)
	}
	catalog, meta, err := s.getRelationSchemaCatalogWithMeta(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	catalog = upsertRelationSchema(catalog, normalized)
	prepareRelationSchemaCatalog(&catalog, tenantID, loaded.Manifest.Version)
	if err := validateRelationSchemaGraph(loaded.Graph, catalog); err != nil {
		return RelationSchemaCatalog{}, err
	}
	if err := s.putRelationSchemaCatalog(ctx, tenantID, catalog, meta); err != nil {
		if errors.Is(err, ErrConflict) {
			return RelationSchemaCatalog{}, fmt.Errorf("%w: relation schemas for tenant %q changed while publishing", ErrConflict, tenantID)
		}
		return RelationSchemaCatalog{}, err
	}
	return catalog, nil
}

func (s *TenantStore) DeleteRelationSchema(ctx context.Context, tenantID string, relationType string) (RelationSchemaCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RelationSchemaCatalog{}, err
	}
	relationType = strings.TrimSpace(relationType)
	if relationType == "" {
		return RelationSchemaCatalog{}, fmt.Errorf("relation type is required")
	}
	if s.coordinated() {
		return s.deleteCoordinatedRelationSchema(ctx, tenantID, relationType)
	}
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	defer unlock()
	ctx, err = s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return RelationSchemaCatalog{}, err
	}
	loaded, err := s.loadForWriteLocked(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	catalog, meta, err := s.getRelationSchemaCatalogWithMeta(ctx, tenantID)
	if err != nil {
		return RelationSchemaCatalog{}, err
	}
	next := make([]RelationSchema, 0, len(catalog.RelationSchemas))
	found := false
	for _, schema := range catalog.RelationSchemas {
		if schema.RelationType == relationType {
			found = true
			continue
		}
		next = append(next, schema)
	}
	if !found {
		return catalog, nil
	}
	catalog.RelationSchemas = next
	prepareRelationSchemaCatalog(&catalog, tenantID, loaded.Manifest.Version)
	if err := s.putRelationSchemaCatalog(ctx, tenantID, catalog, meta); err != nil {
		return RelationSchemaCatalog{}, err
	}
	return catalog, nil
}

func (s *TenantStore) getRelationSchemaCatalogWithMeta(ctx context.Context, tenantID string) (RelationSchemaCatalog, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RelationSchemaCatalog{}, ObjectMeta{}, err
	}
	if s.coordinated() {
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return RelationSchemaCatalog{}, ObjectMeta{}, err
		}
		return snapshot.RelationSchemas,
			coordinatedManifestMeta(s.relationSchemaCatalogKey(tenantID), head), nil
	}
	key := s.relationSchemaCatalogKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return emptyRelationSchemaCatalog(tenantID), ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return RelationSchemaCatalog{}, ObjectMeta{}, err
	}
	var catalog RelationSchemaCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return RelationSchemaCatalog{}, ObjectMeta{}, fmt.Errorf("decode relation schema catalog: %w", err)
	}
	if catalog.LayoutVersion != relationSchemaLayoutVersion {
		return RelationSchemaCatalog{}, ObjectMeta{}, fmt.Errorf("unsupported relation schema layout version %d", catalog.LayoutVersion)
	}
	if catalog.TenantID != tenantID {
		return RelationSchemaCatalog{}, ObjectMeta{}, fmt.Errorf("relation schema tenant mismatch: path tenant %q contains tenant %q", tenantID, catalog.TenantID)
	}
	normalized, err := normalizeRelationSchemaCatalog(catalog)
	if err != nil {
		return RelationSchemaCatalog{}, ObjectMeta{}, err
	}
	return normalized, meta, nil
}

func (s *TenantStore) putRelationSchemaCatalog(ctx context.Context, tenantID string, catalog RelationSchemaCatalog, meta ObjectMeta) error {
	if s.coordinated() {
		return s.putCoordinatedRelationSchemaCatalog(ctx, tenantID, catalog)
	}
	data, err := json.Marshal(catalog)
	if err != nil {
		return err
	}
	_, err = s.putTenantBytesWithMetaResult(ctx, tenantID, s.relationSchemaCatalogKey(tenantID), data, meta)
	return err
}

func (s *TenantStore) putCoordinatedRelationSchemaCatalog(
	ctx context.Context,
	tenantID string,
	catalog RelationSchemaCatalog,
) error {
	if _, err := s.ensureCoordinatedTenantHead(ctx, tenantID); err != nil {
		return err
	}
	for attempt := 0; attempt < s.CoordinatorRetryLimit+1; attempt++ {
		loaded, err := s.loadForWriteLocked(ctx, tenantID)
		if err != nil {
			return err
		}
		token, err := parseCoordinatedHeadToken(loaded.Meta)
		if err != nil {
			return err
		}
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return err
		}
		if !sameCoordinationPoint(head, token) {
			if err := coordinatorRetryDelay(ctx, attempt); err != nil {
				return err
			}
			continue
		}
		catalog.LayoutVersion = relationSchemaLayoutVersion
		catalog.TenantID = tenantID
		catalog.GraphVersion = loaded.Manifest.Version
		catalog.UpdatedAt = time.Now().UTC()
		normalized, err := normalizeRelationSchemaCatalog(catalog)
		if err != nil {
			return err
		}
		if err := validateRelationSchemaGraph(loaded.Graph, normalized); err != nil {
			return err
		}
		snapshot.RelationSchemas = normalized
		_, published, err := s.publishCoordinatedWriteContext(ctx, head, snapshot)
		if err != nil {
			return err
		}
		if published {
			return s.mirrorLatestWriteContext(ctx, tenantID)
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: relation schemas for tenant %q changed while publishing", ErrWriteConflict, tenantID)
}

func emptyRelationSchemaCatalog(tenantID string) RelationSchemaCatalog {
	return RelationSchemaCatalog{LayoutVersion: relationSchemaLayoutVersion, TenantID: tenantID, RelationSchemas: []RelationSchema{}}
}

func prepareRelationSchemaCatalog(catalog *RelationSchemaCatalog, tenantID string, graphVersion int64) {
	catalog.LayoutVersion = relationSchemaLayoutVersion
	catalog.TenantID = tenantID
	catalog.Revision++
	catalog.GraphVersion = graphVersion
	catalog.UpdatedAt = time.Now().UTC()
	sort.Slice(catalog.RelationSchemas, func(i, j int) bool {
		return catalog.RelationSchemas[i].RelationType < catalog.RelationSchemas[j].RelationType
	})
}

func upsertRelationSchema(catalog RelationSchemaCatalog, schema RelationSchema) RelationSchemaCatalog {
	for i := range catalog.RelationSchemas {
		if catalog.RelationSchemas[i].RelationType == schema.RelationType {
			catalog.RelationSchemas[i] = schema
			return catalog
		}
	}
	catalog.RelationSchemas = append(catalog.RelationSchemas, schema)
	return catalog
}

func normalizeRelationSchemaCatalog(catalog RelationSchemaCatalog) (RelationSchemaCatalog, error) {
	seen := map[string]struct{}{}
	for i, schema := range catalog.RelationSchemas {
		normalized, err := normalizeRelationSchema(schema)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		if _, exists := seen[normalized.RelationType]; exists {
			return RelationSchemaCatalog{}, fmt.Errorf("duplicate relation schema %q", normalized.RelationType)
		}
		seen[normalized.RelationType] = struct{}{}
		catalog.RelationSchemas[i] = normalized
	}
	sort.Slice(catalog.RelationSchemas, func(i, j int) bool {
		return catalog.RelationSchemas[i].RelationType < catalog.RelationSchemas[j].RelationType
	})
	return catalog, nil
}

func normalizeRelationSchema(schema RelationSchema) (RelationSchema, error) {
	schema.RelationType = strings.TrimSpace(schema.RelationType)
	if schema.RelationType == "" {
		return RelationSchema{}, fmt.Errorf("relation_type is required")
	}
	fields, err := graph.NormalizePropertyFieldSpecs(schema.RelationType, schema.Fields)
	if err != nil {
		return RelationSchema{}, err
	}
	for name, spec := range fields {
		if spec.MergeStrategy != "" || spec.Indexed || spec.Unique {
			return RelationSchema{}, fmt.Errorf("relation schema %q field %q only supports type, required, enum and default", schema.RelationType, name)
		}
	}
	schema.Description = strings.TrimSpace(schema.Description)
	schema.Fields = fields
	return schema, nil
}

func (s *TenantStore) putCoordinatedRelationSchema(
	ctx context.Context,
	tenantID string,
	schema RelationSchema,
) (RelationSchemaCatalog, error) {
	if _, err := s.ensureCoordinatedTenantHead(ctx, tenantID); err != nil {
		return RelationSchemaCatalog{}, err
	}
	for attempt := 0; attempt < s.CoordinatorRetryLimit+1; attempt++ {
		loaded, err := s.loadForWriteLocked(ctx, tenantID)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		token, err := parseCoordinatedHeadToken(loaded.Meta)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		if !sameCoordinationPoint(head, token) {
			if err := coordinatorRetryDelay(ctx, attempt); err != nil {
				return RelationSchemaCatalog{}, err
			}
			continue
		}
		if _, exists := loaded.Graph.RelationTypes[schema.RelationType]; !exists {
			return RelationSchemaCatalog{}, fmt.Errorf("relation schema references missing relation type %q", schema.RelationType)
		}
		catalog := upsertRelationSchema(snapshot.RelationSchemas, schema)
		prepareRelationSchemaCatalog(&catalog, tenantID, loaded.Manifest.Version)
		if err := validateRelationSchemaGraph(loaded.Graph, catalog); err != nil {
			return RelationSchemaCatalog{}, err
		}
		snapshot.RelationSchemas = catalog
		_, published, err := s.publishCoordinatedWriteContext(ctx, head, snapshot)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		if published {
			if err := s.mirrorLatestWriteContext(ctx, tenantID); err != nil {
				return RelationSchemaCatalog{}, err
			}
			return catalog, nil
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return RelationSchemaCatalog{}, err
		}
	}
	return RelationSchemaCatalog{}, fmt.Errorf("%w: relation schemas for tenant %q changed while publishing", ErrWriteConflict, tenantID)
}

func (s *TenantStore) deleteCoordinatedRelationSchema(
	ctx context.Context,
	tenantID string,
	relationType string,
) (RelationSchemaCatalog, error) {
	if _, err := s.ensureCoordinatedTenantHead(ctx, tenantID); err != nil {
		return RelationSchemaCatalog{}, err
	}
	for attempt := 0; attempt < s.CoordinatorRetryLimit+1; attempt++ {
		loaded, err := s.loadForWriteLocked(ctx, tenantID)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		token, err := parseCoordinatedHeadToken(loaded.Meta)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		snapshot, head, err := s.loadCoordinatedWriteContext(ctx, tenantID)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		if !sameCoordinationPoint(head, token) {
			if err := coordinatorRetryDelay(ctx, attempt); err != nil {
				return RelationSchemaCatalog{}, err
			}
			continue
		}
		catalog := snapshot.RelationSchemas
		next := make([]RelationSchema, 0, len(catalog.RelationSchemas))
		found := false
		for _, item := range catalog.RelationSchemas {
			if item.RelationType == relationType {
				found = true
				continue
			}
			next = append(next, item)
		}
		if !found {
			return catalog, nil
		}
		catalog.RelationSchemas = next
		prepareRelationSchemaCatalog(&catalog, tenantID, loaded.Manifest.Version)
		snapshot.RelationSchemas = catalog
		_, published, err := s.publishCoordinatedWriteContext(ctx, head, snapshot)
		if err != nil {
			return RelationSchemaCatalog{}, err
		}
		if published {
			if err := s.mirrorLatestWriteContext(ctx, tenantID); err != nil {
				return RelationSchemaCatalog{}, err
			}
			return catalog, nil
		}
		if err := coordinatorRetryDelay(ctx, attempt); err != nil {
			return RelationSchemaCatalog{}, err
		}
	}
	return RelationSchemaCatalog{}, fmt.Errorf("%w: relation schemas for tenant %q changed while publishing", ErrWriteConflict, tenantID)
}
