package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
)

type CommitRequest struct {
	Mutations       graph.Mutations `json:"mutations"`
	ExpectedVersion *int64          `json:"expected_version,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
}

type Plan struct {
	IngestResult  *storage.IngestResult
	Changes       ChangeSet
	CITypes       []graph.CIType
	RelationTypes []graph.RelationType
	Report        graph.ApplyReport
	EntityCount   int64
	EdgeCount     int64
}

// MutationBudgetError reports that planning exceeded the transaction's resource limit.
type MutationBudgetError struct{ Reason string }

func (e *MutationBudgetError) Error() string { return e.Reason }

func mutationBudgetExceeded(err error) bool {
	var budget *MutationBudgetError
	return errors.Is(err, ErrReadBudget) || errors.As(err, &budget)
}

type edgeLocation struct {
	ID        string `json:"id"`
	Partition uint8  `json:"partition"`
}

type fragmentLoader struct {
	view           View
	ctx            context.Context
	entities       map[string]graph.Entity
	edges          map[string]graph.Edge
	entityLookups  map[string][]string
	primaryMissing map[string]bool
	edgeLookups    map[string]bool
	edgeLocations  map[string]edgeLocation
	directories    *directoryReads
	bytes          int
}

func (l *fragmentLoader) charge(value any) error {
	l.bytes += estimateFragmentBytes(value)
	if l.bytes > 64<<20 || len(l.entities) > 8192 || len(l.edges) > 8192 {
		return &MutationBudgetError{Reason: "mutation dependency set exceeds the transaction memory budget"}
	}
	return nil
}

func estimateFragmentBytes(value any) int {
	switch v := value.(type) {
	case nil:
		return 8
	case graph.Entity:
		size := 128 + len(v.ID) + len(v.Kind) + len(v.Source) + len(v.ExternalID) + len(v.SplitFrom)
		for _, id := range v.MergedFrom {
			size += len(id) + 16
		}
		for k, val := range v.Fields {
			size += len(k) + estimateAnyBytes(val) + 16
		}
		for k, val := range v.Identity {
			size += len(k) + estimateAnyBytes(val) + 16
		}
		for k, mode := range v.FieldWriteModes {
			size += len(k) + len(mode) + 32
		}
		for k, fs := range v.FieldSources {
			size += len(k) + len(fs.Source) + 64
		}
		for _, src := range v.Sources {
			size += len(src.Source) + len(src.ExternalID) + 64
		}
		return size
	case graph.Edge:
		size := 128 + len(v.ID) + len(v.Type) + len(v.From) + len(v.To) + len(v.Source) + len(v.ExternalID)
		for k, val := range v.Fields {
			size += len(k) + estimateAnyBytes(val) + 16
		}
		for k, fs := range v.FieldSources {
			size += len(k) + len(fs.Source) + 64
		}
		for _, src := range v.Sources {
			size += len(src.Source) + len(src.ExternalID) + 64
		}
		return size
	default:
		return estimateAnyBytes(v)
	}
}

func estimateAnyBytes(value any) int {
	switch v := value.(type) {
	case nil:
		return 4
	case string:
		return len(v) + 16
	case []byte:
		return len(v) + 24
	case json.RawMessage:
		return len(v) + 24
	case bool, int, int64, int32, float64, float32, uint, uint64, uint32:
		return 16
	case []string:
		n := 24
		for _, s := range v {
			n += len(s) + 16
		}
		return n
	case []any:
		n := 24
		for _, item := range v {
			n += estimateAnyBytes(item)
		}
		return n
	case map[string]string:
		n := 48
		for k, item := range v {
			n += len(k) + len(item) + 32
		}
		return n
	case map[string]any:
		n := 48
		for k, item := range v {
			n += len(k) + estimateAnyBytes(item) + 16
		}
		return n
	default:
		return 64
	}
}

func (l *fragmentLoader) entity(id string) ([]string, error) {
	if id == "" {
		return nil, nil
	}
	if owners, seen := l.entityLookups[id]; seen {
		return owners, nil
	}
	l.entityLookups[id] = nil
	var entity graph.Entity
	if !l.primaryMissing[id] {
		found, err := l.view.Get(l.ctx, Bucket(id), EntityKey(id), &entity)
		if err != nil {
			return nil, err
		}
		if found {
			if entity.ID != id {
				return nil, fmt.Errorf("entity fragment identity mismatch")
			}
			l.entities[id] = entity
			l.entityLookups[id] = []string{id}
			return []string{id}, l.charge(entity)
		}
	}
	owners, err := l.directories.get(EntityAliasKey(id))
	if err != nil {
		return nil, err
	}
	for _, owner := range owners {
		if owner == id {
			continue
		}
		if _, err := l.entity(owner); err != nil {
			return nil, err
		}
	}
	l.entityLookups[id] = owners
	return owners, nil
}

func (l *fragmentLoader) prefetchEntities(entities []graph.Entity) error {
	ids := make([]string, 0, len(entities))
	for _, entity := range entities {
		ids = append(ids, strings.TrimSpace(entity.ID))
	}
	return l.prefetchEntityIDs(ids)
}

func (l *fragmentLoader) prefetchEntityIDs(ids []string) error {
	if len(ids) < 2 {
		return nil
	}
	if _, ok := l.view.Remote.(batchReader); !ok {
		return nil
	}
	if l.primaryMissing == nil {
		l.primaryMissing = make(map[string]bool)
	}
	lookup := Lookup{View: l.view}
	for start := 0; start < len(ids); start += 128 {
		var missing []string
		seen := make(map[string]bool)
		for _, id := range ids[start:min(len(ids), start+128)] {
			if id != "" && !seen[id] && !l.primaryMissing[id] && len(l.entityLookups[id]) == 0 {
				missing = append(missing, id)
				seen[id] = true
			}
		}
		found, _, err := lookup.GetEntities(l.ctx, missing, nil)
		if err != nil {
			return err
		}
		for _, id := range missing {
			if entity, ok := found[id]; ok {
				l.entities[id] = entity
				l.entityLookups[id] = []string{id}
				if err := l.charge(entity); err != nil {
					return err
				}
			} else {
				l.primaryMissing[id] = true
			}
		}
	}
	return nil
}

func (l *fragmentLoader) edge(id string) error {
	if id == "" || l.edgeLookups[id] {
		return nil
	}
	l.edgeLookups[id] = true
	key := EdgeLocatorKey(id)
	loc, cached := l.edgeLocations[id]
	found := loc.ID != ""
	if cached {
		delete(l.edgeLocations, id)
	} else {
		var err error
		found, err = l.view.Get(l.ctx, Bucket(key), key, &loc)
		if err != nil {
			return err
		}
	}
	if !found {
		owners, err := l.directories.get(EdgeAliasKey(id))
		if err != nil {
			return err
		}
		for _, owner := range owners {
			if err := l.edge(owner); err != nil {
				return err
			}
		}
		return nil
	}
	edge, alreadyLoaded := l.edges[loc.ID]
	if !alreadyLoaded {
		var err error
		found, err = l.view.Get(l.ctx, loc.Partition, EdgeKey(loc.ID), &edge)
		if err != nil {
			return err
		}
		if !found || edge.ID != loc.ID {
			return fmt.Errorf("edge locator has no matching primary record")
		}
		l.edges[edge.ID] = edge
		if err := l.charge(edge); err != nil {
			return err
		}
	}
	if _, err := l.entity(edge.From); err != nil {
		return err
	}
	_, err := l.entity(edge.To)
	return err
}

func (l *fragmentLoader) incident(id string) error {
	for _, direction := range []string{"out", "in"} {
		prefix := Key(direction, id)
		start := ""
		for {
			batch, err := l.view.Read(l.ctx, Bucket(id), ReadRequest{Prefix: prefix, Start: start, Limit: 128})
			if err != nil {
				return err
			}
			keys := make(map[uint8][]string)
			var locations []edgeLocation
			for _, row := range batch.Rows {
				var loc edgeLocation
				if err := decodeRecord(row, &loc); err != nil {
					return err
				}
				if !l.edgeLookups[loc.ID] {
					l.edgeLookups[loc.ID] = true
					keys[loc.Partition] = append(keys[loc.Partition], EdgeKey(loc.ID))
					locations = append(locations, loc)
				}
			}
			edges, err := l.view.fetchEdges(l.ctx, keys)
			if err != nil {
				return err
			}
			endpoints := make([]string, 0, len(edges)*2)
			for _, loc := range locations {
				edge, ok := edges[loc.ID]
				if !ok || Bucket(edge.From) != loc.Partition {
					return fmt.Errorf("%w: adjacency references a missing edge", ErrPartitionCorrupt)
				}
				l.edges[loc.ID] = edge
				if err := l.charge(edge); err != nil {
					return err
				}
				endpoints = append(endpoints, edge.From, edge.To)
			}
			if err := l.prefetchEntityIDs(endpoints); err != nil {
				return err
			}
			for _, id := range endpoints {
				if _, err := l.entity(id); err != nil {
					return err
				}
			}
			if batch.Next == "" {
				break
			}
			if batch.Next <= start {
				return fmt.Errorf("%w: incident edge scan made no progress", ErrPartitionCorrupt)
			}
			start = batch.Next
		}
	}
	return nil
}

func (l *fragmentLoader) constraints(g *graph.Graph, entities []graph.Entity, incoming bool) error {
	var ids, constraints, aliases []string
	for _, entity := range entities {
		normalized, refs, err := g.PartitionEntityReferences(entity, incoming)
		if err != nil {
			return invalid(err.Error())
		}
		ids = append(ids, normalized.ID)
		if l.primaryMissing[normalized.ID] {
			aliases = append(aliases, EntityAliasKey(normalized.ID))
		}
		for _, ref := range refs {
			constraints = append(constraints, ConstraintKey(ref))
		}
	}
	if err := l.directories.prefetch(append(aliases, constraints...)); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := l.entity(id); err != nil {
			return err
		}
	}
	var ownerIDs []string
	for _, key := range constraints {
		owners, err := l.directories.get(key)
		if err != nil {
			return err
		}
		ownerIDs = append(ownerIDs, owners...)
	}
	if len(ownerIDs) > 0 {
		if err := l.prefetchEntityIDs(ownerIDs); err != nil {
			return err
		}
		for _, id := range ownerIDs {
			if _, err := l.entity(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func cardinalityKeys(edge graph.Edge, types map[string]graph.RelationType) []string {
	var keys []string
	switch types[edge.Type].Cardinality {
	case graph.OneToOne:
		keys = append(keys, Key("cardinality", edge.Type, "out", edge.From), Key("cardinality", edge.Type, "in", edge.To))
	case graph.ManyToOne:
		keys = append(keys, Key("cardinality", edge.Type, "out", edge.From))
	case graph.OneToMany:
		keys = append(keys, Key("cardinality", edge.Type, "in", edge.To))
	}
	return keys
}

func (l *fragmentLoader) edgeDependencies(edge graph.Edge, types map[string]graph.RelationType) error {
	edge.Type = strings.TrimSpace(edge.Type)
	edge.From = strings.TrimSpace(edge.From)
	edge.To = strings.TrimSpace(edge.To)
	if err := l.edge(graph.CanonicalEdgeID(edge)); err != nil {
		return err
	}
	if edge.ID != "" {
		if err := l.edge(edge.ID); err != nil {
			return err
		}
	}
	for _, key := range cardinalityKeys(edge, types) {
		owners, err := l.directories.get(key)
		if err != nil {
			return err
		}
		for _, owner := range owners {
			if err := l.edge(owner); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *fragmentLoader) snapshot() graph.Snapshot {
	snapshot := graph.Snapshot{Version: l.view.Graph.Version, CITypes: l.view.Graph.CITypes, RelationTypes: l.view.Graph.RelationTypes}
	for _, entity := range l.entities {
		snapshot.Entities = append(snapshot.Entities, entity)
	}
	sort.Slice(snapshot.Entities, func(i, j int) bool { return snapshot.Entities[i].ID < snapshot.Entities[j].ID })
	for _, edge := range l.edges {
		snapshot.Edges = append(snapshot.Edges, edge)
	}
	sort.Slice(snapshot.Edges, func(i, j int) bool { return snapshot.Edges[i].ID < snapshot.Edges[j].ID })
	return snapshot
}

// BuildPlan loads the dependency closure of a bounded mutation batch. Global
// identity/unique/cardinality directories bring conflicting owners into the
// same validation graph; unrelated graph partitions stay on disk.
func BuildPlan(ctx context.Context, view View, request CommitRequest, at time.Time) (Plan, error) {
	return buildPlan(ctx, view, request, at, nil, false)
}

func buildPlan(ctx context.Context, view View, request CommitRequest, at time.Time, conditions []storage.IngestPrecondition, atomic bool) (Plan, error) {
	if view.Budget == nil {
		view.Budget = &ReadBudget{Requests: 8192, Bytes: 64 << 20}
	}
	m := request.Mutations
	if len(m.MarkSourceStale) > 0 {
		return Plan{}, invalid("source-wide reconciliation requires a partitioned maintenance task")
	}
	if view.Graph.EntityCount > 0 && (len(m.UpsertCITypes)+len(m.DeleteCITypes)+len(m.UpsertRelationTypes)+len(m.DeleteRelationTypes) > 0) {
		return Plan{}, invalid("schema changes on populated partitioned graphs require a validation task")
	}
	var err error
	if len(view.Graph.RelationSchemas.RelationSchemas) > 0 {
		m.UpsertEdges = append([]graph.Edge(nil), m.UpsertEdges...)
		for i, edge := range m.UpsertEdges {
			if schema, ok := view.Graph.RelationSchemas.Schema(edge.Type); ok {
				edge = graph.CopyEdge(edge)
				edge.Fields = graph.ApplyPropertyDefaults(edge.Fields, schema.Fields)
				m.UpsertEdges[i] = edge
			}
		}
	}
	var policyReport graph.ApplyReport
	m, policyReport, err = graph.ApplySourcePolicy(m, view.Graph.SourcePolicy)
	if err != nil {
		return Plan{}, invalid(err.Error())
	}
	schema, err := graph.FromSnapshot(graph.Snapshot{Version: view.Graph.Version, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes})
	if err != nil {
		return Plan{}, err
	}
	preview, _, err := schema.ApplyCommitCopyWithOptions(graph.Commit{IdentityEncoding: graph.IdentityEncodingTuple, ID: "schema-preview", Version: schema.Version + 1, CreatedAt: at, Mutations: graph.Mutations{UpsertCITypes: m.UpsertCITypes, DeleteCITypes: m.DeleteCITypes, UpsertRelationTypes: m.UpsertRelationTypes, DeleteRelationTypes: m.DeleteRelationTypes}}, graph.ApplyOptions{})
	if err != nil {
		return Plan{}, invalid(err.Error())
	}
	directories := &directoryReads{ctx: ctx, view: view, owners: map[string][]string{}}
	l := &fragmentLoader{view: view, ctx: ctx, entities: map[string]graph.Entity{}, edges: map[string]graph.Edge{}, entityLookups: map[string][]string{}, edgeLookups: map[string]bool{}, directories: directories}
	prefetchIDs := make([]string, 0, len(m.UpsertEntities)+len(m.DeleteEntities)+len(m.UpsertEdges)*2)
	for _, entity := range m.UpsertEntities {
		prefetchIDs = append(prefetchIDs, strings.TrimSpace(entity.ID))
	}
	for _, id := range m.DeleteEntities {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			prefetchIDs = append(prefetchIDs, trimmed)
		}
	}
	for _, r := range m.DeleteEntityRequests {
		if trimmed := strings.TrimSpace(r.ID); trimmed != "" {
			prefetchIDs = append(prefetchIDs, trimmed)
		}
	}
	for _, r := range m.MergeEntities {
		if trimmed := strings.TrimSpace(r.TargetID); trimmed != "" {
			prefetchIDs = append(prefetchIDs, trimmed)
		}
		for _, sourceID := range r.SourceIDs {
			if trimmed := strings.TrimSpace(sourceID); trimmed != "" {
				prefetchIDs = append(prefetchIDs, trimmed)
			}
		}
	}
	for _, r := range m.SplitEntities {
		if trimmed := strings.TrimSpace(r.SourceID); trimmed != "" {
			prefetchIDs = append(prefetchIDs, trimmed)
		}
	}
	for _, edge := range m.UpsertEdges {
		prefetchIDs = append(prefetchIDs, strings.TrimSpace(edge.From), strings.TrimSpace(edge.To))
	}
	for _, condition := range conditions {
		if condition.ResourceType == "entity" {
			if trimmed := strings.TrimSpace(condition.ID); trimmed != "" {
				prefetchIDs = append(prefetchIDs, trimmed)
			}
		}
	}
	if err := l.prefetchEntitiesAndAliases(prefetchIDs); err != nil {
		return Plan{}, err
	}
	for _, condition := range conditions {
		if condition.ResourceType == "entity" {
			if _, err := l.entity(condition.ID); err != nil {
				return Plan{}, err
			}
		} else if err := l.edge(condition.ID); err != nil {
			return Plan{}, err
		}
	}
	if err := l.constraints(preview, m.UpsertEntities, true); err != nil {
		return Plan{}, err
	}
	var incident []string
	for _, id := range m.DeleteEntities {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			incident = append(incident, trimmed)
		}
	}
	var deleteConstraintKeys []string
	for _, r := range m.DeleteEntityRequests {
		if trimmedID := strings.TrimSpace(r.ID); trimmedID != "" {
			incident = append(incident, trimmedID)
		} else {
			ref, err := preview.PartitionEntitySourceReference(r.Kind, r.Source, r.ExternalID)
			if err != nil {
				return Plan{}, invalid(err.Error())
			}
			deleteConstraintKeys = append(deleteConstraintKeys, ConstraintKey(ref))
		}
	}
	if len(deleteConstraintKeys) > 0 {
		if err := directories.prefetch(deleteConstraintKeys); err != nil {
			return Plan{}, err
		}
		for _, key := range deleteConstraintKeys {
			owners, err := directories.get(key)
			if err != nil {
				return Plan{}, err
			}
			incident = append(incident, owners...)
		}
	}
	for _, r := range m.MergeEntities {
		if trimmed := strings.TrimSpace(r.TargetID); trimmed != "" {
			incident = append(incident, trimmed)
		}
		for _, sourceID := range r.SourceIDs {
			if trimmed := strings.TrimSpace(sourceID); trimmed != "" {
				incident = append(incident, trimmed)
			}
		}
	}
	for _, r := range m.SplitEntities {
		if trimmed := strings.TrimSpace(r.SourceID); trimmed != "" {
			incident = append(incident, trimmed)
		}
		if err := l.constraints(preview, r.Entities, true); err != nil {
			return Plan{}, err
		}
	}
	for _, id := range incident {
		owners, err := l.entity(id)
		if err != nil {
			return Plan{}, err
		}
		for _, owner := range owners {
			if err := l.incident(owner); err != nil {
				return Plan{}, err
			}
		}
	}
	deleteEdgeIDs := make([]string, 0, len(m.DeleteEdges)+len(m.DeleteEdgeRequests))
	for _, id := range m.DeleteEdges {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			deleteEdgeIDs = append(deleteEdgeIDs, trimmed)
		}
	}
	for _, r := range m.DeleteEdgeRequests {
		id := strings.TrimSpace(r.ID)
		relType := strings.TrimSpace(r.Type)
		from := strings.TrimSpace(r.From)
		to := strings.TrimSpace(r.To)
		if relType != "" || from != "" || to != "" {
			if relType != "" && from != "" && to != "" {
				deleteEdgeIDs = append(deleteEdgeIDs, graph.CanonicalEdgeIDParts(relType, from, to))
			}
		} else if id != "" {
			deleteEdgeIDs = append(deleteEdgeIDs, id)
		}
	}
	if err := l.prefetchEdgeIDs(deleteEdgeIDs); err != nil {
		return Plan{}, err
	}
	for _, id := range deleteEdgeIDs {
		if err := l.edge(id); err != nil {
			return Plan{}, err
		}
	}
	for start := 0; start < len(m.UpsertEdges); start += 128 {
		chunk := m.UpsertEdges[start:min(len(m.UpsertEdges), start+128)]
		for _, edge := range chunk {
			if _, err := l.entity(strings.TrimSpace(edge.From)); err != nil {
				return Plan{}, err
			}
			if _, err := l.entity(strings.TrimSpace(edge.To)); err != nil {
				return Plan{}, err
			}
		}
		if err := l.edgeDependencyBatch(chunk, preview.RelationTypes); err != nil {
			return Plan{}, err
		}
	}
	var before, after *graph.Graph
	var report graph.ApplyReport
	for round := 0; round < 32; round++ {
		loaded := len(l.entities) + len(l.edges)
		before, err = graph.FromSnapshot(l.snapshot())
		if err != nil {
			return Plan{}, err
		}
		if err := ingest.EvaluateIngestPreconditions(before, conditions, at); err != nil {
			return Plan{}, err
		}
		after, report, err = before.ApplyCommitCopyWithOptions(graph.Commit{IdentityEncoding: graph.IdentityEncodingTuple, ID: "partition-plan", Version: view.Graph.Version + 1, CreatedAt: at, Mutations: m}, graph.ApplyOptions{})
		if err != nil {
			return Plan{}, invalid(err.Error())
		}
		entities := make([]graph.Entity, 0, after.Entities.Len())
		for _, entity := range after.Entities.All() {
			entities = append(entities, entity)
		}
		if err := l.constraints(after, entities, false); err != nil {
			return Plan{}, err
		}
		if err := l.rewrittenEdgeDependencies(after); err != nil {
			return Plan{}, err
		}
		if loaded == len(l.entities)+len(l.edges) {
			break
		}
		if round == 31 {
			return Plan{}, &MutationBudgetError{Reason: "mutation identity dependency closure exceeds its budget"}
		}
	}
	for _, sidecar := range view.Graph.RelationSchemas.RelationSchemas {
		if _, ok := after.RelationTypes[sidecar.RelationType]; !ok {
			return Plan{}, invalid("relation schema references a deleted relation type")
		}
	}
	for _, edge := range after.Edges.All() {
		if err := storage.ValidateRelationSchemaEdge(edge, view.Graph.RelationSchemas); err != nil {
			return Plan{}, invalid(err.Error())
		}
	}
	report.Suppressed = append(policyReport.Suppressed, report.Suppressed...)
	if atomic && len(report.Suppressed) > 0 {
		return Plan{}, fmt.Errorf("%w: %d mutation conflicts", ingest.ErrIngestAtomicSuppressed, len(report.Suppressed))
	}
	if !report.Changed {
		after = before
	}
	plan, err := buildChanges(directories, before, after, report)
	if err != nil {
		return Plan{}, err
	}
	quota := view.Graph.Config.Quota
	if maximum := quota.MaxEntitiesPerTenant; maximum != nil && *maximum > 0 && plan.EntityCount > int64(*maximum) && plan.EntityCount >= view.Graph.EntityCount {
		return Plan{}, fmt.Errorf("%w: entity limit", ErrQuotaExceeded)
	}
	if maximum := quota.MaxEdgesPerTenant; maximum != nil && *maximum > 0 && plan.EdgeCount > int64(*maximum) && plan.EdgeCount >= view.Graph.EdgeCount {
		return Plan{}, fmt.Errorf("%w: edge limit", ErrQuotaExceeded)
	}
	return plan, nil
}
