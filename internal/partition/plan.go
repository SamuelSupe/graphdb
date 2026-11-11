package partition

import (
	"context"
	"encoding/json"
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

type edgeLocation struct {
	ID        string `json:"id"`
	Partition uint8  `json:"partition"`
}

type fragmentLoader struct {
	view          View
	ctx           context.Context
	entities      map[string]graph.Entity
	edges         map[string]graph.Edge
	entityLookups map[string][]string
	edgeLookups   map[string]bool
	bytes         int
}

func (l *fragmentLoader) charge(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	l.bytes += len(data)
	if l.bytes > 64<<20 || len(l.entities) > 8192 || len(l.edges) > 8192 {
		return invalid("mutation dependency set exceeds the transaction memory budget")
	}
	return nil
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
	key := EntityAliasKey(id)
	var owners []string
	_, err = l.view.Get(l.ctx, Bucket(key), key, &owners)
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

func (l *fragmentLoader) edge(id string) error {
	if id == "" || l.edgeLookups[id] {
		return nil
	}
	l.edgeLookups[id] = true
	key := EdgeLocatorKey(id)
	var loc edgeLocation
	found, err := l.view.Get(l.ctx, Bucket(key), key, &loc)
	if err != nil {
		return err
	}
	if !found {
		key = EdgeAliasKey(id)
		var owners []string
		if _, err := l.view.Get(l.ctx, Bucket(key), key, &owners); err != nil {
			return err
		}
		for _, owner := range owners {
			if err := l.edge(owner); err != nil {
				return err
			}
		}
		return nil
	}
	var edge graph.Edge
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
	if _, err := l.entity(edge.From); err != nil {
		return err
	}
	_, err = l.entity(edge.To)
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
			for _, row := range batch.Rows {
				var loc edgeLocation
				if err := decodeRecord(row, &loc); err != nil {
					return err
				}
				if err := l.edge(loc.ID); err != nil {
					return err
				}
			}
			if batch.Next == "" {
				break
			}
			start = batch.Next
		}
	}
	return nil
}

func (l *fragmentLoader) constraints(g *graph.Graph, entity graph.Entity, incoming bool) error {
	normalized, refs, err := g.PartitionEntityReferences(entity, incoming)
	if err != nil {
		return invalid(err.Error())
	}
	if _, err := l.entity(normalized.ID); err != nil {
		return err
	}
	for _, ref := range refs {
		key := ConstraintKey(ref)
		var owners []string
		if _, err := l.view.Get(l.ctx, Bucket(key), key, &owners); err != nil {
			return err
		}
		for _, id := range owners {
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
		var owners []string
		if _, err := l.view.Get(l.ctx, Bucket(key), key, &owners); err != nil {
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
	l := &fragmentLoader{view: view, ctx: ctx, entities: map[string]graph.Entity{}, edges: map[string]graph.Edge{}, entityLookups: map[string][]string{}, edgeLookups: map[string]bool{}}
	for _, condition := range conditions {
		if condition.ResourceType == "entity" {
			if _, err := l.entity(condition.ID); err != nil {
				return Plan{}, err
			}
		} else if err := l.edge(condition.ID); err != nil {
			return Plan{}, err
		}
	}
	for _, entity := range m.UpsertEntities {
		if err := l.constraints(preview, entity, true); err != nil {
			return Plan{}, err
		}
	}
	var incident []string
	incident = append(incident, m.DeleteEntities...)
	for _, r := range m.DeleteEntityRequests {
		if r.ID != "" {
			incident = append(incident, r.ID)
		} else {
			entity := graph.Entity{Kind: r.Kind, Source: r.Source, ExternalID: r.ExternalID}
			if err := l.constraints(preview, entity, true); err != nil {
				return Plan{}, err
			}
			_, refs, err := preview.PartitionEntityReferences(entity, true)
			if err != nil {
				return Plan{}, err
			}
			for _, ref := range refs {
				var owners []string
				key := ConstraintKey(ref)
				if _, err := view.Get(ctx, Bucket(key), key, &owners); err != nil {
					return Plan{}, err
				}
				incident = append(incident, owners...)
			}
		}
	}
	for _, r := range m.MergeEntities {
		incident = append(incident, r.TargetID)
		incident = append(incident, r.SourceIDs...)
	}
	for _, r := range m.SplitEntities {
		incident = append(incident, r.SourceID)
		for _, entity := range r.Entities {
			if err := l.constraints(preview, entity, true); err != nil {
				return Plan{}, err
			}
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
	for _, id := range m.DeleteEdges {
		if err := l.edge(id); err != nil {
			return Plan{}, err
		}
	}
	for _, r := range m.DeleteEdgeRequests {
		id := r.ID
		if id == "" && r.Type != "" && r.From != "" && r.To != "" {
			id = graph.CanonicalEdgeIDParts(r.Type, r.From, r.To)
		}
		if err := l.edge(id); err != nil {
			return Plan{}, err
		}
	}
	for _, edge := range m.UpsertEdges {
		if _, err := l.entity(strings.TrimSpace(edge.From)); err != nil {
			return Plan{}, err
		}
		if _, err := l.entity(strings.TrimSpace(edge.To)); err != nil {
			return Plan{}, err
		}
		if err := l.edgeDependencies(edge, preview.RelationTypes); err != nil {
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
		for _, entity := range after.Entities.All() {
			if err := l.constraints(after, entity, false); err != nil {
				return Plan{}, err
			}
		}
		for _, edge := range after.Edges.All() {
			if err := l.edgeDependencies(edge, after.RelationTypes); err != nil {
				return Plan{}, err
			}
		}
		if loaded == len(l.entities)+len(l.edges) {
			break
		}
		if round == 31 {
			return Plan{}, invalid("mutation identity dependency closure exceeds its budget")
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
	plan, err := buildChanges(ctx, view, before, after, report)
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
