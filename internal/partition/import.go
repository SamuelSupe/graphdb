package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type ImportChunk struct {
	Schema   bool           `json:"schema,omitempty"`
	Entities []graph.Entity `json:"entities,omitempty"`
	Edges    []graph.Edge   `json:"edges,omitempty"`
}

type transactionInput struct {
	Ingest               *IngestInput                  `json:"ingest,omitempty"`
	LegacyCollectorState *storage.CollectorStateUpdate `json:"legacy_collector_state,omitempty"`
	Commit               *CommitRequest                `json:"commit,omitempty"`
	Import               *ImportChunk                  `json:"import,omitempty"`
}

func LegacyTransaction(record storage.DirectCommitRecord, generation int64) (Transaction, error) {
	request := storage.NormalizeDirectCommitRequest(record.Request)
	commit := CommitRequest{Mutations: request.Mutations, ExpectedVersion: request.ExpectedVersion, IdempotencyKey: request.IdempotencyKey}
	encoded, err := json.Marshal(transactionInput{Commit: &commit, LegacyCollectorState: request.CollectorState})
	if err != nil {
		return Transaction{}, err
	}
	state := "retryable"
	if record.Status == "committed" || record.Status == "" {
		state = "committed"
	}
	return Transaction{Tenant: record.TenantID, Generation: generation, Purpose: "legacy", ID: record.Result.HeadCommitID, Identity: "direct:" + request.IdempotencyKey, Digest: fmt.Sprintf("%x", sha256.Sum256(encoded)), At: record.StartedAt, BaseVersion: max(int64(0), record.Result.Version-1), State: state, Result: record.Result}, nil
}

type CopyProgress struct {
	Phase    string `json:"phase"`
	Cursor   string `json:"cursor,omitempty"`
	DataHash string `json:"data_hash"`
}

func BuildImportPlan(ctx context.Context, view View, chunk ImportChunk) (Plan, error) {
	if view.Graph.State != "initializing" {
		return Plan{}, invalid("raw records can only populate an unpublished graph generation")
	}
	entityIDs := make(map[string]struct{}, len(chunk.Entities))
	for _, entity := range chunk.Entities {
		if _, exists := entityIDs[entity.ID]; exists {
			return Plan{}, invalid("duplicate entity ID in import chunk")
		}
		entityIDs[entity.ID] = struct{}{}
	}
	edgeIDs := make(map[string]struct{}, len(chunk.Edges))
	for _, edge := range chunk.Edges {
		if _, exists := edgeIDs[edge.ID]; exists {
			return Plan{}, invalid("duplicate edge ID in import chunk")
		}
		edgeIDs[edge.ID] = struct{}{}
	}
	schema, err := graph.FromSnapshot(graph.Snapshot{Version: view.Graph.Version, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes})
	if err != nil {
		return Plan{}, err
	}
	b := &changeBuilder{changes: ChangeSet{}, ownerEdits: map[string]map[string]bool{}}
	if chunk.Schema {
		for _, ci := range view.Graph.CITypes {
			if err := b.changes.Logical(0, ci.Name, ci); err != nil {
				return Plan{}, err
			}
		}
		for _, rt := range view.Graph.RelationTypes {
			if err := b.changes.Logical(2, rt.Name, rt); err != nil {
				return Plan{}, err
			}
		}
	}
	lookup := &Lookup{View: view}
	for entities := range slices.Chunk(chunk.Entities, 16) {
		ids := make([]string, 0, len(entities))
		for _, entity := range entities {
			if entity.ID == "" || entity.Version > view.Graph.Version {
				return Plan{}, invalid("import entity identity or version is invalid")
			}
			fields, err := schema.EffectiveFields(entity.Kind)
			if err != nil {
				return Plan{}, invalid(err.Error())
			}
			if err := graph.ValidatePropertyFields("entity "+entity.ID, entity.Fields, fields, false); err != nil {
				return Plan{}, invalid(err.Error())
			}
			ids = append(ids, entity.ID)
		}
		existing, _, err := lookup.GetEntities(ctx, ids, nil)
		if err != nil {
			return Plan{}, err
		}
		for _, entity := range entities {
			if prior, found := existing[entity.ID]; found && !equalJSON(entity, prior) {
				return Plan{}, conflict("import entity already exists with different content")
			}
			if err := b.entity(schema, entity, true); err != nil {
				return Plan{}, err
			}
		}
	}
	for edges := range slices.Chunk(chunk.Edges, 16) {
		if err := addImportEdges(ctx, lookup, b, schema, edges); err != nil {
			return Plan{}, err
		}
	}
	directories := &directoryReads{ctx: ctx, view: view, owners: map[string][]string{}}
	if err := b.finishDirectories(directories, "import violates a global identity, unique or cardinality constraint"); err != nil {
		return Plan{}, err
	}
	return Plan{Changes: b.changes, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes, Report: graph.ApplyReport{Changed: len(b.changes) > 0}}, nil
}

// Bound each dependency round because an endpoint may contain up to one MiB.
func addImportEdges(ctx context.Context, lookup *Lookup, b *changeBuilder, schema *graph.Graph, edges []graph.Edge) error {
	view := lookup.View
	ids := make([]string, 0, len(edges)*2)
	seen := make(map[string]bool, len(edges)*2)
	keys := make(map[uint8][]string)
	for _, edge := range edges {
		if edge.ID != graph.CanonicalEdgeID(edge) || edge.Version > view.Graph.Version {
			return invalid("import edge identity or version is invalid")
		}
		for _, id := range []string{edge.From, edge.To} {
			if !seen[id] {
				ids = append(ids, id)
				seen[id] = true
			}
		}
		bucket := Bucket(edge.From)
		keys[bucket] = append(keys[bucket], EdgeKey(edge.ID))
	}
	endpoints, _, err := lookup.GetEntities(ctx, ids, nil)
	if err != nil {
		return err
	}
	existing, err := view.fetchEdges(ctx, keys)
	if err != nil {
		return err
	}
	for _, edge := range edges {
		from, found := endpoints[edge.From]
		if !found {
			return invalid("import edge has no source entity")
		}
		to, found := endpoints[edge.To]
		if !found {
			return invalid("import edge has no target entity")
		}
		if _, err := graph.FromSnapshot(graph.Snapshot{Version: view.Graph.Version, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes, Entities: []graph.Entity{from, to}, Edges: []graph.Edge{edge}}); err != nil {
			return invalid(err.Error())
		}
		if prior, found := existing[edge.ID]; found && !equalJSON(prior, edge) {
			return conflict("import edge already exists with different content")
		}
		if err := storage.ValidateRelationSchemaEdge(edge, view.Graph.RelationSchemas); err != nil {
			return invalid(err.Error())
		}
		if err := b.edge(schema, edge, true); err != nil {
			return err
		}
	}
	return nil
}

func (e Engine) Import(ctx context.Context, chunk ImportChunk, key string) (storage.CommitResult, error) {
	data, err := json.Marshal(transactionInput{Import: &chunk})
	if err != nil {
		return storage.CommitResult{}, err
	}
	return e.submitInput(ctx, data, "build:"+key, nil, "build", "", nil, nil)
}

func ImportIdentity(phase, cursor string) string {
	return fmt.Sprintf("%s:%x", phase, sha256.Sum256([]byte(cursor)))
}
