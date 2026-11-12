package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

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
	for _, entity := range chunk.Entities {
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
		var existing graph.Entity
		found, err := view.Get(ctx, Bucket(entity.ID), EntityKey(entity.ID), &existing)
		if err != nil {
			return Plan{}, err
		}
		if found && !equalJSON(entity, existing) {
			return Plan{}, conflict("import entity already exists with different content")
		}
		if err := b.entity(schema, entity, true); err != nil {
			return Plan{}, err
		}
	}
	for _, edge := range chunk.Edges {
		if edge.ID != graph.CanonicalEdgeID(edge) || edge.Version > view.Graph.Version {
			return Plan{}, invalid("import edge identity or version is invalid")
		}
		var from, to graph.Entity
		found, err := view.Get(ctx, Bucket(edge.From), EntityKey(edge.From), &from)
		if err != nil {
			return Plan{}, err
		}
		if !found {
			return Plan{}, invalid("import edge has no source entity")
		}
		found, err = view.Get(ctx, Bucket(edge.To), EntityKey(edge.To), &to)
		if err != nil {
			return Plan{}, err
		}
		if !found {
			return Plan{}, invalid("import edge has no target entity")
		}
		if _, err := graph.FromSnapshot(graph.Snapshot{Version: view.Graph.Version, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes, Entities: []graph.Entity{from, to}, Edges: []graph.Edge{edge}}); err != nil {
			return Plan{}, invalid(err.Error())
		}
		var existing graph.Edge
		found, err = view.Get(ctx, Bucket(edge.From), EdgeKey(edge.ID), &existing)
		if err != nil {
			return Plan{}, err
		}
		if found && !equalJSON(existing, edge) {
			return Plan{}, conflict("import edge already exists with different content")
		}
		if err := storage.ValidateRelationSchemaEdge(edge, view.Graph.RelationSchemas); err != nil {
			return Plan{}, invalid(err.Error())
		}
		if err := b.edge(schema, edge, true); err != nil {
			return Plan{}, err
		}
	}
	directories := &directoryReads{ctx: ctx, view: view, owners: map[string][]string{}}
	if err := b.finishDirectories(directories, "import violates a global identity, unique or cardinality constraint"); err != nil {
		return Plan{}, err
	}
	return Plan{Changes: b.changes, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes, Report: graph.ApplyReport{Changed: len(b.changes) > 0}}, nil
}

func (e Engine) Import(ctx context.Context, chunk ImportChunk, key string) (storage.CommitResult, error) {
	data, err := json.Marshal(transactionInput{Import: &chunk})
	if err != nil {
		return storage.CommitResult{}, err
	}
	return e.submitInput(ctx, data, "build:"+key, nil, "build", "", nil)
}

func ImportIdentity(phase, cursor string) string {
	return fmt.Sprintf("%s:%x", phase, sha256.Sum256([]byte(cursor)))
}
