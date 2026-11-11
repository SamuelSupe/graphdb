package partition

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestPartitionIngestDecisionSurvivesLostResponses(t *testing.T) {
	for _, lost := range []string{"begin", "prepare", "commit", "finalize", "complete"} {
		t.Run(lost, func(t *testing.T) {
			ctx := context.Background()
			engine, remote := newLocalGraph(t)
			engine.Generation = 1
			if _, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}, UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true}}}}); err != nil {
				t.Fatal(err)
			}
			input := IngestInput{AcceptedAt: time.Now().UTC(), Request: storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "one", Items: []storage.IngestItem{
				{Entity: &graph.Entity{ID: "left", Kind: "host"}}, {Entity: &graph.Entity{ID: "right", Kind: "host"}},
				{Edge: &graph.Edge{Type: "calls", From: "left", To: "right"}},
			}}}
			remote.lose = lost
			if lost == "finalize" {
				remote.blocked = "finalize"
			}
			_, firstErr := engine.SubmitIngest(ctx, "ingest:one", input)
			if lost == "finalize" && firstErr == nil {
				t.Fatal("reported completion before participant finalization was durable")
			}
			remote.blocked = ""
			if lost == "finalize" {
				remote.lose = ""
			}
			result, err := engine.SubmitIngest(ctx, "ingest:one", input)
			if err != nil || result.Version != 2 || result.Applied != 3 || result.Failed != 0 {
				t.Fatalf("retry after lost %s: %+v %v", lost, result, err)
			}
			txn, err := remote.Transaction(ctx, engine.Home, "t", 1, "ingest:one")
			if err != nil || txn.IngestResult == nil || !reflect.DeepEqual(*txn.IngestResult, result) {
				t.Fatalf("transaction lost its ingest decision: %+v %v", txn, err)
			}
			g, err := remote.Describe(ctx, engine.Home, "t")
			if err != nil || g.Version != 2 || g.EntityCount != 2 || g.EdgeCount != 1 {
				t.Fatalf("duplicate publication: %+v %v", g, err)
			}
			input.Request.Cursor = "changed"
			if _, err := engine.SubmitIngest(ctx, "ingest:one", input); err == nil {
				t.Fatal("identity accepted different durable input")
			}
		})
	}
}

func TestPartitionIngestTransactionalFailuresAndRecovery(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	engine.Generation = 1
	if _, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}}}); err != nil {
		t.Fatal(err)
	}
	acceptedAt := time.Now().UTC().Add(-time.Hour)
	base := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "batch", Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "guard", Kind: "host", Fields: graph.Fields{"updated_at": acceptedAt.Add(time.Second).Format(time.RFC3339Nano)}}}}}
	if result, err := engine.SubmitIngest(ctx, "ingest:guard", IngestInput{Request: base, AcceptedAt: acceptedAt}); err != nil || result.Version != 2 {
		t.Fatalf("guard: %+v %v", result, err)
	}
	version := int64(1)
	for _, test := range []struct {
		name string
		edit func(*storage.IngestRequest)
		code string
	}{
		{"version", func(r *storage.IngestRequest) { r.ExpectedVersion = &version }, storage.IngestErrorVersionConflict},
		{"precondition", func(r *storage.IngestRequest) {
			r.Preconditions = []storage.IngestPrecondition{{ResourceType: "entity", ID: "guard", Op: "not_exists"}}
		}, storage.IngestErrorPreconditionFailed},
		{"large-detail", func(r *storage.IngestRequest) {
			r.Preconditions = []storage.IngestPrecondition{{ResourceType: "entity", ID: "guard", Field: "updated_at", Op: "eq", Value: strings.Repeat("x", MaxValueBytes)}}
		}, storage.IngestErrorPreconditionFailed},
		{"atomic", func(r *storage.IngestRequest) {
			r.FailureMode = storage.IngestFailureModeAtomic
			r.Items = append(r.Items, storage.IngestItem{})
		}, storage.IngestErrorAtomicValidation},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, _ := json.Marshal(base)
			var request storage.IngestRequest
			if err := json.Unmarshal(data, &request); err != nil {
				t.Fatal(err)
			}
			request.BatchID = test.name
			request.Items[0].Entity.ID = test.name
			test.edit(&request)
			result, err := engine.SubmitIngest(ctx, "ingest:"+test.name, IngestInput{Request: request, AcceptedAt: acceptedAt})
			if err != nil || result.Applied != 0 || result.Failed != len(request.Items) || result.ErrorCode != test.code {
				t.Fatalf("terminal failure: %+v %v", result, err)
			}
			if test.name == "large-detail" && (!result.DetailsTruncated || len(result.Conflicts) != 0) {
				t.Fatal("oversized diagnostics were not bounded")
			}
			g, err := remote.Describe(ctx, engine.Home, "t")
			if err != nil || g.Version != 2 || g.Active != nil || g.EntityCount != 1 || len(g.CITypes) != 1 {
				t.Fatalf("failed batch changed data or stranded the writer: %+v %v", g, err)
			}
		})
	}
	base.BatchID = "after-failure"
	base.Items[0].Entity.ID = "after-failure"
	base.Preconditions = []storage.IngestPrecondition{{ResourceType: "entity", ID: "guard", Field: "updated_at", Op: "gt", ValueFrom: "accepted_at"}}
	base.Items = append(base.Items, storage.IngestItem{})
	result, err := engine.SubmitIngest(ctx, "ingest:after-failure", IngestInput{Request: base, AcceptedAt: acceptedAt})
	if err != nil || result.Version != 3 || result.Applied != 1 || result.Failed != 1 {
		t.Fatalf("best-effort batch after failures: %+v %v", result, err)
	}
}
