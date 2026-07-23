package storage

import (
	"sync"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorSamePriorityConflictFollowsCommitOrder(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "field-order")
	objects := NewMemoryStore()
	writers := []*TenantStore{
		NewTenantStore(objects, "test"),
		NewTenantStore(objects, "test"),
	}
	for index, writer := range writers {
		writer.InstanceID = "field-writer-" + string(rune('a'+index))
		writer.SetCoordinator(coordinator)
	}
	type outcome struct {
		value   string
		version int64
		err     error
	}
	start := make(chan struct{})
	results := make(chan outcome, len(writers))
	var wg sync.WaitGroup
	for index, writer := range writers {
		index, writer := index, writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			value := string(rune('a' + index))
			result, err := writer.CommitWithReport(ctx, "tenant-a", graph.Mutations{
				UpsertEntities: []graph.Entity{{
					ID: "document:shared", Kind: "document",
					Fields: graph.Fields{"owner": value},
				}},
			}, CommitOptions{})
			results <- outcome{value: value, version: result.Version, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var winner outcome
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent same-field commit: %v", result.err)
		}
		if result.version > winner.version {
			winner = result
		}
	}
	g, manifest, err := writers[0].Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph: %v", err)
	}
	entity, ok := g.GetEntity("document:shared")
	if !ok {
		t.Fatal("shared entity is missing")
	}
	if manifest.Version != 2 || entity.Fields["owner"] != winner.value {
		t.Fatalf("manifest=%#v entity=%#v winning commit=%#v", manifest, entity, winner)
	}
}

func TestPostgresCoordinatorSchemaCommitRaceCannotBothSucceed(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "schema-race")
	objects := NewMemoryStore()
	schemaWriter := NewTenantStore(objects, "test")
	graphWriter := NewTenantStore(objects, "test")
	schemaWriter.SetCoordinator(coordinator)
	graphWriter.SetCoordinator(coordinator)
	if _, err := graphWriter.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "cites", Directed: true, FromKind: "document", ToKind: "document",
		}},
		UpsertEntities: []graph.Entity{
			{ID: "document:a", Kind: "document"},
			{ID: "document:b", Kind: "document"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	start := make(chan struct{})
	var schemaErr error
	var commitErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, schemaErr = schemaWriter.PutRelationSchema(ctx, "tenant-a", RelationSchema{
			RelationType: "cites",
			Strict:       true,
			Fields: map[string]graph.FieldSpec{
				"weight": {Type: "number", Required: true},
			},
		})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, commitErr = graphWriter.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEdges: []graph.Edge{{
				ID: "edge:missing-weight", Type: "cites",
				From: "document:a", To: "document:b",
			}},
		}, CommitOptions{})
	}()
	close(start)
	wg.Wait()
	if schemaErr == nil && commitErr == nil {
		t.Fatal("schema update and invalid edge commit both succeeded")
	}

	g, _, err := graphWriter.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after race: %v", err)
	}
	edgeExists := false
	for _, edge := range g.Edges {
		if edge.Type == "cites" &&
			edge.From == "document:a" &&
			edge.To == "document:b" {
			edgeExists = true
			break
		}
	}
	catalog, err := schemaWriter.GetRelationSchemas(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load relation schemas: %v", err)
	}
	schemaExists := len(catalog.RelationSchemas) == 1
	if schemaExists == edgeExists {
		t.Fatalf(
			"race produced inconsistent state: schema_exists=%v edge_exists=%v schema_err=%v commit_err=%v",
			schemaExists, edgeExists, schemaErr, commitErr,
		)
	}
}

func TestPostgresCoordinatorResolvesCandidateAfterHeadAdvances(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "candidate-resolution")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	candidate, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("candidate head exists=%v err=%v", exists, err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:second", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	current, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || current.Revision <= candidate.Revision {
		t.Fatalf("current head=%#v candidate=%#v err=%v", current, candidate, err)
	}

	resolved, published, err := coordinator.resolveAmbiguousPublish(
		ctx,
		HeadPublishRequest{TenantID: "tenant-a"},
		candidate,
	)
	if err != nil || !published {
		t.Fatalf("resolve advanced candidate published=%v err=%v", published, err)
	}
	if resolved.Revision != candidate.Revision ||
		resolved.CommitID != candidate.CommitID ||
		resolved.ManifestHash != candidate.ManifestHash {
		t.Fatalf("resolved candidate=%#v, want %#v", resolved, candidate)
	}
}
