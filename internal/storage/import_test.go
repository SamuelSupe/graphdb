package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestBulkImportJSONLAndCSV(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	jsonl := strings.Join([]string{
		`{"entity_type":{"name":"document"}}`,
		`{"entity_type":{"name":"person"}}`,
		`{"relation_type":{"name":"authored_by","from_kind":"document","to_kind":"person","directed":true}}`,
		`{"entity":{"id":"doc:1","kind":"document","labels":["article"],"fields":{"title":"One"}}}`,
		`{"entity":{"id":"person:1","kind":"person","fields":{"name":"Ada"}}}`,
		`{"edge":{"id":"edge:doc1-author","type":"authored_by","from":"doc:1","to":"person:1","fields":{"role":"author"}}}`,
	}, "\n")
	task, err := store.StartImport(ctx, "tenant-a", []byte(jsonl), ImportOptions{Format: "jsonl", BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	task = waitForTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded || task.Result["applied"] != float64(6) || task.Result["batches"] != float64(3) {
		t.Fatalf("JSONL task = %#v", task)
	}

	csvData := strings.Join([]string{
		"record_type,id,entity_type,labels,relation_type,from,to,title,pages",
		"entity,doc:2,document,article|guide,,,,Two,12",
		"edge,edge:doc2-author,,,authored_by,doc:2,person:1,,",
	}, "\n")
	task, err = store.StartImport(ctx, "tenant-a", []byte(csvData), ImportOptions{Format: "csv", BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	task = waitForTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded || task.Result["applied"] != float64(2) {
		t.Fatalf("CSV task = %#v", task)
	}

	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	document, ok := g.Entities["doc:2"]
	if !ok || document.Fields["title"] != "Two" || document.Fields["pages"] != float64(12) {
		t.Fatalf("document = %#v ok=%v", document, ok)
	}
	if labels := graph.EntityLabels(document); len(labels) != 2 || labels[0] != "article" || labels[1] != "guide" {
		t.Fatalf("labels = %#v", labels)
	}
	neighbors := g.Neighbors("doc:2", "out", "authored_by")
	if len(neighbors) != 1 || neighbors[0].Entity.ID != "person:1" {
		t.Fatalf("neighbors = %#v", neighbors)
	}
}

func TestBulkImportContinueReportsInvalidJSONL(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	data := strings.Join([]string{
		`{"entity_type":{"name":"topic"}}`,
		`{"entity":`,
		`{"entity":{"id":"topic:1","kind":"topic","labels":["knowledge"]}}`,
	}, "\n")
	task, err := store.StartImport(ctx, "tenant-a", []byte(data), ImportOptions{Format: "jsonl", OnError: "continue"})
	if err != nil {
		t.Fatal(err)
	}
	task = waitForTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded || task.Result["failed"] != float64(1) || task.Result["applied"] != float64(2) {
		t.Fatalf("task = %#v", task)
	}
	issues, ok := task.Result["issues"].([]any)
	if !ok || len(issues) != 1 {
		t.Fatalf("issues = %#v", task.Result["issues"])
	}
}

func TestBulkImportSourceFollowsTaskRetention(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	task, err := store.StartImport(ctx, "tenant-a", []byte(`{"entity_type":{"name":"topic"}}`), ImportOptions{Format: "jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	task = waitForTask(t, ctx, store, "tenant-a", task.ID)
	sourceKey := stringTaskParam(task.Params, "source_key")
	if _, err := store.Objects.Get(ctx, sourceKey); err != nil {
		t.Fatalf("get staged source: %v", err)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	task.StartedAt, task.UpdatedAt, task.FinishedAt = old, old, old
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{TaskMaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if report.DeletedTasks != 1 || report.DeletedImportSources != 1 {
		t.Fatalf("GC report = %#v", report)
	}
	if _, err := store.Objects.Get(ctx, sourceKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("source error = %v, want ErrNotFound", err)
	}
}
