package storage

import (
	"bytes"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestCommitPayloadKeepsVersion10CanonicalEntityJSON(t *testing.T) {
	entity := graph.Entity{
		ID:     "document:1",
		Kind:   "document",
		Fields: graph.Fields{"title": "one"},
	}
	if err := graph.SetEntityLabels(&entity, []string{"article"}); err != nil {
		t.Fatal(err)
	}
	commit := graph.Commit{
		ID:        "commit-1",
		TenantID:  "tenant-a",
		Version:   1,
		CreatedAt: time.Unix(1, 0).UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{entity}},
	}
	payload, err := commitPayloadJSON(commit)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`"labels"`)) {
		t.Fatalf("persisted commit hash payload contains 1.1 API-only labels: %s", payload)
	}
	if !bytes.Contains(payload, []byte(`"__graphdb_labels":["article"]`)) {
		t.Fatalf("persisted commit hash payload lost compatible labels field: %s", payload)
	}

	data, err := marshalCommitObject(commit)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := unmarshalCommitObject(data)
	if err != nil {
		t.Fatal(err)
	}
	if labels := graph.EntityLabels(decoded.Mutations.UpsertEntities[0]); len(labels) != 1 || labels[0] != "article" {
		t.Fatalf("decoded labels = %#v", labels)
	}
}
