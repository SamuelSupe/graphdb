package storage

import (
	"bytes"
	"encoding/json"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestLabelsUseExistingEntityPageFieldRows(t *testing.T) {
	entity := graph.Entity{ID: "document:1", Kind: "document"}
	if err := graph.SetEntityLabels(&entity, []string{"article", "knowledge"}); err != nil {
		t.Fatal(err)
	}
	rows, err := entityPageRows(entity)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Kind != entityPageRowField || rows[0].Key != graph.ReservedLabelsField {
		t.Fatalf("labels changed entity page layout: %#v", rows)
	}
	decoded := graph.Entity{ID: entity.ID, Kind: entity.Kind}
	if err := applyEntityPageRow(&decoded, rows[0]); err != nil {
		t.Fatal(err)
	}
	labels := graph.EntityLabels(decoded)
	if len(labels) != 2 || labels[0] != "article" || labels[1] != "knowledge" {
		t.Fatalf("round-trip labels=%#v", labels)
	}
}

func TestEntityPageHashKeepsVersion10CanonicalEntityJSON(t *testing.T) {
	entity := graph.Entity{ID: "document:1", Kind: "document"}
	if err := graph.SetEntityLabels(&entity, []string{"article"}); err != nil {
		t.Fatal(err)
	}
	page := EntityPageData{Shard: "01", Entities: []graph.Entity{entity}}
	payload, err := json.Marshal(struct {
		Shard    string         `json:"shard"`
		Entities []legacyEntity `json:"entities"`
	}{
		Shard: page.Shard, Entities: legacyEntities(page.Entities),
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`"labels"`)) {
		t.Fatalf("persisted entity page hash contains API-only labels: %s", payload)
	}
	if !bytes.Contains(payload, []byte(`"__graphdb_labels":["article"]`)) {
		t.Fatalf("persisted entity page hash lost compatible labels field: %s", payload)
	}
	if got := entityPageContentHash(page); got != objectContentHash(payload) {
		t.Fatalf("entity page hash = %q, want legacy hash %q", got, objectContentHash(payload))
	}
}

func TestSnapshotHashKeepsVersion10CanonicalEntityJSON(t *testing.T) {
	entity := graph.Entity{ID: "document:1", Kind: "document"}
	if err := graph.SetEntityLabels(&entity, []string{"knowledge"}); err != nil {
		t.Fatal(err)
	}
	record := snapshotRecord{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Snapshot: graph.Snapshot{
			Version: 1, Entities: []graph.Entity{entity},
			RelationTypes: []graph.RelationType{}, Edges: []graph.Edge{},
		},
	}
	payload, err := snapshotRecordPayloadJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`"labels"`)) {
		t.Fatalf("persisted snapshot hash contains API-only labels: %s", payload)
	}
	if !bytes.Contains(payload, []byte(`"__graphdb_labels":["knowledge"]`)) {
		t.Fatalf("persisted snapshot hash lost compatible labels field: %s", payload)
	}
}
