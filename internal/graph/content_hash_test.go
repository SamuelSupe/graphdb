package graph

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestContentMD5MatchesSnapshotBasedEncoding(t *testing.T) {
	g := graphWithCompany(t)
	assertContentMD5MatchesSnapshotBasedEncoding(t, g)
}

func TestContentMD5MatchesSnapshotBasedEncodingForRichMetadata(t *testing.T) {
	g := New()
	g.CITypes["service"] = CIType{
		Name: "service",
		Fields: map[string]FieldSpec{
			"region": {Type: "string", Indexed: true, Enum: []any{"sg", "us"}, Default: "sg"},
		},
		IdentityKeys: []IdentityKey{{Name: "name-region", Fields: []string{"name", "region"}, ConfidenceThreshold: 0.8}},
	}
	g.RelationTypes["calls"] = RelationType{Name: "calls", FromKind: "service", ToKind: "service", Directed: true, Cardinality: ManyToMany}
	owner := FieldSource{Source: "agent", Priority: 10, Confidence: 0.9}
	g.Entities["service:api"] = Entity{
		ID:              "service:api",
		Kind:            "service",
		Fields:          Fields{"name": "api", "region": "sg", "replicas": float64(3)},
		FieldSources:    map[string]FieldSource{"region": owner},
		ExistenceSource: &owner,
		Source:          "agent",
		ExternalID:      "api-1",
		Identity:        map[string]any{"name-region": "api|sg"},
		Confidence:      0.9,
		SourceRank:      10,
		Sources: []EntitySource{
			{Source: "z-source", ExternalID: "z"},
			{Source: "a-source", ExternalID: "a", Confidence: 0.8},
		},
		MergedFrom: []string{"legacy:z", "legacy:a"},
		SplitFrom:  "service:monolith",
	}
	g.Entities["service:db"] = Entity{ID: "service:db", Kind: "service"}
	g.Edges["edge:api-db"] = Edge{
		ID:              "edge:api-db",
		Type:            "calls",
		From:            "service:api",
		To:              "service:db",
		Fields:          Fields{"protocol": "grpc"},
		FieldSources:    map[string]FieldSource{"protocol": owner},
		ExistenceSource: &owner,
		Sources: []EdgeSource{
			{Source: "z-source", ExternalID: "z", EdgeID: "z-edge"},
			{Source: "a-source", ExternalID: "a", EdgeID: "a-edge"},
		},
	}
	assertContentMD5MatchesSnapshotBasedEncoding(t, g)
}

func assertContentMD5MatchesSnapshotBasedEncoding(t *testing.T, g *Graph) {
	t.Helper()
	got, logicalBytes, err := g.ContentMD5WithLogicalSize()
	if err != nil {
		t.Fatalf("content md5: %v", err)
	}

	snapshot := g.Snapshot()
	legacy := logicalSnapshot{
		CITypes:       snapshot.CITypes,
		RelationTypes: snapshot.RelationTypes,
		Entities:      make([]logicalEntity, 0, len(snapshot.Entities)),
		Edges:         make([]logicalEdge, 0, len(snapshot.Edges)),
	}
	for _, entity := range snapshot.Entities {
		legacy.Entities = append(legacy.Entities, logicalEntityFromEntity(entity))
	}
	for _, edge := range snapshot.Edges {
		legacy.Edges = append(legacy.Edges, logicalEdgeFromEdge(edge))
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy snapshot: %v", err)
	}
	sum := md5.Sum(data)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("content md5 = %q, snapshot-based value = %q", got, want)
	}
	if logicalBytes != int64(len(data)) {
		t.Fatalf("logical bytes = %d, snapshot encoding bytes = %d", logicalBytes, len(data))
	}
}
