package graph

import (
	"strings"
	"testing"
)

func TestContentHashMatchesColdEncoding(t *testing.T) {
	g := graphWithCompany(t)
	assertContentHashMatchesColdEncoding(t, g)
}

func TestContentHashMatchesColdEncodingForRichMetadata(t *testing.T) {
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
	g.Entities.Set("service:api", Entity{
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
	})
	g.Entities.Set("service:db", Entity{ID: "service:db", Kind: "service"})
	g.Edges.Set("edge:api-db", Edge{
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
	})
	assertContentHashMatchesColdEncoding(t, g)
}

func TestContentHashCacheTracksStorageMutations(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID: "seed", Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:b", Kind: "host"},
			{ID: "host:d", Kind: "host"},
		}},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	if _, err := g.ContentHash(); err != nil {
		t.Fatalf("prime content cache: %v", err)
	}
	next, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
		ID: "copy", Version: 2,
		Mutations: Mutations{
			DeleteEntities: []string{"host:b"},
			UpsertEntities: []Entity{
				{ID: "host:a", Kind: "host"},
				{ID: "host:d", Kind: "host", Fields: Fields{"state": "ready"}},
			},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatalf("apply storage copy: %v", err)
	}
	assertContentHashMatchesColdEncoding(t, next)
	if err := next.ApplyCommitInPlaceForStorage(Commit{
		ID: "replay", Version: 3,
		Mutations: Mutations{
			DeleteEntities: []string{"host:d"},
			UpsertEntities: []Entity{{ID: "host:c", Kind: "host"}},
		},
	}); err != nil {
		t.Fatalf("apply in-place replay: %v", err)
	}
	assertContentHashMatchesColdEncoding(t, next)
	assertContentHashMatchesColdEncoding(t, g)
}

func assertContentHashMatchesColdEncoding(t *testing.T, g *Graph) {
	t.Helper()
	got, logicalBytes, err := g.ContentHashWithLogicalSize()
	if err != nil {
		t.Fatalf("content hash: %v", err)
	}

	cold := g.Clone()
	cold.logicalHashCache = nil
	want, wantBytes, err := cold.ContentHashWithLogicalSize()
	if err != nil {
		t.Fatal(err)
	}
	if got != want || logicalBytes != wantBytes || !strings.HasPrefix(got, ContentHashAlgorithm+":") {
		t.Fatalf("cached hash/size = %s/%d, cold = %s/%d", got, logicalBytes, want, wantBytes)
	}
}

func TestContentHashV2ContractAndSnapshotRoundTrip(t *testing.T) {
	g := New()
	g.RelationTypes = nil
	g.Entities.Set("host:a", Entity{ID: "host:a", Kind: "host", Fields: Fields{"state": "ready"}})
	const expected = "sha256-shards-v2:df65092bfc4b1470ae96be175542b9ed68e9b987b6d5a3c02fc757516abf959e"
	got, err := g.ContentHash()
	if err != nil || got != expected {
		t.Fatalf("digest = %q, %v; want %q", got, err, expected)
	}
	g = graphWithCompany(t)
	want, err := g.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := FromSnapshot(g.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored.Version = 99
	got, err = restored.ContentHash()
	if err != nil || got != want {
		t.Fatalf("snapshot round trip changed digest: %q, %v", got, err)
	}
}
