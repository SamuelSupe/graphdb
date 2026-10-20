package storage

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestRangeSkewedByteSplitKeepsSmallRecordsTogether(t *testing.T) {
	entities := []graph.Entity{{ID: "host:000", Kind: "host", Fields: graph.Fields{"blob": strings.Repeat("x", 20<<20)}}}
	for i := 1; i < 129; i++ {
		entities = append(entities, graph.Entity{ID: fmt.Sprintf("host:%03d", i), Kind: "host", Fields: graph.Fields{"blob": "small"}})
	}
	pages, err := prepareBoundedEntityPages(context.Background(), "tenant-a", []EntityPageData{{Shard: "00", Entities: entities}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 2 || len(pages[0].Entities) != 1 || len(pages[1].Entities) != 128 {
		t.Fatalf("skewed bytes fragmented 128 small records: %d pages", len(pages))
	}
	edges := make([]graph.Edge, len(entities))
	for i, entity := range entities {
		edges[i] = graph.Edge{ID: entity.ID, Type: "links", From: "host:a", To: "host:b", Fields: entity.Fields}
	}
	shards, err := prepareBoundedEdgeShards(context.Background(), "tenant-a", []EdgeShardData{{Shard: "00", RelationType: "links", Edges: edges}})
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 2 || len(shards[0].Edges) != 1 || len(shards[1].Edges) != 128 {
		t.Fatalf("skewed bytes fragmented 128 small edges: %d pages", len(shards))
	}
}

func TestRangePageEncodedByteLimitAndOversizedRecord(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(712))
	payload := func(size int) string {
		data := make([]byte, size)
		rng.Read(data)
		return base64.RawStdEncoding.EncodeToString(data)
	}
	for _, oversized := range []bool{false, true} {
		t.Run(fmt.Sprint(oversized), func(t *testing.T) {
			size := 4 << 20
			if oversized {
				size = 9 << 20
			}
			entities := []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"blob": payload(size)}}, {ID: "host:b", Kind: "host", Fields: graph.Fields{"blob": payload(size)}}}
			pages, err := prepareBoundedEntityPages(ctx, "tenant-a", []EntityPageData{{Shard: "00", Version: 1, Entities: entities}})
			if err != nil {
				t.Fatal(err)
			}
			if len(pages) < 2 {
				t.Fatal("wide records were left in one unbounded file")
			}
			for _, page := range pages {
				data, err := marshalParquetEntityPage(ctx, page)
				if err != nil {
					t.Fatal(err)
				}
				routing, err := decodeIndexRange(page.pageRole)
				if err != nil {
					t.Fatal(err)
				}
				if routing.Bytes != int64(len(data)) {
					t.Fatal("manifest byte size differs from persisted encoding")
				}
				if int64(len(data)) > indexPageMaxBytes && len(page.Entities) != 1 {
					t.Fatal("multi-record entity page exceeds byte limit")
				}
				if oversized && int64(len(data)) <= indexPageMaxBytes {
					t.Fatal("fixture failed to exercise the oversized-record exception")
				}
			}
			edges := []graph.Edge{{ID: "edge:a", Type: "links", From: "host:a", To: "host:b", Fields: entities[0].Fields}, {ID: "edge:b", Type: "links", From: "host:b", To: "host:a", Fields: entities[1].Fields}}
			for _, reverse := range []bool{false, true} {
				shards, err := prepareBoundedEdgeShards(ctx, "tenant-a", []EdgeShardData{{Shard: "00", RelationType: "links", Version: 1, Edges: edges, reverse: reverse}})
				if err != nil {
					t.Fatal(err)
				}
				if len(shards) < 2 {
					t.Fatal("wide edges were left in one unbounded file")
				}
				for _, shard := range shards {
					data, err := marshalParquetEdgeShard(ctx, shard)
					if err != nil {
						t.Fatal(err)
					}
					routing, _ := decodeIndexRange(shard.pageRole)
					if routing.Bytes != int64(len(data)) || (int64(len(data)) > indexPageMaxBytes && len(shard.Edges) != 1) {
						t.Fatal("edge byte bound or manifest size is invalid")
					}
				}
			}
		})
	}
}

func TestRangeCountSplitIsBalanced(t *testing.T) {
	entities := make([]graph.Entity, 1025)
	edges := make([]graph.Edge, 1025)
	for i := range entities {
		entities[i] = graph.Entity{ID: fmt.Sprintf("host:%04d", i), Kind: "host"}
		edges[i] = graph.Edge{ID: fmt.Sprintf("edge:%04d", i), Type: "links", From: "host:a", To: "host:b"}
	}
	pages, err := prepareBoundedEntityPages(context.Background(), "tenant-a", []EntityPageData{{Shard: "00", Entities: entities}})
	if err != nil {
		t.Fatal(err)
	}
	shards, err := prepareBoundedEdgeShards(context.Background(), "tenant-a", []EdgeShardData{{Shard: "00", RelationType: "links", Edges: edges}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 2 || len(shards) != 2 || len(pages[0].Entities) < 500 || len(pages[1].Entities) < 500 || len(shards[0].Edges) < 500 || len(shards[1].Edges) < 500 {
		t.Fatal("overflow created a nearly full page and a tiny append page")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepareBoundedEntityPages(canceled, "tenant-a", pages); err == nil {
		t.Fatal("canceled entity encoding succeeded")
	}
	if _, err := prepareBoundedEdgeShards(canceled, "tenant-a", shards); err == nil {
		t.Fatal("canceled edge encoding succeeded")
	}
}
