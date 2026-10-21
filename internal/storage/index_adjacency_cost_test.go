package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type adjacencyCostStore struct {
	ObjectStore
	mu       sync.Mutex
	writes   map[string][]byte
	putCount int
	putBytes int
}

func (s *adjacencyCostStore) record(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writes == nil {
		s.writes = map[string][]byte{}
	}
	s.writes[key] = append([]byte(nil), data...)
	s.putCount++
	s.putBytes += len(data)
}

func (s *adjacencyCostStore) Put(ctx context.Context, key string, data []byte) error {
	s.record(key, data)
	return s.ObjectStore.Put(ctx, key, data)
}
func (s *adjacencyCostStore) PutConditional(ctx context.Context, key string, data []byte, c PutCondition) (ObjectMeta, error) {
	s.record(key, data)
	return s.ObjectStore.PutConditional(ctx, key, data, c)
}
func (s *adjacencyCostStore) reset() {
	s.mu.Lock()
	s.writes = map[string][]byte{}
	s.putCount, s.putBytes = 0, 0
	s.mu.Unlock()
}

func TestAdjacencyTreeLocalWriteAndReadCosts(t *testing.T) {
	ctx := context.Background()
	for _, count := range []int{1024, 10000, 100000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			objects := &adjacencyCostStore{ObjectStore: NewMemoryStore()}
			s := NewTenantStore(objects, "test")
			defer s.ShutdownTasks(ctx)
			changes := make([]adjacencyChange, 0, count)
			for i := 0; i < count; i++ {
				changes = append(changes, adjacencyChange{key: adjacencyKey{fmt.Sprintf("host:%06d", i%97), fmt.Sprintf("edge:%09d", i)}})
			}
			root, err := s.updateAdjacency(ctx, "tenant-a", s.adjacencyPrefix("tenant-a", 1, "out"), nil, changes)
			if err != nil {
				t.Fatal(err)
			}
			objects.reset()
			key := adjacencyKey{"host:000050", "edge:000050000"}
			next, err := s.updateAdjacency(ctx, "tenant-a", s.adjacencyPrefix("tenant-a", 2, "out"), root, []adjacencyChange{{key: key}})
			if err != nil {
				t.Fatal(err)
			}
			if next.Count != count+1 {
				t.Fatalf("count=%d", next.Count)
			}
			leafWrites, bytes := 0, 0
			for _, data := range objects.writes {
				var n adjacencyNode
				if err := json.Unmarshal(data, &n); err != nil {
					t.Fatal(err)
				}
				if len(n.Entries) > 0 {
					leafWrites++
				}
				bytes += len(data)
			}
			if leafWrites > 2 || objects.putCount > root.Height+4 || objects.putBytes > 2*adjacencyLeafBytes+(root.Height+2)*adjacencyBranchBytes {
				t.Fatalf("unbounded update: leaves=%d nodes=%d bytes=%d height=%d", leafWrites, len(objects.writes), bytes, root.Height)
			}
			it, err := s.seekAdjacency(ctx, "tenant-a", *next, adjacencyKey{"host:000050", ""})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for {
				id, ok, err := it.next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				got = append(got, id)
			}
			var want []string
			for _, c := range changes {
				if c.key.Endpoint == "host:000050" {
					want = append(want, c.key.ID)
				}
			}
			want = append(want, key.ID)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("endpoint seek differs: got=%d want=%d", len(got), len(want))
			}
			// The previous root remains independently readable after path copying.
			seen := map[string]struct{}{}
			if err := s.walkAdjacency(ctx, "tenant-a", *root, seen, nil); err != nil {
				t.Fatal(err)
			}
			// Same base and batch yield exactly the same bytes and root.
			again, err := s.updateAdjacency(ctx, "tenant-a", s.adjacencyPrefix("tenant-a", 2, "out"), root, []adjacencyChange{{key: key}})
			if err != nil || !reflect.DeepEqual(next, again) {
				t.Fatalf("nondeterministic tree: %v", err)
			}
		})
	}
}

func TestAdjacencyFieldsOnlyCommitReusesRootAndBoundsPayload(t *testing.T) {
	ctx := context.Background()
	objects := &adjacencyCostStore{ObjectStore: NewMemoryStore()}
	s := NewTenantStore(objects, "test")
	s.WriteIndexRangePages = true
	s.WriteIndexAdjacencyPages = true
	defer s.ShutdownTasks(ctx)
	entities := []graph.Entity{{ID: "host:hub", Kind: "host"}}
	var edges []graph.Edge
	for i := 0; i < 2050; i++ {
		id := fmt.Sprintf("host:leaf-%06d", i)
		entities = append(entities, graph.Entity{ID: id, Kind: "host"})
		edges = append(edges, graph.Edge{Type: "links", From: entities[0].ID, To: id})
	}
	if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities, UpsertEdges: edges, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	old, err := s.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	oldReverse, err := s.GetReverseIndexCatalog(ctx, "tenant-a", old.Version)
	if err != nil {
		t.Fatal(err)
	}
	objects.reset()
	edge := edges[0]
	edge.Fields = graph.Fields{"value": "changed"}
	result, err := s.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{edge}}, CommitOptions{})
	if err != nil || len(result.IndexWarnings) != 0 {
		t.Fatalf("commit=%+v %v", result, err)
	}
	next, err := s.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := s.GetReverseIndexCatalog(ctx, "tenant-a", next.Version)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2][]EdgeShard{{old.EdgeShards, next.EdgeShards}, {oldReverse.EdgeShards, rev.EdgeShards}} {
		for i, spec := range pair[0] {
			if spec.AdjacencyRoot == nil || !reflect.DeepEqual(spec.AdjacencyRoot, pair[1][i].AdjacencyRoot) {
				t.Fatal("field-only update changed adjacency root")
			}
		}
	}
	for key := range objects.writes {
		if isAdjacencyObjectKey(key) {
			t.Fatal("field-only commit rewrote adjacency", key)
		}
	}
	rows := 0
	oldKeys := map[string]bool{}
	for _, spec := range old.EdgeShards {
		for _, o := range spec.Objects {
			oldKeys[o.Key] = true
		}
	}
	for _, spec := range next.EdgeShards {
		for _, o := range spec.Objects {
			if !oldKeys[o.Key] {
				rows += o.RowCount
			}
		}
	}
	if rows == 0 || rows > 1024 {
		t.Fatalf("field-only payload rewrites %d rows", rows)
	}
	// Dense adjacency bypasses the ID leaves. Losing a later payload page
	// must still fail after partial traversal instead of requesting fallback.
	if err := objects.Delete(ctx, next.EdgeShards[0].Objects[1].Key); err != nil {
		t.Fatal(err)
	}
	fresh := NewTenantStore(objects, "test")
	defer fresh.ShutdownTasks(ctx)
	q := &PersistedIndexLookup{Store: fresh, TenantID: "tenant-a", Version: next.Version, Catalog: next}
	visited := 0
	if _, err := q.VisitOutEdges(ctx, "host:hub", nil, "", func(graph.Edge) (bool, error) { visited++; return true, nil }); err == nil || visited == 0 {
		t.Fatalf("missing dense payload after %d rows: %v", visited, err)
	}
}

func TestAdjacencyOversizedTupleAndCanceledUpdate(t *testing.T) {
	ctx := context.Background()
	s := NewTenantStore(NewMemoryStore(), "test")
	defer s.ShutdownTasks(ctx)
	long := strings.Repeat("x", 300<<10)
	root, err := s.updateAdjacency(ctx, "tenant-a", s.adjacencyPrefix("tenant-a", 1, "long"), nil, []adjacencyChange{{key: adjacencyKey{long, "a"}}, {key: adjacencyKey{long, "b"}}, {key: adjacencyKey{"z", "c"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.walkAdjacency(ctx, "tenant-a", *root, map[string]struct{}{}, nil); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.updateAdjacency(canceled, "tenant-a", s.adjacencyPrefix("tenant-a", 2, "long"), root, []adjacencyChange{{key: adjacencyKey{"z", "c"}, remove: true}}); err != context.Canceled {
		t.Fatalf("cancellation=%v", err)
	}
	it, err := s.seekAdjacency(ctx, "tenant-a", *root, adjacencyKey{long, "b"})
	if err != nil {
		t.Fatal(err)
	}
	if id, ok, err := it.next(ctx); err != nil || !ok || id != "b" {
		t.Fatalf("seek oversized: %s %v %v", id, ok, err)
	}
}

func TestAdjacencyCompactionMergesAcrossRoutingBoundary(t *testing.T) {
	ctx := context.Background()
	s := NewTenantStore(NewMemoryStore(), "test")
	defer s.ShutdownTasks(ctx)
	prefix := s.adjacencyPrefix("tenant-a", 1, "boundary")
	var leaves []AdjacencyRef
	index := 0
	for _, count := range []int{600, 200, 200, 600} {
		var entries []adjacencyKey
		for j := 0; j < count; j++ {
			entries = append(entries, adjacencyKey{"host:hub", fmt.Sprintf("edge:%06d", index)})
			index++
		}
		refs, err := s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Entries: entries}, 0)
		if err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, refs...)
	}
	a, err := s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: leaves[:2]}, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: leaves[2:]}, 1)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: append(a, b...)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	next, changed, done, err := s.compactAdjacencyRoot(ctx, "tenant-a", s.adjacencyPrefix("tenant-a", 2, "boundary"), &roots[0], map[string]bool{})
	if err != nil || !changed || done || next.Count != 1600 {
		t.Fatalf("cross-boundary compaction: %+v %v %v %v", next, changed, done, err)
	}
	if next.Height != 1 {
		t.Fatal("routing root did not contract")
	}
	node, err := s.loadAdjacencyNode(ctx, "tenant-a", *next)
	if err != nil {
		t.Fatal(err)
	}
	if len(node.Children) != 3 || node.Children[1].Count != 400 {
		t.Fatal("boundary leaves did not merge locally")
	}
	it, err := s.seekAdjacency(ctx, "tenant-a", *next, adjacencyKey{"host:hub", "edge:000700"})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		_, ok, err := it.next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		count++
	}
	if count != 900 {
		t.Fatalf("compaction seek lost entries: %d", count)
	}
	if err := s.walkAdjacency(ctx, "tenant-a", roots[0], map[string]struct{}{}, nil); err != nil {
		t.Fatal("old root damaged", err)
	}
}
