package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type countedPageStore struct {
	storage.ObjectStore
	gets  int64
	bytes int64
}

func (s *countedPageStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.ObjectStore.Get(ctx, key)
	if strings.Contains(key, "/pages/") {
		s.gets++
		s.bytes += int64(len(data))
	}
	return data, err
}

func batchReadFixture(tb testing.TB, onDisk bool) (Store, ReadRequest, *countedPageStore) {
	tb.Helper()
	ctx := context.Background()
	var objects storage.ObjectStore = storage.NewMemoryStore()
	if onDisk {
		files, err := storage.OpenFileStore(filepath.Join(tb.TempDir(), "data"))
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = files.Close() })
		objects = files
	}
	counted := &countedPageStore{ObjectStore: objects}
	store := Store{Objects: counted, Prefix: "graphdb"}
	rows := make([]Row, 4096)
	for i := range rows {
		id := fmt.Sprintf("node-%06d", i)
		value, _ := json.Marshal(graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"payload": strings.Repeat("x", 96)}})
		rows[i] = Row{Key: EntityKey(id), Value: value}
	}
	root, err := store.Tree("t", 1, 0).Update(ctx, Root{}, rows)
	if err != nil {
		tb.Fatal(err)
	}
	if err := store.save(ctx, store.dataKey("t", 1, 0), DataState{Tenant: "t", Generation: 1, Partition: 0, Epoch: 1, State: "active", Root: root}); err != nil {
		tb.Fatal(err)
	}
	request := ReadRequest{Tenant: "t", Generation: 1, Epoch: 1, Root: root}
	for i := range 128 {
		request.Keys = append(request.Keys, rows[i].Key)
	}
	counted.gets = 0
	counted.bytes = 0
	return store, request, counted
}

func TestPartitionBatchReadReusesPagesAndKeepsRequestIsolation(t *testing.T) {
	ctx := context.Background()
	store, request, counted := batchReadFixture(t, false)
	response, err := store.Read(ctx, request)
	if err != nil || len(response.Rows) != len(request.Keys) {
		t.Fatalf("batch result rows=%d error=%v", len(response.Rows), err)
	}
	for i, row := range response.Rows {
		if row.Key != request.Keys[i] {
			t.Fatalf("batch changed request order at %d", i)
		}
	}
	if counted.gets > 4 {
		t.Fatalf("batch reread shared tree pages: got %d page reads for 128 neighboring keys", counted.gets)
	}
	tree := store.Tree("t", 1, 0)
	tree.cache = &treeReadCache{}
	if _, _, err := tree.Get(ctx, request.Root, request.Keys[0]); err != nil {
		t.Fatal(err)
	}
	invalid := request.Root
	invalid.Count++
	if _, _, err := tree.Get(ctx, invalid, request.Keys[0]); err == nil {
		t.Fatal("page reuse accepted a root with mismatched metadata")
	}
	// A fresh request must validate the physical pages again. A request-local
	// optimization must not hide missing/corrupted data from subsequent reads.
	rootKey := store.Tree("t", 1, 0).pageKey(request.Root.Hash)
	original, err := store.Objects.Get(ctx, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), original...)
	corrupt[len(corrupt)/2] ^= 1
	if err := store.Objects.Put(ctx, rootKey, corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(ctx, request); err == nil {
		t.Fatal("batch page reuse outlived the request and hid corruption")
	}
}

func TestPartitionBatchReadContinuesAtResponseByteLimit(t *testing.T) {
	ctx := context.Background()
	store, request, _ := batchReadFixture(t, false)
	value, _ := json.Marshal(strings.Repeat("x", 900<<10))
	rows := make([]Row, 6)
	for i := range rows {
		rows[i] = Row{Key: fmt.Sprintf("large-%d", i), Value: value}
	}
	root, err := store.Tree("t", 1, 0).Update(ctx, Root{}, rows)
	if err != nil {
		t.Fatal(err)
	}
	request.Root = root
	request.Keys = nil
	for _, row := range rows {
		request.Keys = append(request.Keys, row.Key)
	}
	first, err := store.Read(ctx, request)
	if err != nil || len(first.Rows) != 4 || len(first.Remaining) != 2 {
		t.Fatalf("bounded batch rows=%d remaining=%d error=%v", len(first.Rows), len(first.Remaining), err)
	}
	request.Keys = first.Remaining
	last, err := store.Read(ctx, request)
	if err != nil || len(last.Rows) != 2 || len(last.Remaining) != 0 {
		t.Fatalf("continued batch rows=%d remaining=%d error=%v", len(last.Rows), len(last.Remaining), err)
	}
	combined := append(first.Rows, last.Rows...)
	for i, row := range combined {
		if row.Key != rows[i].Key || string(row.Value) != string(value) {
			t.Fatalf("continuation changed row %d", i)
		}
	}
}

func BenchmarkPartitionReadBatch(b *testing.B) {
	for _, onDisk := range []bool{false, true} {
		name := "memory"
		if onDisk {
			name = "file"
		}
		b.Run(name, func(b *testing.B) {
			for _, scattered := range []bool{false, true} {
				name := "adjacent"
				if scattered {
					name = "scattered"
				}
				b.Run(name, func(b *testing.B) {
					store, request, counted := batchReadFixture(b, onDisk)
					if scattered {
						for i := range request.Keys {
							request.Keys[i] = EntityKey(fmt.Sprintf("node-%06d", i*32))
						}
					}
					ctx := context.Background()
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						response, err := store.Read(ctx, request)
						if err != nil || len(response.Rows) != 128 {
							b.Fatalf("batch failed rows=%d error=%v", len(response.Rows), err)
						}
					}
					b.ReportMetric(float64(counted.gets)/float64(b.N), "page_get/op")
					b.ReportMetric(float64(counted.bytes)/float64(b.N), "page_bytes/op")
				})
			}
		})
	}
}

type countedReadRemote struct {
	Remote
	calls int
	bytes int64
}

func (r *countedReadRemote) Read(ctx context.Context, g Group, request ReadRequest) (ReadResult, error) {
	result, err := r.Remote.Read(ctx, g, request)
	r.calls++
	for _, row := range result.Rows {
		r.bytes += int64(len(row.Key) + len(row.Value))
	}
	return result, err
}

func bothEdgesFixture(tb testing.TB, sparse bool) (View, *countedReadRemote) {
	tb.Helper()
	ctx := context.Background()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	id := "hub"
	bucket := Bucket(id)
	var rows []Row
	for i := range 256 {
		kind := "calls"
		if sparse && i%64 != 0 {
			kind = "noise"
		}
		edge := graph.Edge{ID: fmt.Sprintf("edge-%06d", i), Type: kind, From: id, To: id, Fields: graph.Fields{"payload": strings.Repeat("x", 1024)}}
		value, _ := json.Marshal(edge)
		rows = append(rows, Row{Key: EdgeKey(edge.ID), Value: value})
		reference, _ := json.Marshal(edgeLocation{ID: edge.ID, Partition: bucket})
		for _, direction := range []string{"out", "in"} {
			rows = append(rows, Row{Key: AdjacencyKey(id, direction, edge.ID), Value: reference})
		}
	}
	root, err := store.Tree("t", 1, bucket).Update(ctx, Root{}, rows)
	if err != nil {
		tb.Fatal(err)
	}
	if err := store.save(ctx, store.dataKey("t", 1, bucket), DataState{Tenant: "t", Generation: 1, Partition: bucket, Epoch: 1, State: "active", Root: root}); err != nil {
		tb.Fatal(err)
	}
	group := Group{ID: "a", ClusterID: "a", Peers: map[uint64]string{1: "http://one", 2: "http://two", 3: "http://three"}}
	g := Graph{Format: FormatVersion, Tenant: "t", Generation: 1, Version: 1, State: "active", Groups: map[string]Group{"a": group}}
	g.Partitions[bucket] = Placement{Group: "a", Epoch: 1}
	g.Roots[bucket] = root
	remote := &countedReadRemote{Remote: &localRemote{stores: map[string]Store{"a": store}}}
	return View{Graph: g, Remote: remote}, remote
}

func TestDistributedBothEdgesStopsWithinReadBudget(t *testing.T) {
	view, _ := bothEdgesFixture(t, false)
	view.Budget = &ReadBudget{Requests: 64, Bytes: 64 << 10}
	seen := 0
	ok, err := (&Lookup{View: view}).VisitBothEdges(context.Background(), "hub", nil, "", func(edge graph.Edge, direction string) (bool, error) {
		seen++
		if edge.ID != "edge-000000" || direction != "in" {
			t.Fatalf("unexpected first neighbor: %s %s", edge.ID, direction)
		}
		return false, nil
	})
	if err != nil || !ok || seen != 1 {
		t.Fatalf("bounded both-direction read: seen=%d available=%v error=%v", seen, ok, err)
	}
}

func TestDistributedBothEdgesSparseFilterKeepsRPCBudget(t *testing.T) {
	view, remote := bothEdgesFixture(t, true)
	view.Budget = &ReadBudget{Requests: 16, Bytes: 2 << 20}
	seen := 0
	ok, err := (&Lookup{View: view}).VisitBothEdges(context.Background(), "hub", map[string]struct{}{"calls": {}}, "", func(edge graph.Edge, direction string) (bool, error) {
		seen++
		if edge.ID != "edge-000000" || direction != "in" {
			t.Fatalf("unexpected filtered neighbor: %s %s", edge.ID, direction)
		}
		return false, nil
	})
	if err != nil || !ok || seen != 1 || remote.calls > 16 {
		t.Fatalf("sparse filter multiplied RPCs: seen=%d requests=%d error=%v", seen, remote.calls, err)
	}
}

func BenchmarkDistributedBothEdges(b *testing.B) {
	for _, scenario := range []struct {
		name        string
		all, sparse bool
	}{{name: "first"}, {name: "all", all: true}, {name: "sparse_first", sparse: true}, {name: "sparse_all", all: true, sparse: true}} {
		b.Run(scenario.name, func(b *testing.B) {
			view, remote := bothEdgesFixture(b, scenario.sparse)
			ctx := context.Background()
			var allowed map[string]struct{}
			if scenario.sparse {
				allowed = map[string]struct{}{"calls": {}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				seen := 0
				ok, err := (&Lookup{View: view}).VisitBothEdges(ctx, "hub", allowed, "", func(graph.Edge, string) (bool, error) { seen++; return scenario.all, nil })
				want := 1
				if scenario.all {
					want = 512
					if scenario.sparse {
						want = 8
					}
				}
				if err != nil || !ok || seen != want {
					b.Fatalf("visit seen=%d error=%v", seen, err)
				}
			}
			b.ReportMetric(float64(remote.calls)/float64(b.N), "rpc/op")
			b.ReportMetric(float64(remote.bytes)/float64(b.N), "row_bytes/op")
		})
	}
}

type scanRPCRemote struct {
	Remote
	stores map[string]Store
	calls  int
}

func (r *scanRPCRemote) Read(ctx context.Context, group Group, req ReadRequest) (ReadResult, error) {
	r.calls++
	return r.Remote.Read(ctx, group, req)
}

func (r *scanRPCRemote) ReadBatch(ctx context.Context, group Group, req []ReadRequest) ([]ReadResult, error) {
	r.calls++
	return r.stores[group.ID].ReadBatch(ctx, req)
}

func TestDistributedScanBatchesHeadsAndPrimaryRecords(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	g, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil {
		t.Fatal(err)
	}
	g.Version = 1
	g.CITypes = []graph.CIType{{Name: "host"}, {Name: "rare"}}
	changes := ChangeSet{}
	const count = 40000
	for i := range count {
		id := fmt.Sprintf("node-%05d", i)
		kind := "host"
		if i%1000 == 0 {
			kind = "rare"
		}
		bucket := Bucket(id)
		if err := changes.Set(bucket, EntityKey(id), graph.Entity{ID: id, Kind: kind}); err != nil {
			t.Fatal(err)
		}
		if err := changes.Set(bucket, Key("entity_scan", id), entityScanEntry{ID: id, Kind: kind}); err != nil {
			t.Fatal(err)
		}
		g.EntityCounts[bucket]++
		g.EntityCount++
	}
	for _, bucket := range changes.Participants() {
		owner, err := g.GroupFor(bucket)
		if err != nil {
			t.Fatal(err)
		}
		g.Roots[bucket], err = remote.stores[owner.ID].Tree("t", 1, bucket).Update(ctx, Root{}, changes.Rows(bucket))
		if err != nil {
			t.Fatal(err)
		}
	}
	reader := &scanRPCRemote{Remote: remote, stores: remote.stores}
	view := View{Graph: g, Remote: reader}
	for _, tc := range []struct {
		name            string
		request         query.Request
		results, maxRPC int
	}{
		{"first100", query.Request{Op: "match", Limit: 100}, 100, 32},
		{"first1000", query.Request{Op: "match", Limit: 1000}, 1000, 128},
		{"all_sorted", query.Request{Op: "match", Limit: 1000, Sort: []query.SortSpec{{Field: "id"}}}, 1000, 1800},
		{"rare_sorted", query.Request{Op: "match", Kind: "rare", Limit: 100, Sort: []query.SortSpec{{Field: "id"}}}, 40, 24},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader.calls = 0
			response, err := ExecuteQuery(ctx, view, tc.request)
			if err != nil || len(response.Results) != tc.results || reader.calls > tc.maxRPC {
				t.Fatalf("rows=%d calls=%d error=%v", len(response.Results), reader.calls, err)
			}
			t.Logf("rows=%d read RPCs=%d", len(response.Results), reader.calls)
		})
	}
}
