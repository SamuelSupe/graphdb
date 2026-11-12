package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestBalancedLeafPreparePreservesLegacyReplay(t *testing.T) {
	ctx := context.Background()
	baseRows := make([]Row, pageEntries)
	for i := range baseRows {
		baseRows[i] = Row{Key: fmt.Sprintf("k/%03d", i), Value: []byte(`true`)}
	}
	newRow := Row{Key: "k/999", Value: []byte(`true`)}
	var legacyRoot Root
	for _, revision := range []int{ActionRevision, BatchGCRevision, PlannedGCRevision, BalancedLeafRevision} {
		t.Run(fmt.Sprint(revision), func(t *testing.T) {
			store := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
			tree := store.Tree("t", 1, 0)
			base, err := tree.Update(ctx, Root{}, baseRows)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.save(ctx, store.dataKey("t", 1, 0), DataState{Tenant: "t", Generation: 1, Epoch: 1, State: "active", Root: base}); err != nil {
				t.Fatal(err)
			}
			changes := []Row{newRow}
			data, err := json.Marshal(changes)
			if err != nil {
				t.Fatal(err)
			}
			action := Action{Operation: "prepare", Tenant: "t", Generation: 1, Epoch: 1, Txn: "replay", Sequence: 1,
				Revision: revision, Base: base, Changes: changes, Digest: fmt.Sprintf("%x", sha256.Sum256(data))}
			value, err := store.ApplyFragment(ctx, action)
			if err != nil {
				t.Fatal(err)
			}
			prepared := value.(Prepared)
			page, err := tree.load(ctx, prepared.Root)
			if err != nil {
				t.Fatal(err)
			}
			var counts []int64
			for _, child := range page.Children {
				counts = append(counts, child.Count)
			}
			wantCounts := []int64{64, 1}
			if revision == BalancedLeafRevision {
				wantCounts = []int64{33, 32}
			} else if legacyRoot.Hash == "" {
				legacyRoot = prepared.Root
			} else if legacyRoot != prepared.Root {
				t.Fatal("old revisions produced different roots")
			}
			if !slices.Equal(counts, wantCounts) {
				t.Fatalf("leaf counts: %v, want %v", counts, wantCounts)
			}
			action.Revision = BalancedLeafRevision
			retry, err := store.ApplyFragment(ctx, action)
			if err != nil || retry.(Prepared).Root != prepared.Root {
				t.Fatalf("retry with upgraded coordinator changed prepare: %+v %v", retry, err)
			}
			var got []string
			err = tree.Scan(ctx, prepared.Root, "", "", func(row Row) (bool, error) {
				got = append(got, row.Key)
				return true, nil
			})
			if err != nil || len(got) != len(baseRows)+1 || got[len(got)-1] != newRow.Key {
				t.Fatalf("scan of split root: %d rows, %v", len(got), err)
			}
			for i, row := range baseRows {
				if got[i] != row.Key {
					t.Fatalf("scan row %d: %q", i, got[i])
				}
			}
		})
	}
}

func TestBalancedLeafSplitHonorsByteLimit(t *testing.T) {
	value := []byte(`"` + strings.Repeat("x", pageBytes/8-16) + `"`)
	rows := make([]Row, 9)
	for i := range rows {
		rows[i] = Row{Key: fmt.Sprintf("k/%03d", i), Value: value}
	}
	if split := balancedLeafSplit(rows); split != 5 {
		t.Fatalf("byte overflow split at %d, want 5", split)
	}
	rows[0].Value = []byte(`"` + strings.Repeat("x", MaxValueBytes/2) + `"`)
	if split := balancedLeafSplit(rows); split != 1 {
		t.Fatalf("large value split at %d, want singleton", split)
	}
}

func TestTreePersistentBatchUpdatesAndPaging(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	tree := Tree{Objects: objects, Prefix: "pages"}
	rows := make([]Row, MaxChanges)
	for i := range rows {
		rows[i] = Row{Key: fmt.Sprintf("entity/%06d", i), Value: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))}
	}
	root, err := tree.Update(ctx, Root{}, rows)
	if err != nil {
		t.Fatal(err)
	}
	old := root
	want := map[string]string{}
	for _, r := range rows {
		want[r.Key] = string(r.Value)
	}
	rng := rand.New(rand.NewSource(391))
	for batch := 0; batch < 25; batch++ {
		edits := map[string]Row{}
		for i := 0; i < 150; i++ {
			id := rng.Intn(6000)
			key := fmt.Sprintf("entity/%06d", id)
			row := Row{Key: key}
			if rng.Intn(3) != 0 {
				row.Value = json.RawMessage(fmt.Sprintf(`{"batch":%d}`, batch))
			}
			edits[key] = row
		}
		var changes []Row
		for key, row := range edits {
			changes = append(changes, row)
			if len(row.Value) == 0 {
				delete(want, key)
			} else {
				want[key] = string(row.Value)
			}
		}
		root, err = tree.Update(ctx, root, changes)
		if err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	err = tree.Scan(ctx, root, "entity/", "", func(row Row) (bool, error) {
		if string(row.Value) != want[row.Key] {
			t.Fatalf("unexpected row %q", row.Key)
		}
		got = append(got, row.Key)
		return true, nil
	})
	if err != nil || len(got) != len(want) || !sort.StringsAreSorted(got) {
		t.Fatalf("scan count=%d want=%d err=%v", len(got), len(want), err)
	}
	for key, value := range want {
		v, ok, err := tree.Get(ctx, root, key)
		if err != nil || !ok || string(v) != value {
			t.Fatalf("get %q: %s %v %v", key, v, ok, err)
		}
	}
	for _, row := range rows {
		v, ok, err := tree.Get(ctx, old, row.Key)
		if err != nil || !ok || string(v) != string(row.Value) {
			t.Fatalf("old root changed: %q", row.Key)
		}
	}
	count := 0
	err = tree.Scan(ctx, root, "entity/", "entity/003000", func(row Row) (bool, error) {
		if row.Key < "entity/003000" {
			t.Fatal("scan did not seek")
		}
		count++
		return count < 7, nil
	})
	if err != nil || count != 7 {
		t.Fatalf("bounded scan=%d err=%v", count, err)
	}
	page, err := objects.Get(ctx, tree.pageKey(root.Hash))
	if err != nil {
		t.Fatal(err)
	}
	page[len(page)/2] ^= 1
	if err := objects.Put(ctx, tree.pageKey(root.Hash), page); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tree.Get(ctx, root, got[0]); err == nil {
		t.Fatal("corrupt root was accepted")
	}
}
