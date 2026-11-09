package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

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
