package partition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type digestReadCounter struct {
	*storage.FileStore
	bodies    int
	unchanged int
}

func (c *digestReadCounter) Get(ctx context.Context, key string) ([]byte, error) {
	c.bodies++
	return c.FileStore.Get(ctx, key)
}

func (c *digestReadCounter) GetIfFileUnchanged(ctx context.Context, key string, previous os.FileInfo) ([]byte, os.FileInfo, error) {
	data, info, err := c.FileStore.GetIfFileUnchanged(ctx, key, previous)
	if err == nil && info != nil && data == nil {
		c.unchanged++
	} else {
		c.bodies++
	}
	return data, info, err
}

func TestLogicalBucketDigestReusesVerifiedLeaves(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	files, err := storage.OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	counter := &digestReadCounter{FileStore: files}
	tree := Tree{Objects: counter, Prefix: "pages", logicalDigests: &LogicalDigestCache{}}
	prefix := LogicalPrefix(3, 42)
	neighbor := LogicalPrefix(3, 43)
	var rows []Row
	for n := range 32 * pageEntries {
		rows = append(rows, Row{Key: fmt.Sprintf("%s%04d", prefix, n), Value: json.RawMessage(fmt.Sprintf(`"%064x"`, n+1))})
	}
	// Escaped hex is accepted by the historical JSON string decoder.
	last := Row{Key: neighbor + "tail", Value: json.RawMessage(fmt.Sprintf(`"\u0030%s"`, fmt.Sprintf("%063X", 255)))}
	rows = append(rows, last)
	root, err := tree.Update(ctx, Root{}, rows)
	if err != nil {
		t.Fatal(err)
	}
	changes := []Row{rows[0], last}
	assertDigest := func(root Root) {
		t.Helper()
		got, err := tree.LogicalBuckets(ctx, root, changes)
		if err != nil {
			t.Fatal(err)
		}
		want := make(map[int]string)
		for index, prefix := range map[int]string{3*256 + 42: prefix, 3*256 + 43: neighbor} {
			h := sha256.New()
			count := 0
			err := (Tree{Objects: files, Prefix: tree.Prefix}).Scan(ctx, root, prefix, "", func(row Row) (bool, error) {
				var text string
				if err := json.Unmarshal(row.Value, &text); err != nil {
					return false, err
				}
				decoded, err := hex.DecodeString(text)
				if err != nil {
					return false, err
				}
				count++
				_, _ = h.Write(decoded)
				return true, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			want[index] = ""
			if count != 0 {
				want[index] = hex.EncodeToString(h.Sum(nil))
			}
		}
		if !maps.Equal(got, want) {
			t.Fatalf("logical digests changed: got %v, want %v", got, want)
		}
	}
	assertDigest(root)
	updated, err := tree.Update(ctx, root, []Row{
		{Key: rows[pageEntries*12].Key, Value: json.RawMessage(fmt.Sprintf(`"%064x"`, 4000))},
		{Key: rows[0].Key},
		{Key: last.Key},
		{Key: prefix + "new", Value: rows[1].Value},
	})
	if err != nil {
		t.Fatal(err)
	}
	counter.bodies, counter.unchanged = 0, 0
	assertDigest(updated)
	if (runtime.GOOS == "linux" || runtime.GOOS == "darwin") && (counter.unchanged < 28 || counter.bodies > 6) {
		t.Fatalf("sparse edit: %d page bodies, %d unchanged pages", counter.bodies, counter.unchanged)
	}
	branch, err := tree.load(ctx, updated)
	if err != nil {
		t.Fatal(err)
	}
	leaf := branch.Children[3]
	key := tree.pageKey(leaf.Hash)
	path := filepath.Join(dir, filepath.FromSlash(key))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 1
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := tree.LogicalBuckets(ctx, updated, changes); !errors.Is(err, ErrPartitionCorrupt) {
		t.Fatalf("warm digest hid a modified leaf: %v", err)
	}
	if err := files.Put(ctx, key, good); err != nil {
		t.Fatal(err)
	}
	assertDigest(updated)
	if err := files.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := tree.LogicalBuckets(ctx, updated, changes); !errors.Is(err, ErrPartitionCorrupt) {
		t.Fatalf("warm digest hid a missing leaf: %v", err)
	}
}
