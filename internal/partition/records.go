package partition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type ChangeSet map[uint8]map[string]Row

func decodeRecord(row Row, value any) error { return json.Unmarshal(row.Value, value) }

func (c ChangeSet) Set(bucket uint8, key string, value any) error {
	var data []byte
	var err error
	if value != nil {
		data, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}
	if c[bucket] == nil {
		c[bucket] = map[string]Row{}
	}
	c[bucket][key] = Row{Key: key, Value: data}
	return nil
}
func (c ChangeSet) Rows(bucket uint8) []Row {
	out := make([]Row, 0, len(c[bucket]))
	for _, row := range c[bucket] {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
func (c ChangeSet) Participants() []uint8 {
	ids := make([]uint8, 0, len(c))
	for id := range c {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func EntityKey(id string) string { return Key("entity", id) }
func EdgeKey(id string) string   { return Key("edge", id) }
func ConstraintKey(signature string) string {
	return Key("constraint", fmt.Sprintf("%x", sha256.Sum256([]byte(signature))))
}
func EntityAliasKey(id string) string                { return Key("entity_alias", id) }
func EdgeAliasKey(id string) string                  { return Key("edge_alias", id) }
func EdgeLocatorKey(id string) string                { return Key("edge_locator", id) }
func AdjacencyKey(id, direction, edge string) string { return Key(direction, id, edge) }
func LogicalPrefix(category, bucket int) string {
	return fmt.Sprintf("logical/%d/%02x/", category, bucket)
}
func LogicalBucket(category, bucket int) uint8 { return Bucket(LogicalPrefix(category, bucket)) }

func (c ChangeSet) Logical(category int, id string, value any) error {
	var bucket uint8
	var digest string
	var err error
	if value != nil {
		bucket, digest, err = graph.LogicalItemDigest(category, id, value)
		if err != nil {
			return err
		}
	} else {
		// The logical bucket depends only on the key, so a deletion can use
		// the shared contract without reconstructing a former logical value.
		switch category {
		case 1:
			bucket, _, err = graph.LogicalItemDigest(1, id, graph.Entity{})
		case 3:
			bucket, _, err = graph.LogicalItemDigest(3, id, graph.Edge{})
		default:
			bucket, _, err = graph.LogicalItemDigest(category, id, nil)
		}
		if err != nil {
			return err
		}
	}
	key := LogicalPrefix(category, int(bucket)) + id
	if value == nil {
		return c.Set(LogicalBucket(category, int(bucket)), key, nil)
	}
	return c.Set(LogicalBucket(category, int(bucket)), key, digest)
}

func logicalBucketsForChanges(changes []Row) []int {
	set := map[int]bool{}
	for _, row := range changes {
		parts := strings.SplitN(row.Key, "/", 4)
		if len(parts) != 4 || parts[0] != "logical" {
			continue
		}
		category, err := strconv.Atoi(parts[1])
		if err != nil || category < 0 || category > 3 {
			continue
		}
		bucket, err := strconv.ParseUint(parts[2], 16, 8)
		if err != nil {
			continue
		}
		set[category*256+int(bucket)] = true
	}
	out := make([]int, 0, len(set))
	for i := range set {
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}

func (t Tree) LogicalBuckets(ctx context.Context, root Root, changes []Row) (map[int]string, error) {
	result := map[int]string{}
	for _, index := range logicalBucketsForChanges(changes) {
		h := sha256.New()
		count := 0
		err := t.Scan(ctx, root, LogicalPrefix(index/256, index%256), "", func(row Row) (bool, error) {
			var digest string
			if err := json.Unmarshal(row.Value, &digest); err != nil {
				return false, err
			}
			b, err := hex.DecodeString(digest)
			if err != nil || len(b) != sha256.Size {
				return false, fmt.Errorf("invalid stored logical leaf")
			}
			_, _ = h.Write(b)
			count++
			return true, nil
		})
		if err != nil {
			return nil, err
		}
		if count == 0 {
			result[index] = ""
		} else {
			result[index] = hex.EncodeToString(h.Sum(nil))
		}
	}
	return result, nil
}

// Scan heads retain only identity and kind; global ordering must not fetch the
// full property payload of every partition before it can return one entity.
type entityScanEntry struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}
