package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
)

const (
	adjacencyCodec       = "adjacency-tuples-v1"
	adjacencyLeafRows    = 1024
	adjacencyLeafBytes   = 256 << 10
	adjacencyBranchRows  = 128
	adjacencyBranchBytes = 64 << 10
)

// Tuple fields are compared separately: IDs may themselves contain separators.
type adjacencyKey struct {
	Endpoint string `json:"endpoint"`
	ID       string `json:"id"`
}

func (a adjacencyKey) less(b adjacencyKey) bool {
	return a.Endpoint < b.Endpoint || a.Endpoint == b.Endpoint && a.ID < b.ID
}

// AdjacencyRef is independent of payload page locations. Splitting or merging
// ID-ordered payload pages therefore never changes adjacency entries.
type AdjacencyRef struct {
	// Relative to the store prefix, so moving a tenant between storage
	// locations preserves child hashes and does not rewrite the whole tree.
	Key    string       `json:"key"`
	Hash   string       `json:"hash"`
	Count  int          `json:"count"`
	Height int          `json:"height"`
	Bytes  int          `json:"bytes"`
	First  adjacencyKey `json:"first"`
	Last   adjacencyKey `json:"last"`
}
type adjacencyNode struct {
	Codec    string         `json:"codec"`
	Entries  []adjacencyKey `json:"entries,omitempty"`
	Children []AdjacencyRef `json:"children,omitempty"`
}
type adjacencyChange struct {
	key    adjacencyKey
	remove bool
}

func adjacencyHash(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func (s *TenantStore) adjacencyPrefix(tenant string, version int64, scope string) string {
	return strings.TrimPrefix(s.parquetVersionPrefix(tenant, version), s.Prefix+"/") + "/adjacency/" + adjacencyHash([]byte(scope)) + "/"
}

func (s *TenantStore) adjacencyObjectKey(ref AdjacencyRef) string {
	return path.Join(s.Prefix, ref.Key)
}
func (s *TenantStore) loadAdjacencyNode(ctx context.Context, tenant string, ref AdjacencyRef) (adjacencyNode, error) {
	key := s.adjacencyObjectKey(ref)
	if ref.Key != path.Clean(ref.Key) || strings.HasPrefix(ref.Key, "/") || !strings.HasPrefix(ref.Key, "tenants/"+tenant+"/indexes/parquet/versions/") || !isAdjacencyObjectKey(ref.Key) {
		return adjacencyNode{}, fmt.Errorf("invalid adjacency object key")
	}
	if err := s.validateTenantObjectKey(tenant, key); err != nil {
		return adjacencyNode{}, err
	}
	n, ok, err := s.adjacencyCache.load(ctx, ref.Key+"\x00"+ref.Hash, func() (adjacencyNode, bool, error) {
		data, _, err := s.Objects.GetWithMeta(ctx, key)
		if err != nil {
			return adjacencyNode{}, false, err
		}
		var node adjacencyNode
		if len(data) != ref.Bytes || adjacencyHash(data) != ref.Hash {
			return node, false, fmt.Errorf("adjacency node checksum mismatch")
		}
		if err = json.Unmarshal(data, &node); err != nil {
			return node, false, err
		}
		if err = validateAdjacencyNode(node, ref); err != nil {
			return node, false, err
		}
		return node, true, nil
	})
	if err == nil && !ok {
		err = fmt.Errorf("adjacency node unavailable")
	}
	// Validate even cache hits against the caller's authenticated routing metadata.
	if err == nil {
		err = validateAdjacencyNode(n, ref)
	}
	return n, err
}
func validateAdjacencyNode(n adjacencyNode, r AdjacencyRef) error {
	bad := func() error { return fmt.Errorf("invalid adjacency tree") }
	if n.Codec != adjacencyCodec || r.Height < 0 || r.Height > 32 || r.Count <= 0 {
		return bad()
	}
	if r.Height == 0 {
		if len(n.Children) != 0 || len(n.Entries) != r.Count || len(n.Entries) == 0 || len(n.Entries) > adjacencyLeafRows || r.Bytes > adjacencyLeafBytes && len(n.Entries) > 1 {
			return bad()
		}
		for i, k := range n.Entries {
			if i > 0 && !n.Entries[i-1].less(k) {
				return bad()
			}
		}
		if n.Entries[0] != r.First || n.Entries[len(n.Entries)-1] != r.Last {
			return bad()
		}
	} else {
		if len(n.Entries) != 0 || len(n.Children) == 0 || len(n.Children) > adjacencyBranchRows || r.Bytes > adjacencyBranchBytes && len(n.Children) > 2 {
			return bad()
		}
		count := 0
		for i, c := range n.Children {
			if c.Height != r.Height-1 || c.Count <= 0 || c.Last.less(c.First) || i > 0 && !n.Children[i-1].Last.less(c.First) {
				return bad()
			}
			count += c.Count
		}
		if count != r.Count || n.Children[0].First != r.First || n.Children[len(n.Children)-1].Last != r.Last {
			return bad()
		}
	}
	return nil
}

func (s *TenantStore) saveAdjacencyNodes(ctx context.Context, prefix string, n adjacencyNode, height int) ([]AdjacencyRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n.Codec = adjacencyCodec
	count, limit, byteLimit, minItems := len(n.Entries), adjacencyLeafRows, adjacencyLeafBytes, 1
	if height > 0 {
		count, limit, byteLimit, minItems = len(n.Children), adjacencyBranchRows, adjacencyBranchBytes, 2
	}
	if count == 0 {
		return nil, nil
	}
	split := func() ([]AdjacencyRef, error) {
		left, right := n, n
		middle := count / 2
		if height == 0 {
			left.Entries, right.Entries = n.Entries[:middle], n.Entries[middle:]
		} else {
			left.Children, right.Children = n.Children[:middle], n.Children[middle:]
		}
		a, err := s.saveAdjacencyNodes(ctx, prefix, left, height)
		if err != nil {
			return nil, err
		}
		b, err := s.saveAdjacencyNodes(ctx, prefix, right, height)
		return append(a, b...), err
	}
	if count > limit {
		return split()
	}
	data, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}
	if len(data) > byteLimit && count > minItems {
		return split()
	}
	r := AdjacencyRef{Hash: adjacencyHash(data), Height: height, Bytes: len(data)}
	if height == 0 {
		r.Count = count
		r.First = n.Entries[0]
		r.Last = n.Entries[count-1]
	} else {
		r.First = n.Children[0].First
		r.Last = n.Children[count-1].Last
		for _, c := range n.Children {
			r.Count += c.Count
		}
	}
	r.Key = prefix + r.Hash + ".adj.json"
	if err := s.Objects.Put(ctx, s.adjacencyObjectKey(r), data); err != nil {
		return nil, err
	}
	return []AdjacencyRef{r}, nil
}

// Changes are sorted and coalesced before descending. A batch rewrites each
// touched leaf and shared ancestor once, rather than once per edge.
func (s *TenantStore) updateAdjacency(ctx context.Context, tenant, prefix string, root *AdjacencyRef, changes []adjacencyChange) (*AdjacencyRef, error) {
	if len(changes) == 0 {
		return root, ctx.Err()
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].key.less(changes[j].key) })
	refs, err := s.editAdjacency(ctx, tenant, prefix, root, changes)
	if err != nil {
		return nil, err
	}
	for len(refs) > 1 {
		refs, err = s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: refs}, refs[0].Height+1)
		if err != nil {
			return nil, err
		}
	}
	if len(refs) == 0 {
		return nil, nil
	}
	r := refs[0]
	for r.Height > 0 {
		node, err := s.loadAdjacencyNode(ctx, tenant, r)
		if err != nil {
			return nil, err
		}
		if len(node.Children) != 1 {
			break
		}
		r = node.Children[0]
	}
	return &r, nil
}
func (s *TenantStore) editAdjacency(ctx context.Context, tenant, prefix string, ref *AdjacencyRef, changes []adjacencyChange) ([]AdjacencyRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n := adjacencyNode{Codec: adjacencyCodec}
	height := 0
	if ref != nil {
		var err error
		n, err = s.loadAdjacencyNode(ctx, tenant, *ref)
		if err != nil {
			return nil, err
		}
		height = ref.Height
	}
	if height == 0 {
		rows := make([]adjacencyKey, 0, len(n.Entries)+len(changes))
		i := 0
		for _, c := range changes {
			for i < len(n.Entries) && n.Entries[i].less(c.key) {
				rows = append(rows, n.Entries[i])
				i++
			}
			if i < len(n.Entries) && n.Entries[i] == c.key {
				i++
			}
			if !c.remove {
				rows = append(rows, c.key)
			}
		}
		rows = append(rows, n.Entries[i:]...)
		return s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Entries: rows}, 0)
	}
	groups := make(map[int][]adjacencyChange)
	for _, c := range changes {
		i := sort.Search(len(n.Children), func(i int) bool { return c.key.less(n.Children[i].First) }) - 1
		if i < 0 {
			i = 0
		}
		groups[i] = append(groups[i], c)
	}
	var children []AdjacencyRef
	var shrink []bool
	for i, c := range n.Children {
		g := groups[i]
		if len(g) == 0 {
			children = append(children, c)
			shrink = append(shrink, false)
			continue
		}
		next, err := s.editAdjacency(ctx, tenant, prefix, &c, g)
		if err != nil {
			return nil, err
		}
		deleted := false
		for _, change := range g {
			deleted = deleted || change.remove
		}
		for _, child := range next {
			children = append(children, child)
			shrink = append(shrink, deleted && len(next) == 1)
		}
	}
	// One local neighbor per shrunken child; never cascade a successful merge.
	attempted := map[string]bool{}
	for i := 0; i+1 < len(children); i++ {
		a, b := children[i], children[i+1]
		if a.Height == 0 && (a.Bytes > adjacencyLeafBytes || b.Bytes > adjacencyLeafBytes) {
			continue
		}
		low := func(r AdjacencyRef) bool {
			if r.Height == 0 {
				return r.Count < 384 || r.Bytes < adjacencyLeafBytes/4
			}
			return r.Bytes < adjacencyBranchBytes/4
		}
		if attempted[a.Key] || attempted[b.Key] || !(shrink[i] && low(a) || shrink[i+1] && low(b)) {
			continue
		}
		attempted[a.Key], attempted[b.Key] = true, true
		x, err := s.loadAdjacencyNode(ctx, tenant, a)
		if err != nil {
			return nil, err
		}
		y, err := s.loadAdjacencyNode(ctx, tenant, b)
		if err != nil {
			return nil, err
		}
		merged := adjacencyNode{Codec: adjacencyCodec, Entries: append(append([]adjacencyKey(nil), x.Entries...), y.Entries...), Children: append(append([]AdjacencyRef(nil), x.Children...), y.Children...)}
		data, err := json.Marshal(merged)
		if err != nil {
			return nil, err
		}
		fits := len(merged.Entries) <= 768 && len(data) <= 3*adjacencyLeafBytes/4
		if a.Height > 0 {
			fits = len(merged.Children) <= 96 && len(data) <= 3*adjacencyBranchBytes/4
		}
		if !fits {
			continue
		}
		refs, err := s.saveAdjacencyNodes(ctx, prefix, merged, a.Height)
		if err != nil {
			return nil, err
		}
		children[i] = refs[0]
		children = append(children[:i+1], children[i+2:]...)
		shrink = append(shrink[:i+1], shrink[i+2:]...)
		shrink[i] = false
	}
	return s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: children}, height)
}

type adjacencyFrame struct {
	node adjacencyNode
	next int
}
type adjacencyIterator struct {
	store    *TenantStore
	tenant   string
	endpoint string
	stack    []adjacencyFrame
	entries  []adjacencyKey
	offset   int
}

func (s *TenantStore) seekAdjacency(ctx context.Context, tenant string, root AdjacencyRef, target adjacencyKey) (*adjacencyIterator, error) {
	it := &adjacencyIterator{store: s, tenant: tenant, endpoint: target.Endpoint}
	if root.Last.less(target) || root.First.Endpoint > target.Endpoint {
		return it, nil
	}
	r := root
	for {
		n, err := s.loadAdjacencyNode(ctx, tenant, r)
		if err != nil {
			return nil, err
		}
		if r.Height == 0 {
			it.entries = n.Entries
			it.offset = sort.Search(len(n.Entries), func(i int) bool { return !n.Entries[i].less(target) })
			return it, nil
		}
		i := sort.Search(len(n.Children), func(i int) bool { return target.less(n.Children[i].First) }) - 1
		if i < 0 {
			i = 0
		}
		it.stack = append(it.stack, adjacencyFrame{node: n, next: i + 1})
		r = n.Children[i]
	}
}
func (it *adjacencyIterator) next(ctx context.Context) (string, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		if it.offset < len(it.entries) {
			k := it.entries[it.offset]
			it.offset++
			if k.Endpoint != it.endpoint {
				return "", false, nil
			}
			return k.ID, true, nil
		}
		if len(it.stack) == 0 {
			return "", false, nil
		}
		f := &it.stack[len(it.stack)-1]
		if f.next == len(f.node.Children) {
			it.stack = it.stack[:len(it.stack)-1]
			continue
		}
		r := f.node.Children[f.next]
		f.next++
		if r.First.Endpoint > it.endpoint {
			return "", false, nil
		}
		for {
			n, err := it.store.loadAdjacencyNode(ctx, it.tenant, r)
			if err != nil {
				return "", false, err
			}
			if r.Height == 0 {
				it.entries = n.Entries
				it.offset = 0
				break
			}
			it.stack = append(it.stack, adjacencyFrame{node: n, next: 1})
			r = n.Children[0]
		}
	}
}
