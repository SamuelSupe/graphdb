// Package partition stores immutable graph fragments and their replicated
// transaction state. Fragment roots never imply a graph-wide publication.
package partition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const (
	pageEntries    = 64
	pageBytes      = 256 << 10
	MaxValueBytes  = 1 << 20
	MaxChanges     = 4096
	MaxChangeBytes = 8 << 20
)

type Row struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
}

type Root struct {
	Hash  string `json:"hash,omitempty"`
	First string `json:"first,omitempty"`
	Last  string `json:"last,omitempty"`
	Count int64  `json:"count,omitempty"`
}

type treePage struct {
	Rows     []Row  `json:"rows,omitempty"`
	Children []Root `json:"children,omitempty"`
}

// Pages are reused only within one batched read. A process-wide cache would
// need coordination with GC, migration retirement and corruption detection.
type treeReadCache struct {
	pages map[Root]treePage
	bytes int
}

type Tree struct {
	cache   *treeReadCache
	saved   func(context.Context, string) error
	Objects storage.ObjectStore
	Prefix  string
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

func (t Tree) pageKey(hash string) string { return t.Prefix + "/" + hash[:2] + "/" + hash }

func (t Tree) load(ctx context.Context, root Root) (treePage, error) {
	if !validDigest(root.Hash) || root.Count <= 0 || root.First == "" || root.First > root.Last {
		return treePage{}, fmt.Errorf("%w: invalid fragment root", ErrPartitionCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return treePage{}, err
	}
	if t.cache != nil {
		if page, ok := t.cache.pages[root]; ok {
			return page, nil
		}
	}
	data, err := t.Objects.Get(ctx, t.pageKey(root.Hash))
	if errors.Is(err, storage.ErrNotFound) {
		return treePage{}, fmt.Errorf("%w: required fragment page %s is missing", ErrPartitionCorrupt, root.Hash)
	}
	if err != nil {
		return treePage{}, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != root.Hash {
		return treePage{}, fmt.Errorf("%w: fragment page checksum mismatch", ErrPartitionCorrupt)
	}
	var p treePage
	if err := json.Unmarshal(data, &p); err != nil {
		return p, fmt.Errorf("%w: malformed fragment page: %v", ErrPartitionCorrupt, err)
	}
	var count int64
	first, last := "", ""
	if len(p.Children) > 0 && len(p.Rows) > 0 || len(p.Children) > pageEntries || len(p.Rows) > pageEntries {
		return p, fmt.Errorf("%w: invalid fragment page shape", ErrPartitionCorrupt)
	}
	for i, row := range p.Rows {
		if row.Key == "" || len(row.Value) == 0 || !json.Valid(row.Value) || i > 0 && last >= row.Key {
			return p, fmt.Errorf("%w: invalid fragment row ordering", ErrPartitionCorrupt)
		}
		if i == 0 {
			first = row.Key
		}
		last = row.Key
		count++
	}
	for i, child := range p.Children {
		if !validDigest(child.Hash) || child.Count <= 0 || child.First == "" || child.First > child.Last || i > 0 && last >= child.First {
			return p, fmt.Errorf("%w: invalid fragment child ordering", ErrPartitionCorrupt)
		}
		if i == 0 {
			first = child.First
		}
		last = child.Last
		count += child.Count
	}
	if first != root.First || last != root.Last || count != root.Count {
		return p, fmt.Errorf("%w: fragment root metadata mismatch", ErrPartitionCorrupt)
	}
	if t.cache != nil {
		// Encoded bytes bound retained strings/raw values; the extra terms cover
		// decoded slices, root keys and map entries. Stop caching when either cap fills.
		size := len(data) + 64*len(p.Rows) + 96*len(p.Children) + len(root.Hash) + len(root.First) + len(root.Last) + 128
		if len(t.cache.pages) < 128 && t.cache.bytes+size <= 4<<20 {
			if t.cache.pages == nil {
				t.cache.pages = make(map[Root]treePage)
			}
			t.cache.pages[root] = p
			t.cache.bytes += size
		}
	}
	return p, nil
}

func (t Tree) save(ctx context.Context, p treePage) (Root, error) {
	if len(p.Rows) == 0 && len(p.Children) == 0 {
		return Root{}, nil
	}
	data, err := json.Marshal(p)
	if err != nil {
		return Root{}, err
	}
	root := Root{Hash: fmt.Sprintf("%x", sha256.Sum256(data))}
	if len(p.Rows) > 0 {
		root.First, root.Last, root.Count = p.Rows[0].Key, p.Rows[len(p.Rows)-1].Key, int64(len(p.Rows))
	} else {
		root.First, root.Last = p.Children[0].First, p.Children[len(p.Children)-1].Last
		for _, child := range p.Children {
			root.Count += child.Count
		}
	}
	_, err = t.Objects.PutConditional(ctx, t.pageKey(root.Hash), data, storage.PutCondition{IfNoneMatch: true})
	if err == storage.ErrConflict {
		existing, readErr := t.Objects.Get(ctx, t.pageKey(root.Hash))
		if readErr != nil {
			return Root{}, readErr
		}
		if !bytes.Equal(existing, data) {
			return Root{}, fmt.Errorf("immutable fragment page changed")
		}
		err = nil
	}
	if err == nil && t.saved != nil {
		err = t.saved(ctx, root.Hash)
	}
	return root, err
}

func (t Tree) Get(ctx context.Context, root Root, key string) (json.RawMessage, bool, error) {
	for depth := 0; root.Hash != ""; depth++ {
		if depth > 32 {
			return nil, false, fmt.Errorf("%w: fragment tree depth exceeded", ErrPartitionCorrupt)
		}
		if key < root.First || key > root.Last {
			return nil, false, nil
		}
		p, err := t.load(ctx, root)
		if err != nil {
			return nil, false, err
		}
		if len(p.Children) == 0 {
			i := sort.Search(len(p.Rows), func(i int) bool { return p.Rows[i].Key >= key })
			if i < len(p.Rows) && p.Rows[i].Key == key {
				return p.Rows[i].Value, true, nil
			}
			return nil, false, nil
		}
		i := sort.Search(len(p.Children), func(i int) bool { return p.Children[i].Last >= key })
		if i == len(p.Children) {
			return nil, false, nil
		}
		root = p.Children[i]
	}
	return nil, false, nil
}

// Scan visits keys in byte order, including start. Returning false releases
// the current page without decoding the rest of the tree.
func (t Tree) Scan(ctx context.Context, root Root, prefix, start string, visit func(Row) (bool, error)) error {
	var scan func(Root, int) (bool, error)
	scan = func(ref Root, depth int) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if ref.Hash == "" || ref.Last < start || ref.Last < prefix {
			return true, nil
		}
		if depth > 32 {
			return false, fmt.Errorf("%w: fragment tree depth exceeded", ErrPartitionCorrupt)
		}
		if ref.First >= prefix && !strings.HasPrefix(ref.First, prefix) {
			return false, nil
		}
		p, err := t.load(ctx, ref)
		if err != nil {
			return false, err
		}
		for _, child := range p.Children {
			more, err := scan(child, depth+1)
			if err != nil || !more {
				return more, err
			}
		}
		for _, row := range p.Rows {
			if row.Key < start || row.Key < prefix {
				continue
			}
			if !strings.HasPrefix(row.Key, prefix) {
				return false, nil
			}
			more, err := visit(row)
			if err != nil || !more {
				return more, err
			}
		}
		return true, nil
	}
	_, err := scan(root, 0)
	return err
}

// Update writes only paths touched by the batch. Empty values delete keys;
// callers retain the old immutable root until graph publication and GC agree.
func (t Tree) Update(ctx context.Context, root Root, changes []Row) (Root, error) {
	if len(changes) > MaxChanges {
		return Root{}, fmt.Errorf("%w: fragment change count exceeded", storage.ErrInvalidArgument)
	}
	edits := append([]Row(nil), changes...)
	sort.Slice(edits, func(i, j int) bool { return edits[i].Key < edits[j].Key })
	bytes := 0
	for i, row := range edits {
		bytes += len(row.Key) + len(row.Value)
		if row.Key == "" || len(row.Key) > 4096 || len(row.Value) > MaxValueBytes || len(row.Value) > 0 && !json.Valid(row.Value) || i > 0 && row.Key == edits[i-1].Key {
			return Root{}, fmt.Errorf("%w: invalid fragment change", storage.ErrInvalidArgument)
		}
	}
	if bytes > MaxChangeBytes {
		return Root{}, fmt.Errorf("%w: fragment change byte budget exceeded", storage.ErrInvalidArgument)
	}
	if len(edits) == 0 {
		return root, nil
	}
	refs, err := t.update(ctx, root, edits, 0)
	if err != nil {
		return Root{}, err
	}
	for len(refs) > 1 {
		refs, err = t.saveBranches(ctx, refs)
		if err != nil {
			return Root{}, err
		}
	}
	if len(refs) == 0 {
		return Root{}, nil
	}
	return refs[0], nil
}

func (t Tree) update(ctx context.Context, ref Root, edits []Row, depth int) ([]Root, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if depth > 32 {
		return nil, fmt.Errorf("%w: fragment tree depth exceeded", ErrPartitionCorrupt)
	}
	p := treePage{}
	if ref.Hash != "" {
		var err error
		p, err = t.load(ctx, ref)
		if err != nil {
			return nil, err
		}
	}
	if len(p.Children) > 0 {
		out := make([]Root, 0, len(p.Children)+1)
		at := 0
		for i, child := range p.Children {
			end := len(edits)
			if i+1 < len(p.Children) {
				end = at + sort.Search(len(edits)-at, func(j int) bool { return edits[at+j].Key > child.Last })
			}
			if end == at {
				out = append(out, child)
			} else {
				next, err := t.update(ctx, child, edits[at:end], depth+1)
				if err != nil {
					return nil, err
				}
				out = append(out, next...)
			}
			at = end
		}
		return t.saveBranches(ctx, out)
	}
	rows := make([]Row, 0, len(p.Rows)+len(edits))
	i, j := 0, 0
	for i < len(p.Rows) || j < len(edits) {
		if j == len(edits) || i < len(p.Rows) && p.Rows[i].Key < edits[j].Key {
			rows = append(rows, p.Rows[i])
			i++
			continue
		}
		if i < len(p.Rows) && p.Rows[i].Key == edits[j].Key {
			i++
		}
		if len(edits[j].Value) > 0 {
			rows = append(rows, edits[j])
		}
		j++
	}
	var refs []Root
	for len(rows) > 0 {
		end, total := 0, 0
		for end < len(rows) && end < pageEntries {
			size := len(rows[end].Key) + len(rows[end].Value)
			if end > 0 && total+size > pageBytes {
				break
			}
			total += size
			end++
		}
		next, err := t.save(ctx, treePage{Rows: rows[:end]})
		if err != nil {
			return nil, err
		}
		refs = append(refs, next)
		rows = rows[end:]
	}
	return refs, nil
}

func (t Tree) saveBranches(ctx context.Context, children []Root) ([]Root, error) {
	var refs []Root
	for len(children) > 0 {
		end := min(pageEntries, len(children))
		if len(children) == 1 {
			refs = append(refs, children[0])
			break
		}
		ref, err := t.save(ctx, treePage{Children: children[:end]})
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
		children = children[end:]
	}
	return refs, nil
}
