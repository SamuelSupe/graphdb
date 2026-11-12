package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type DataGC struct {
	ID       string   `json:"id"`
	Phase    string   `json:"phase"`
	Stack    []Root   `json:"stack,omitempty"`
	Marked   Root     `json:"marked"`
	Seen     Root     `json:"seen"`
	Cursor   string   `json:"cursor,omitempty"`
	Buffered bool     `json:"buffered,omitempty"`
	Pending  []string `json:"pending,omitempty"`
}

const gcPendingLimit = 128

func (s Store) gcTree(state DataState, family string) Tree {
	return Tree{Objects: s.Objects, Prefix: path.Join(s.namespace(state.Tenant, state.Generation), fmt.Sprintf("partitions/%02x/gc/%s/%s", state.Partition, state.GC.ID, family))}
}

func (t Tree) addHashes(ctx context.Context, root Root, rows []Row) (Root, error) {
	for len(rows) > 0 {
		end := min(len(rows), 512)
		var err error
		root, err = t.Update(ctx, root, rows[:end])
		if err != nil {
			return root, err
		}
		rows = rows[end:]
	}
	return root, nil
}

// A prepare already publishes the partition state. Protect its pages in that
// same write, then amortize immutable mark-tree updates across many prepares.
func (s Store) markGCPages(ctx context.Context, state DataState, rows []Row) error {
	gc := state.GC
	if len(rows) == 0 {
		return nil
	}
	if gc.Buffered {
		for _, row := range rows {
			gc.Pending = append(gc.Pending, row.Key)
		}
		slices.Sort(gc.Pending)
		gc.Pending = slices.Compact(gc.Pending)
		if len(gc.Pending) <= gcPendingLimit {
			return nil
		}
		rows = make([]Row, len(gc.Pending))
		for i, hash := range gc.Pending {
			rows[i] = Row{Key: hash, Value: json.RawMessage(`true`)}
		}
		gc.Pending = nil
	}
	var err error
	gc.Marked, err = s.gcTree(state, "marks").addHashes(ctx, gc.Marked, rows)
	return err
}

func (s Store) recordNewPages(ctx context.Context, state *DataState, rows []Row, revision int) error {
	var err error
	if state.Capture != "" {
		state.CaptureRoot, err = s.deltaTree(state.Tenant, state.Generation, state.Partition, state.Capture).addHashes(ctx, state.CaptureRoot, rows)
		if err != nil {
			return err
		}
	}
	if state.GC != nil && state.GC.Phase != "cleanup" {
		if (state.GC.Buffered || revision >= BatchGCRevision) && state.GC.Phase == "sweep" && state.GC.Cursor != "" {
			// The page keys are ordered by the hash prefix and then the hash.
			// The current sweep will never revisit pages before its cursor.
			cutoff := path.Base(state.GC.Cursor)
			remaining := rows[:0]
			for _, row := range rows {
				if row.Key > cutoff {
					remaining = append(remaining, row)
				}
			}
			rows = remaining
		}
		return s.markGCPages(ctx, *state, rows)
	}
	return nil
}

func (s Store) applyDataGC(ctx context.Context, a Action) (any, error) {
	if !validTokenID(a.Txn) || a.Epoch == 0 {
		return nil, invalid("invalid partition GC scope")
	}
	state, err := s.Data(ctx, a.Tenant, a.Generation, a.Partition)
	if err != nil {
		return nil, err
	}
	return s.applyDataGCState(ctx, a, state)
}

func (s Store) applyDataGCState(ctx context.Context, a Action, state DataState) (any, error) {
	if !validTokenID(a.Txn) || a.Epoch == 0 {
		return nil, invalid("invalid partition GC scope")
	}
	if state.Epoch != a.Epoch || state.State != "active" || state.Capture != "" {
		return nil, conflict("partition GC ownership changed")
	}
	if state.LastGC == a.Txn && state.GC == nil {
		return state, nil
	}
	if a.GCSweep != nil {
		if a.Revision < PlannedGCRevision || a.Operation != "data_gc_step" || a.Purpose == "cancel" {
			return nil, invalid("invalid GC sweep action")
		}
		if state.GC == nil || state.GC.ID != a.Txn || state.GC.Phase != "sweep" || state.GC.Cursor != a.ExpectedCursor {
			return nil, conflict("partition GC sweep checkpoint changed")
		}
	}
	if a.Operation == "data_gc_begin" {
		if state.GC != nil {
			if state.GC.ID == a.Txn {
				return state, nil
			}
			return nil, conflict("partition is already collecting garbage")
		}
		maxRoots := 4097
		if a.Revision != 0 {
			maxRoots = 65536
		}
		if state.Prepared != nil || len(a.Roots) > maxRoots {
			return nil, conflict("resolve partition transaction before GC")
		}
		roots := append(append([]Root(nil), a.Roots...), state.Root)
		state.GC = &DataGC{ID: a.Txn, Phase: "mark", Stack: roots, Buffered: a.Revision >= BufferedGCRevision}
	} else {
		if a.Operation == "data_gc_step" && a.Purpose == "cancel" {
			if state.GC == nil {
				state.LastGC = a.Txn
				return state, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
			}
			if state.GC.ID != a.Txn || state.GC.Phase == "sweep" {
				return nil, conflict("graph GC can no longer cancel this partition")
			}
			state.GC.Phase = "cleanup"
			state.GC.Stack = nil
			state.GC.Marked = Root{}
			state.GC.Seen = Root{}
			state.GC.Pending = nil
		}
		if a.Operation != "data_gc_step" || state.GC == nil || state.GC.ID != a.Txn {
			return nil, conflict("partition GC identity changed")
		}
		gc := state.GC
		switch gc.Phase {
		case "mark":
			seenTree := s.gcTree(state, "seen")
			seenTree.cache = &treeReadCache{}
			dataTree := s.Tree(a.Tenant, a.Generation, a.Partition)
			var marked, seen []Row
			limit := 16
			if gc.Buffered || a.Revision >= BatchGCRevision {
				limit = 32
			}
			for count := 0; len(gc.Stack) > 0 && count < limit; count++ {
				last := len(gc.Stack) - 1
				root := gc.Stack[last]
				gc.Stack = gc.Stack[:last]
				if root.Hash == "" {
					continue
				}
				if _, found, err := seenTree.Get(ctx, gc.Seen, root.Hash); err != nil {
					return nil, err
				} else if found {
					continue
				}
				page, err := dataTree.load(ctx, root)
				if err != nil {
					return nil, err
				}
				gc.Stack = append(gc.Stack, page.Children...)
				row := Row{Key: root.Hash, Value: json.RawMessage(`true`)}
				marked = append(marked, row)
				seen = append(seen, row)
			}
			if err := s.markGCPages(ctx, state, uniqueHashRows(marked)); err != nil {
				return nil, err
			}
			if gc.Buffered && len(gc.Stack) == 0 {
				gc.Seen = Root{}
			} else {
				var err error
				gc.Seen, err = seenTree.addHashes(ctx, gc.Seen, uniqueHashRows(seen))
				if err != nil {
					return nil, err
				}
			}
			if len(gc.Stack) == 0 {
				gc.Phase = "sweep"
			}
		case "sweep":
			prefix := s.Tree(a.Tenant, a.Generation, a.Partition).Prefix + "/"
			limit := 64
			if gc.Buffered || a.Revision >= BatchGCRevision {
				limit = 128
			}
			objects, next, err := s.gcSweepPage(ctx, a, prefix, gc.Cursor, limit)
			if err != nil {
				return nil, err
			}
			bytes := int64(0)
			marks := s.gcTree(state, "marks")
			marks.cache = &treeReadCache{}
			var deletes []string
			for _, object := range objects {
				hash := path.Base(object.Key)
				if !validDigest(hash) {
					return nil, fmt.Errorf("invalid partition page in GC")
				}
				_, marked := slices.BinarySearch(gc.Pending, hash)
				if !marked {
					_, marked, err = marks.Get(ctx, gc.Marked, hash)
					if err != nil {
						return nil, err
					}
				}
				if !marked {
					if bytes+object.Size > 8<<20 && bytes > 0 {
						next = gc.Cursor
						break
					}
					deletes = append(deletes, object.Key)
					bytes += object.Size
				}
				gc.Cursor = object.Key
			}
			if err := s.deleteGCObjects(ctx, deletes); err != nil {
				return nil, err
			}
			if next == "" {
				gc.Phase = "cleanup"
				gc.Marked = Root{}
				gc.Seen = Root{}
				gc.Cursor = ""
				gc.Pending = nil
			} else if gc.Buffered {
				cutoff := path.Base(gc.Cursor)
				for len(gc.Pending) > 0 && gc.Pending[0] <= cutoff {
					gc.Pending = gc.Pending[1:]
				}
			}
		case "cleanup":
			prefix := path.Join(s.namespace(a.Tenant, a.Generation), fmt.Sprintf("partitions/%02x/gc/%s", a.Partition, gc.ID)) + "/"
			limit := 4
			if gc.Buffered || a.Revision >= BatchGCRevision {
				limit = 32
			}
			objects, next, err := s.objectPage(ctx, prefix, "", limit)
			if err != nil {
				return nil, err
			}
			keys := make([]string, len(objects))
			for i, object := range objects {
				keys[i] = object.Key
			}
			if err := s.deleteGCObjects(ctx, keys); err != nil {
				return nil, err
			}
			if len(objects) == 0 || gc.Buffered && next == "" {
				state.LastGC = gc.ID
				state.GC = nil
			}
		default:
			return nil, fmt.Errorf("invalid partition GC phase")
		}
	}
	return state, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
}

func (s Store) deleteGCObjects(ctx context.Context, keys []string) error {
	if err := s.journalObjects(ctx, keys...); err != nil {
		return err
	}
	for _, key := range keys {
		if err := s.Objects.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func uniqueHashRows(rows []Row) []Row {
	out := rows[:0]
	seen := map[string]bool{}
	for _, row := range rows {
		if !seen[row.Key] {
			seen[row.Key] = true
			out = append(out, row)
		}
	}
	return out
}

func (s Store) objectPage(ctx context.Context, prefix, cursor string, limit int) ([]storage.ObjectInfo, string, error) {
	if paged, ok := s.Objects.(interface {
		ListPage(context.Context, string, string, int) ([]storage.ObjectInfo, string, error)
	}); ok {
		return paged.ListPage(ctx, prefix, cursor, limit)
	}
	var objects []storage.ObjectInfo
	if err := s.walkObjects(ctx, prefix, func(object storage.ObjectInfo) error {
		if object.Key > cursor {
			objects = append(objects, object)
		}
		return nil
	}); err != nil {
		return nil, "", err
	}
	if len(objects) > limit {
		objects = objects[:limit]
		return objects, objects[len(objects)-1].Key, nil
	}
	return objects, "", nil
}
