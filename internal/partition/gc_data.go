package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type DataGC struct {
	ID     string `json:"id"`
	Phase  string `json:"phase"`
	Stack  []Root `json:"stack,omitempty"`
	Marked Root   `json:"marked"`
	Seen   Root   `json:"seen"`
	Cursor string `json:"cursor,omitempty"`
}

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

func (s Store) recordNewPages(ctx context.Context, state *DataState, rows []Row) error {
	var err error
	if state.Capture != "" {
		state.CaptureRoot, err = s.deltaTree(state.Tenant, state.Generation, state.Partition, state.Capture).addHashes(ctx, state.CaptureRoot, rows)
		if err != nil {
			return err
		}
	}
	if state.GC != nil && state.GC.Phase != "cleanup" {
		state.GC.Marked, err = s.gcTree(*state, "marks").addHashes(ctx, state.GC.Marked, rows)
		if err != nil {
			return err
		}
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
	if state.Epoch != a.Epoch || state.State != "active" || state.Capture != "" {
		return nil, conflict("partition GC ownership changed")
	}
	if state.LastGC == a.Txn && state.GC == nil {
		return state, nil
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
		state.GC = &DataGC{ID: a.Txn, Phase: "mark", Stack: roots}
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
		}
		if a.Operation != "data_gc_step" || state.GC == nil || state.GC.ID != a.Txn {
			return nil, conflict("partition GC identity changed")
		}
		gc := state.GC
		switch gc.Phase {
		case "mark":
			var marked, seen []Row
			for count := 0; len(gc.Stack) > 0 && count < 16; count++ {
				last := len(gc.Stack) - 1
				root := gc.Stack[last]
				gc.Stack = gc.Stack[:last]
				if root.Hash == "" {
					continue
				}
				if _, found, err := s.gcTree(state, "seen").Get(ctx, gc.Seen, root.Hash); err != nil {
					return nil, err
				} else if found {
					continue
				}
				page, err := s.Tree(a.Tenant, a.Generation, a.Partition).load(ctx, root)
				if err != nil {
					return nil, err
				}
				gc.Stack = append(gc.Stack, page.Children...)
				row := Row{Key: root.Hash, Value: json.RawMessage(`true`)}
				marked = append(marked, row)
				seen = append(seen, row)
			}
			gc.Marked, err = s.gcTree(state, "marks").addHashes(ctx, gc.Marked, uniqueHashRows(marked))
			if err != nil {
				return nil, err
			}
			gc.Seen, err = s.gcTree(state, "seen").addHashes(ctx, gc.Seen, uniqueHashRows(seen))
			if err != nil {
				return nil, err
			}
			if len(gc.Stack) == 0 {
				gc.Phase = "sweep"
			}
		case "sweep":
			prefix := s.Tree(a.Tenant, a.Generation, a.Partition).Prefix + "/"
			objects, next, err := s.objectPage(ctx, prefix, gc.Cursor, 64)
			if err != nil {
				return nil, err
			}
			bytes := int64(0)
			for _, object := range objects {
				hash := path.Base(object.Key)
				if !validDigest(hash) {
					return nil, fmt.Errorf("invalid partition page in GC")
				}
				_, marked, err := s.gcTree(state, "marks").Get(ctx, gc.Marked, hash)
				if err != nil {
					return nil, err
				}
				if !marked {
					if bytes+object.Size > 8<<20 && bytes > 0 {
						next = gc.Cursor
						break
					}
					if err := s.Objects.Delete(ctx, object.Key); err != nil {
						return nil, err
					}
					bytes += object.Size
				}
				gc.Cursor = object.Key
			}
			if next == "" {
				gc.Phase = "cleanup"
				gc.Marked = Root{}
				gc.Seen = Root{}
				gc.Cursor = ""
			}
		case "cleanup":
			prefix := path.Join(s.namespace(a.Tenant, a.Generation), fmt.Sprintf("partitions/%02x/gc/%s", a.Partition, gc.ID)) + "/"
			objects, _, err := s.objectPage(ctx, prefix, "", 4)
			if err != nil {
				return nil, err
			}
			for _, object := range objects {
				if err := s.Objects.Delete(ctx, object.Key); err != nil {
					return nil, err
				}
			}
			if len(objects) == 0 {
				state.LastGC = gc.ID
				state.GC = nil
			}
		default:
			return nil, fmt.Errorf("invalid partition GC phase")
		}
	}
	return state, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
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
