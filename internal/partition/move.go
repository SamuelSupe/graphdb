package partition

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type Move struct {
	ID          string    `json:"id"`
	Partition   uint8     `json:"partition"`
	Source      Group     `json:"source"`
	Target      Group     `json:"target"`
	Epoch       uint64    `json:"epoch"`
	Sequence    uint64    `json:"sequence"`
	Root        Root      `json:"root"`
	Phase       string    `json:"phase"`
	Cursor      string    `json:"cursor,omitempty"`
	RetireAfter time.Time `json:"retire_after,omitempty"`
}

func (m *Move) BlocksWrites() bool { return m != nil && m.Phase != "copy" && m.Phase != "cleanup" }

type PageObject struct {
	Hash string `json:"hash"`
	Data []byte `json:"data"`
}
type PageExport struct {
	Objects []PageObject `json:"objects"`
	Next    string       `json:"next,omitempty"`
}

// ExportPages uses content-addressed objects. The final pass runs behind the
// source fence and closes any holes caused by writes during the first pass.
func (s Store) ExportPages(ctx context.Context, a Action) (PageExport, error) {
	if storage.ValidateTenantID(a.Tenant) != nil || a.Generation <= 0 || len(a.Cursor) > 1024 {
		return PageExport{}, invalid("invalid partition export scope")
	}
	state, err := s.Data(ctx, a.Tenant, a.Generation, a.Partition)
	if err != nil {
		return PageExport{}, err
	}
	if state.Epoch != a.Epoch || state.State != "active" && state.State != "frozen" {
		return PageExport{}, conflict("partition export ownership changed")
	}
	prefix := s.Tree(a.Tenant, a.Generation, a.Partition).Prefix + "/"
	if a.Purpose == "delta" {
		return s.exportDeltaPages(ctx, state, a)
	}
	if a.Cursor != "" && !strings.HasPrefix(a.Cursor, prefix) {
		return PageExport{}, invalid("partition export cursor scope mismatch")
	}
	paged, ok := s.Objects.(interface {
		ListPage(context.Context, string, string, int) ([]storage.ObjectInfo, string, error)
	})
	var objects []storage.ObjectInfo
	next := ""
	if ok {
		objects, next, err = paged.ListPage(ctx, prefix, a.Cursor, 4)
	} else {
		err = s.walkObjects(ctx, prefix, func(object storage.ObjectInfo) error {
			if object.Key > a.Cursor {
				objects = append(objects, object)
			}
			return nil
		})
		if len(objects) > 4 {
			objects = objects[:4]
			next = objects[len(objects)-1].Key
		}
	}
	if err != nil {
		return PageExport{}, err
	}
	out := PageExport{}
	bytes := 0
	for _, object := range objects {
		pageKey := object.Key
		if a.Purpose == "delta" {
			pageKey = s.Tree(a.Tenant, a.Generation, a.Partition).pageKey(path.Base(object.Key))
		}
		data, err := s.Objects.Get(ctx, pageKey)
		if err != nil {
			return out, err
		}
		if err := verifyObject(object.Key, data); err != nil {
			return out, err
		}
		if bytes+len(data) > 4<<20 && len(out.Objects) > 0 {
			out.Next = objects[len(out.Objects)-1].Key
			return out, nil
		}
		out.Objects = append(out.Objects, PageObject{Hash: path.Base(object.Key), Data: data})
		bytes += len(data)
	}
	if next != "" && len(objects) > 0 {
		out.Next = objects[len(objects)-1].Key
	}
	return out, nil
}

func (s Store) applyDataMove(ctx context.Context, a Action) (any, error) {
	state, err := s.Data(ctx, a.Tenant, a.Generation, a.Partition)
	if !validTokenID(a.Txn) || a.Epoch == 0 {
		return nil, invalid("invalid partition move identity")
	}
	if a.Operation == "data_move_reserve" {
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if err == nil {
			if state.Epoch == a.Epoch && (state.State == "importing" || state.State == "retiring" || state.State == "retired" || state.State == "active") && state.LastTxn == a.Txn {
				return state, nil
			}
			if state.Epoch >= a.Epoch || state.State != "retired" {
				return nil, conflict("move destination is already owned")
			}
		}
		state = DataState{Tenant: a.Tenant, Generation: a.Generation, Partition: a.Partition, Epoch: a.Epoch, State: "importing", LastTxn: a.Txn}
	} else {
		if err != nil {
			return nil, err
		}
		if a.Operation == "data_move_thaw" && state.State == "active" && state.LastTxn == a.Txn && state.Epoch == a.Epoch+2 {
			return state, nil
		}
		if state.Epoch != a.Epoch {
			return nil, conflict("partition move epoch changed")
		}
		switch a.Operation {
		case "data_move_capture":
			if state.State != "active" || state.Capture != "" && state.Capture != a.Txn {
				return nil, conflict("partition is already capturing another move")
			}
			state.Capture = a.Txn
		case "data_move_page":
			if state.State != "importing" || state.LastTxn != a.Txn || len(a.Data) > 2*MaxValueBytes {
				return nil, conflict("partition is not importing this move")
			}
			if err := verifyObject(a.Digest, a.Data); err != nil {
				return nil, invalid(err.Error())
			}
			key := s.Tree(a.Tenant, a.Generation, a.Partition).pageKey(a.Digest)
			_, err := s.Objects.PutConditional(ctx, key, a.Data, storage.PutCondition{IfNoneMatch: true})
			if errors.Is(err, storage.ErrConflict) {
				previous, readErr := s.Objects.Get(ctx, key)
				if readErr != nil {
					return nil, readErr
				}
				if string(previous) != string(a.Data) {
					return nil, fmt.Errorf("immutable imported partition page changed")
				}
				err = nil
			}
			return map[string]bool{"staged": err == nil}, err
		case "data_move_fence":
			if state.State == "frozen" && state.LastTxn == a.Txn && state.Sequence == a.Sequence && state.Root == a.Base {
				return state, nil
			}
			if state.State != "active" || state.Prepared != nil || state.Root != a.Base || state.Sequence >= a.Sequence {
				return nil, conflict("resolve source transactions before move fence")
			}
			state.State = "frozen"
			state.LastTxn = a.Txn
			state.Sequence = a.Sequence
		case "data_move_install":
			if state.State == "active" && state.LastTxn == a.Txn && state.Root == a.Base && state.Sequence == a.Sequence {
				return state, nil
			}
			if state.State != "importing" || state.LastTxn != a.Txn || a.Sequence == 0 {
				return nil, conflict("destination installation changed")
			}
			if err := s.Tree(a.Tenant, a.Generation, a.Partition).Scan(ctx, a.Base, "", "", func(Row) (bool, error) { return true, nil }); err != nil {
				return nil, err
			}
			state.Root = a.Base
			state.Sequence = a.Sequence
			state.State = "active"
		case "data_move_thaw":
			// Cancellation always installs a fresh epoch, including cancellation before
			// the source fence. Neither an old writer nor a staged target can reappear.
			if state.State == "active" && state.LastTxn == a.Txn && state.Epoch == a.Epoch+2 {
				return state, nil
			}
			if state.Prepared != nil || state.State != "active" && !(state.State == "frozen" && state.LastTxn == a.Txn) || a.Epoch > math.MaxUint64-2 {
				return nil, conflict("source cannot cancel this move")
			}
			state.Capture = ""
			state.CaptureRoot = Root{}
			state.Epoch += 2
			state.Sequence = a.Sequence
			state.LastTxn = a.Txn
			state.State = "active"
		case "data_move_retire":
			if state.State == "retired" && state.LastTxn == a.Txn {
				return state, nil
			}
			if state.Prepared != nil || state.LastTxn != a.Txn || state.State != "frozen" && state.State != "importing" && state.State != "active" && state.State != "retiring" {
				return nil, conflict("partition cannot retire this move")
			}
			state.State = "retiring"
			state.Root = Root{}
			state.Capture = ""
			state.CaptureRoot = Root{}
			for _, prefix := range []string{s.Tree(a.Tenant, a.Generation, a.Partition).Prefix + "/", path.Join(s.namespace(a.Tenant, a.Generation), fmt.Sprintf("partitions/%02x/moves", a.Partition)) + "/"} {
				var objects []storage.ObjectInfo
				if paged, ok := s.Objects.(interface {
					ListPage(context.Context, string, string, int) ([]storage.ObjectInfo, string, error)
				}); ok {
					objects, _, err = paged.ListPage(ctx, prefix, "", 4)
				} else {
					objects, err = s.Objects.List(ctx, prefix)
					if len(objects) > 4 {
						objects = objects[:4]
					}
				}
				if err != nil {
					return nil, err
				}
				for _, object := range objects {
					if err := s.Objects.Delete(ctx, object.Key); err != nil {
						return nil, err
					}
				}
				if len(objects) > 0 {
					return state, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
				}
			}
			state.State = "retired"
		default:
			return nil, invalid("unknown partition move data action")
		}
	}
	return state, s.save(ctx, s.dataKey(a.Tenant, a.Generation, a.Partition), state)
}

func (s Store) applyGraphMove(ctx context.Context, g Graph, a Action) (any, error) {
	if a.Operation == "graph_move_begin" {
		if g.State != "active" || g.Active != nil || g.GC != nil || g.Move != nil || a.Target == nil || !validTokenID(a.Txn) || a.Target.ID == "" || a.Target.ID == g.Partitions[a.Partition].Group || g.Partitions[a.Partition].Epoch > math.MaxUint64-2 {
			return nil, conflict("graph is not ready for this partition move")
		}
		source, err := g.GroupFor(a.Partition)
		if err != nil {
			return nil, err
		}
		g.Move = &Move{ID: a.Txn, Partition: a.Partition, Source: source, Target: *a.Target, Epoch: g.Partitions[a.Partition].Epoch + 1, Phase: "capture"}
	} else {
		m := g.Move
		if m == nil || m.ID != a.Txn || a.Phase != m.Phase || a.ExpectedCursor != m.Cursor {
			return nil, conflict("graph partition move checkpoint changed")
		}
		switch a.Operation {
		case "graph_move_progress":
			if a.Purpose != m.Phase && !(m.Phase == "capture" && a.Purpose == "copy") && !(m.Phase == "copy" && a.Purpose == "fence") && !(m.Phase == "fence" && a.Purpose == "final_copy") && !(m.Phase == "final_copy" && a.Purpose == "install") {
				return nil, invalid("invalid graph move phase transition")
			}
			if len(a.Cursor) > 1024 {
				return nil, invalid("graph move cursor exceeds its budget")
			}
			if a.Purpose == "fence" && m.Phase == "copy" {
				if g.Active != nil || g.Sequence == math.MaxUint64 {
					return nil, conflict("finish graph transaction before move fence")
				}
				g.Sequence++
				m.Sequence = g.Sequence
				m.Root = g.Roots[m.Partition]
			}
			m.Phase = a.Purpose
			m.Cursor = a.Cursor
		case "graph_move_switch":
			receipt := a.DataState
			if m.Phase != "install" || g.Active != nil || receipt == nil || receipt.Tenant != g.Tenant || receipt.Generation != g.Generation || receipt.Partition != m.Partition || receipt.Epoch != m.Epoch || receipt.State != "active" || receipt.LastTxn != m.ID || receipt.Sequence != m.Sequence || receipt.Root != m.Root || g.Roots[m.Partition] != m.Root || a.At.IsZero() {
				return nil, conflict("move target is not installed at the fenced boundary")
			}
			g.Groups[m.Target.ID] = m.Target
			g.Partitions[m.Partition] = Placement{Group: m.Target.ID, Epoch: m.Epoch}
			g.LayoutRevision++
			m.Phase = "cleanup"
			m.Cursor = ""
			m.RetireAfter = a.At.Add(MaxQueryLifetime)
		case "graph_move_cancel":
			if m.Phase == "cleanup" || g.Active != nil || g.Sequence == math.MaxUint64 {
				return nil, conflict("move cannot cancel after cutover or during a transaction")
			}
			if m.Phase != "cancel" {
				g.Sequence++
				m.Sequence = g.Sequence
				m.Phase = "cancel"
				m.Cursor = ""
			}
		case "graph_move_cancelled":
			receipt := a.DataState
			if m.Phase != "cancel" || receipt == nil || receipt.State != "active" || receipt.Tenant != g.Tenant || receipt.Generation != g.Generation || receipt.Partition != m.Partition || receipt.LastTxn != m.ID || receipt.Epoch != m.Epoch+1 || receipt.Root != g.Roots[m.Partition] || receipt.Sequence != m.Sequence {
				return nil, conflict("source has not restored a fresh move epoch")
			}
			g.Partitions[m.Partition] = Placement{Group: m.Source.ID, Epoch: receipt.Epoch}
			g.LayoutRevision++
			g.Move = nil
		case "graph_move_complete":
			if m.Phase != "cleanup" || a.At.Before(m.RetireAfter) {
				return nil, conflict("partition move still protects in-flight reads")
			}
			g.Move = nil
		default:
			return nil, invalid("unknown graph partition move action")
		}
	}
	return g, s.save(ctx, s.graphKey(g.Tenant), g)
}

func (s Store) deltaTree(tenant string, generation int64, id uint8, move string) Tree {
	return Tree{Objects: s.Objects, Prefix: path.Join(s.namespace(tenant, generation), fmt.Sprintf("partitions/%02x/moves/%s/delta_tree", id, move))}
}

func (s Store) exportDeltaPages(ctx context.Context, state DataState, a Action) (PageExport, error) {
	if !validTokenID(a.Txn) || state.Capture != a.Txn || a.Cursor != "" && !validDigest(a.Cursor) {
		return PageExport{}, conflict("move delta capture or cursor changed")
	}
	out := PageExport{}
	bytes := 0
	err := s.deltaTree(a.Tenant, a.Generation, a.Partition, a.Txn).Scan(ctx, state.CaptureRoot, "", a.Cursor, func(row Row) (bool, error) {
		if !validDigest(row.Key) {
			return false, fmt.Errorf("invalid captured page digest")
		}
		data, err := s.Objects.Get(ctx, s.Tree(a.Tenant, a.Generation, a.Partition).pageKey(row.Key))
		if err != nil {
			return false, err
		}
		if err := verifyObject(row.Key, data); err != nil {
			return false, err
		}
		if len(out.Objects) >= 4 || bytes+len(data) > 4<<20 && len(out.Objects) > 0 {
			out.Next = row.Key
			return false, nil
		}
		out.Objects = append(out.Objects, PageObject{Hash: row.Key, Data: data})
		bytes += len(data)
		return true, nil
	})
	return out, err
}
