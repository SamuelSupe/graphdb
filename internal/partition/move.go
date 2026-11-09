package partition

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const moveBatchPages = 32
const moveBatchBytes = 4 << 20

type MoveVerification struct {
	Step     uint64 `json:"step"`
	Root     Root   `json:"root"`
	Sequence uint64 `json:"sequence"`
	Stack    []Root `json:"stack"`
}

type Move struct {
	CaptureAt    time.Time `json:"capture_at,omitempty"`
	FailureSince time.Time `json:"failure_since,omitempty"`
	Error        string    `json:"error,omitempty"`
	ID           string    `json:"id"`
	Partition    uint8     `json:"partition"`
	Source       Group     `json:"source"`
	Target       Group     `json:"target"`
	Epoch        uint64    `json:"epoch"`
	Sequence     uint64    `json:"sequence"`
	Root         Root      `json:"root"`
	Phase        string    `json:"phase"`
	Cursor       string    `json:"cursor,omitempty"`
	RetireAfter  time.Time `json:"retire_after,omitempty"`
}

func (m *Move) BlocksWrites() bool {
	return m != nil && m.Phase != "copy" && m.Phase != "cleanup" && m.Phase != "cancel_cleanup"
}

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
	if len(state.CaptureViews) != 0 {
		if state.Capture != a.Txn || !validTokenID(a.Txn) {
			return PageExport{}, conflict("move capture changed")
		}
		return s.exportReachablePages(ctx, state, a.Cursor)
	}
	if a.Cursor != "" && !strings.HasPrefix(a.Cursor, prefix) {
		return PageExport{}, invalid("partition export cursor scope mismatch")
	}
	objects, next, err := s.objectPage(ctx, prefix, a.Cursor, moveBatchPages)
	if err != nil {
		return PageExport{}, err
	}
	out := PageExport{}
	bytes := 0
	for _, object := range objects {
		data, err := s.Objects.Get(ctx, object.Key)
		if err != nil {
			return out, err
		}
		if err := verifyObject(object.Key, data); err != nil {
			return out, err
		}
		if bytes+len(data) > moveBatchBytes && len(out.Objects) > 0 {
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

// VerifyMoveStep runs on the leader before proposing the next install
// checkpoint. Imported pages are immutable, and apply checks the epoch, move
// and previous checkpoint before accepting the result.
func (s Store) VerifyMoveStep(ctx context.Context, a Action) (*MoveVerification, error) {
	state, err := s.Data(ctx, a.Tenant, a.Generation, a.Partition)
	if err != nil {
		return nil, err
	}
	if state.Epoch == a.Epoch && state.LastTxn == a.Txn && state.State == "active" && state.Root == a.Base && state.Sequence == a.Sequence {
		return nil, nil
	}
	if state.Epoch != a.Epoch || state.LastTxn != a.Txn || state.State != "importing" || a.Sequence == 0 {
		return nil, conflict("destination installation changed")
	}
	verification := state.MoveVerify
	if verification == nil {
		verification = &MoveVerification{Root: a.Base, Sequence: a.Sequence}
		if a.Base != (Root{}) {
			verification.Stack = []Root{a.Base}
		}
	}
	if verification.Root != a.Base || verification.Sequence != a.Sequence {
		return nil, conflict("move verification boundary changed")
	}
	tree := s.Tree(a.Tenant, a.Generation, a.Partition)
	for count := 0; count < 32 && len(verification.Stack) > 0; count++ {
		last := len(verification.Stack) - 1
		root := verification.Stack[last]
		verification.Stack = verification.Stack[:last]
		page, err := tree.load(ctx, root)
		if err != nil {
			return nil, err
		}
		verification.Stack = append(verification.Stack, page.Children...)
	}
	verification.Step++
	return verification, nil
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
			if state.Capture == "" && a.Purpose == "reachable" {
				if len(a.Roots) == 0 || len(a.Roots) > 65536 || a.Roots[0] != state.Root {
					return nil, conflict("move capture roots no longer match the source")
				}
				state.CaptureViews = a.Roots
			}
			state.Capture = a.Txn
		case "data_move_page", "data_move_pages":
			if state.State != "importing" || state.LastTxn != a.Txn {
				return nil, conflict("partition is not importing this move")
			}
			pages := a.Pages
			if a.Operation == "data_move_page" {
				pages = []PageObject{{Hash: a.Digest, Data: a.Data}}
			}
			size := 0
			if len(pages) == 0 || len(pages) > moveBatchPages {
				return nil, invalid("invalid move page batch")
			}
			for _, page := range pages {
				size += len(page.Data)
				if size > moveBatchBytes || len(page.Data) > 2*MaxValueBytes || !validDigest(page.Hash) {
					return nil, invalid("move page batch exceeds its budget")
				}
				if err := verifyObject(page.Hash, page.Data); err != nil {
					return nil, invalid(err.Error())
				}
			}
			tree := s.Tree(a.Tenant, a.Generation, a.Partition)
			// Check existing content before writing any page in the batch.
			for _, page := range pages {
				previous, err := s.Objects.Get(ctx, tree.pageKey(page.Hash))
				if err == nil && !bytes.Equal(previous, page.Data) {
					return nil, fmt.Errorf("%w: immutable imported partition page changed", ErrPartitionCorrupt)
				}
				if err != nil && !errors.Is(err, storage.ErrNotFound) {
					return nil, err
				}
			}
			for _, page := range pages {
				_, err := s.Objects.PutConditional(ctx, tree.pageKey(page.Hash), page.Data, storage.PutCondition{IfNoneMatch: true})
				if err != nil && !errors.Is(err, storage.ErrConflict) {
					return nil, err
				}
			}
			return map[string]bool{"staged": true}, nil
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
			if a.Revision == 0 {
				// Replaying the old install must produce an active replica in
				// one entry, because its subsequent cutover is already logged.
				if err := s.Tree(a.Tenant, a.Generation, a.Partition).Scan(ctx, a.Base, "", "", func(Row) (bool, error) { return true, nil }); err != nil {
					return nil, err
				}
			} else {
				step := uint64(0)
				if state.MoveVerify != nil {
					step = state.MoveVerify.Step
				}
				verification := a.Verify
				if verification == nil || verification.Step != step+1 || verification.Root != a.Base || verification.Sequence != a.Sequence {
					return nil, conflict("move verification checkpoint changed")
				}
				if len(verification.Stack) > 0 {
					state.MoveVerify = verification
					break
				}
				state.MoveVerify = nil
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
			state.CaptureViews = nil
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
			state.MoveVerify = nil
			state.Root = Root{}
			state.Capture = ""
			state.CaptureRoot = Root{}
			state.CaptureViews = nil
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
		if a.Revision != 0 {
			g.Move.CaptureAt = a.At
		}
	} else {
		m := g.Move
		if m == nil || m.ID != a.Txn || a.Phase != m.Phase || a.ExpectedCursor != m.Cursor {
			return nil, conflict("graph partition move checkpoint changed")
		}
		switch a.Operation {
		case "graph_move_error":
			if len(a.Error) > 1024 {
				return nil, invalid("move error exceeds its budget")
			}
			if m.Error == a.Error {
				return g, nil
			}
			m.Error = a.Error
			if a.Error == "" {
				m.FailureSince = time.Time{}
			} else if m.FailureSince.IsZero() {
				m.FailureSince = a.At
			}
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
			m.Error = ""
			m.FailureSince = time.Time{}
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
			m.Error = ""
			m.FailureSince = time.Time{}
			m.Phase = "cleanup"
			if m.CaptureAt.After(g.HistoryCutoff) {
				g.HistoryCutoff = m.CaptureAt
			}
			m.Cursor = ""
			m.RetireAfter = a.At.Add(MaxQueryLifetime)
		case "graph_move_cancel":
			if m.Phase == "cleanup" || m.Phase == "cancel_cleanup" || g.Active != nil || g.Sequence == math.MaxUint64 {
				return nil, conflict("move cannot cancel after cutover or during a transaction")
			}
			if m.Phase != "cancel" {
				g.Sequence++
				m.Sequence = g.Sequence
				m.Error = ""
				m.FailureSince = time.Time{}
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
			if a.Revision == 0 {
				g.Move = nil
				break
			}
			m.Phase = "cancel_cleanup"
			m.Error = ""
			m.FailureSince = time.Time{}
		case "graph_move_complete":
			if m.Phase != "cancel_cleanup" && (m.Phase != "cleanup" || a.At.Before(m.RetireAfter)) {
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
		if len(out.Objects) >= moveBatchPages || bytes+len(data) > moveBatchBytes && len(out.Objects) > 0 {
			out.Next = row.Key
			return false, nil
		}
		out.Objects = append(out.Objects, PageObject{Hash: row.Key, Data: data})
		bytes += len(data)
		return true, nil
	})
	return out, err
}
