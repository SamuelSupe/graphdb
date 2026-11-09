package partition

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// MoveRoots is called during capture, before the home group admits writes.
// New pins can only refer to the current root. Old pins can renew only while
// they are still alive, so this plan includes every view eligible to resume.
func (s Store) MoveRoots(ctx context.Context, g Graph, id uint8, now time.Time) ([]Root, error) {
	roots := []Root{g.Roots[id]}
	seenRoots := map[Root]bool{g.Roots[id]: true}
	seenVersions := map[int64]bool{g.Version: true}
	err := s.walkObjects(ctx, s.pinPrefix(g.Tenant, g.Generation), func(object storage.ObjectInfo) error {
		var pin Pin
		if err := s.load(ctx, object.Key, &pin); err != nil {
			return err
		}
		if !now.Before(pin.KeepUntil) || seenVersions[pin.Version] {
			return nil
		}
		view, err := s.Snapshot(ctx, g.Tenant, g.Generation, pin.Version)
		if err != nil {
			return err
		}
		seenVersions[pin.Version] = true
		root := view.Roots[id]
		if root.Hash != "" && !seenRoots[root] {
			roots = append(roots, root)
			seenRoots[root] = true
		}
		return nil
	})
	return roots, err
}

type movePageCursor struct {
	Root int   `json:"root"`
	Path []int `json:"path"`
}

// The cursor names the next DFS path, so each batch seeks through its ancestor
// chain without re-listing the partition or scanning discarded versions.
func (s Store) exportReachablePages(ctx context.Context, state DataState, raw string) (PageExport, error) {
	var cursor movePageCursor
	if raw != "" {
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.Root < 0 ||
			cursor.Root >= len(state.CaptureViews) || len(cursor.Path) > 32 {
			return PageExport{}, invalid("invalid reachable move cursor")
		}
	}
	tree := s.Tree(state.Tenant, state.Generation, state.Partition)
	out := PageExport{}
	bytes := 0
	seen := map[string]bool{}
	for index := cursor.Root; index < len(state.CaptureViews); index++ {
		var visit func(Root, []int, []int) error
		visit = func(root Root, path, seek []int) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if root == (Root{}) && len(path) == 0 && len(seek) == 0 {
				return nil
			}
			if len(path) > 32 {
				return ErrPartitionCorrupt
			}
			page, err := tree.load(ctx, root)
			if err != nil {
				return err
			}
			if len(seek) == 0 {
				if seen[root.Hash] {
					return nil
				}
				data, err := tree.Objects.Get(ctx, tree.pageKey(root.Hash))
				if err != nil {
					return err
				}
				if len(out.Objects) >= moveBatchPages || bytes+len(data) > moveBatchBytes {
					next, _ := json.Marshal(movePageCursor{Root: index, Path: path})
					out.Next = base64.RawURLEncoding.EncodeToString(next)
					return nil
				}
				out.Objects = append(out.Objects, PageObject{Hash: root.Hash, Data: data})
				bytes += len(data)
			}
			start := 0
			var childSeek []int
			if len(seek) != 0 {
				start, childSeek = seek[0], seek[1:]
				if start < 0 || start >= len(page.Children) {
					return invalid("invalid reachable move cursor path")
				}
			}
			for i := start; i < len(page.Children); i++ {
				if err := visit(page.Children[i], append(path, i), childSeek); err != nil {
					return err
				}
				if out.Next != "" {
					return nil
				}
				childSeek = nil
			}
			seen[root.Hash] = true
			return nil
		}
		if err := visit(state.CaptureViews[index], nil, cursor.Path); err != nil {
			return PageExport{}, err
		}
		if out.Next != "" {
			break
		}
		cursor.Path = nil
	}
	return out, nil
}
