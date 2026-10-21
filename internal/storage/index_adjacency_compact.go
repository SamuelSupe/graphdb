package storage

import (
	"context"
	"encoding/json"
	"sort"
)

// Join the boundary leaves of adjacent subtrees, copying their two ancestor
// paths. This also handles a low-occupancy pair straddling routing nodes.
func (s *TenantStore) joinAdjacencyBoundary(ctx context.Context, tenant, prefix string, a, b AdjacencyRef) ([]AdjacencyRef, bool, error) {
	x, err := s.loadAdjacencyNode(ctx, tenant, a)
	if err != nil {
		return nil, false, err
	}
	y, err := s.loadAdjacencyNode(ctx, tenant, b)
	if err != nil {
		return nil, false, err
	}
	if a.Height == 0 {
		n := adjacencyNode{Codec: adjacencyCodec, Entries: append(append([]adjacencyKey(nil), x.Entries...), y.Entries...)}
		data, err := json.Marshal(n)
		if err != nil {
			return nil, false, err
		}
		if len(n.Entries) > 768 || len(data) > 3*adjacencyLeafBytes/4 {
			return nil, false, nil
		}
		refs, err := s.saveAdjacencyNodes(ctx, prefix, n, 0)
		return refs, err == nil, err
	}
	middle, ok, err := s.joinAdjacencyBoundary(ctx, tenant, prefix, x.Children[len(x.Children)-1], y.Children[0])
	if err != nil || !ok {
		return nil, false, err
	}
	children := append(append(append([]AdjacencyRef(nil), x.Children[:len(x.Children)-1]...), middle...), y.Children[1:]...)
	refs, err := s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: children}, a.Height)
	return refs, err == nil, err
}

func (s *TenantStore) compactAdjacencyPath(ctx context.Context, tenant, prefix string, r, left, right AdjacencyRef) ([]AdjacencyRef, bool, error) {
	n, err := s.loadAdjacencyNode(ctx, tenant, r)
	if err != nil {
		return nil, false, err
	}
	index := func(k adjacencyKey) int {
		i := sort.Search(len(n.Children), func(i int) bool { return k.less(n.Children[i].First) }) - 1
		return max(0, i)
	}
	i, j := index(left.First), index(right.First)
	var next []AdjacencyRef
	var ok bool
	if i == j {
		next, ok, err = s.compactAdjacencyPath(ctx, tenant, prefix, n.Children[i], left, right)
	} else {
		next, ok, err = s.joinAdjacencyBoundary(ctx, tenant, prefix, n.Children[i], n.Children[j])
	}
	if err != nil || !ok {
		return nil, false, err
	}
	children := append(append(append([]AdjacencyRef(nil), n.Children[:i]...), next...), n.Children[j+1:]...)
	refs, err := s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: children}, r.Height)
	return refs, err == nil, err
}

func (s *TenantStore) compactAdjacencyRoot(ctx context.Context, tenant, prefix string, root *AdjacencyRef, skipped map[string]bool) (*AdjacencyRef, bool, bool, error) {
	if root == nil || root.Height == 0 {
		return root, false, true, nil
	}
	var previous, left, right *AdjacencyRef
	var find func(AdjacencyRef) error
	find = func(r AdjacencyRef) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if left != nil {
			return nil
		}
		if r.Height == 0 {
			if previous != nil {
				key := previous.Key + "\x00" + r.Key
				if !skipped[key] && previous.Bytes <= adjacencyLeafBytes && r.Bytes <= adjacencyLeafBytes && previous.Count+r.Count <= 768 && (previous.Count < 384 || r.Count < 384 || previous.Bytes < adjacencyLeafBytes/4 || r.Bytes < adjacencyLeafBytes/4) {
					a, b := *previous, r
					left, right = &a, &b
					return nil
				}
			}
			previous = &r
			return nil
		}
		n, err := s.loadAdjacencyNode(ctx, tenant, r)
		if err != nil {
			return err
		}
		for _, child := range n.Children {
			if err := find(child); err != nil {
				return err
			}
			if left != nil {
				break
			}
		}
		return nil
	}
	if err := find(*root); err != nil {
		return nil, false, false, err
	}
	if left == nil {
		return root, false, true, nil
	}
	refs, ok, err := s.compactAdjacencyPath(ctx, tenant, prefix, *root, *left, *right)
	if err != nil {
		return nil, false, false, err
	}
	if !ok {
		skipped[left.Key+"\x00"+right.Key] = true
		return root, false, false, nil
	}
	for len(refs) > 1 {
		refs, err = s.saveAdjacencyNodes(ctx, prefix, adjacencyNode{Children: refs}, refs[0].Height+1)
		if err != nil {
			return nil, false, false, err
		}
	}
	r := refs[0]
	for r.Height > 0 {
		n, err := s.loadAdjacencyNode(ctx, tenant, r)
		if err != nil {
			return nil, false, false, err
		}
		if len(n.Children) != 1 {
			break
		}
		r = n.Children[0]
	}
	return &r, true, false, nil
}
