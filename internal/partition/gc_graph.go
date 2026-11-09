package partition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type GraphGC struct {
	FenceAt     time.Time `json:"fence_at,omitempty"`
	Error       string    `json:"error,omitempty"`
	ID          string    `json:"id"`
	BaseVersion int64     `json:"base_version"`
	Phase       string    `json:"phase"`
	Versions    []int64   `json:"versions"`
	Cursor      string    `json:"cursor,omitempty"`
	Started     [256]bool `json:"started"`
	Done        [256]bool `json:"done"`
	Collected   bool      `json:"collected,omitempty"`
}

func (gc *GraphGC) BlocksWrites() bool {
	return gc != nil && (gc.Phase == "starting" || gc.Phase == "prune" && !gc.Collected)
}

func (gc *GraphGC) Keeps(version int64) bool {
	_, found := slices.BinarySearch(gc.Versions, version)
	return found
}

func (gc *GraphGC) keep(version int64) {
	i, found := slices.BinarySearch(gc.Versions, version)
	if !found {
		gc.Versions = slices.Insert(gc.Versions, i, version)
	}
}

func (s Store) applyGraphGC(ctx context.Context, g Graph, a Action) (any, error) {
	if a.Operation == "graph_gc_begin" {
		if g.State != "active" || g.Active != nil || g.Move != nil || g.GC != nil || !validTokenID(a.Txn) || a.At.IsZero() {
			return nil, conflict("graph is not ready for GC")
		}
		g.GC = &GraphGC{ID: a.Txn, BaseVersion: g.Version, Phase: "collect"}
		if a.Revision == 0 {
			// Old log entries expect pruning before the mark boundary.
			g.GC.Phase = "prune"
			g.GC.keep(g.Version)
			if err := s.walkObjects(ctx, s.pinPrefix(g.Tenant, g.Generation), func(object storage.ObjectInfo) error {
				var pin Pin
				if err := s.load(ctx, object.Key, &pin); err != nil {
					return err
				}
				if !a.At.Before(pin.KeepUntil) || !pin.KeepUntil.After(g.HistoryCutoff) {
					return s.Objects.Delete(ctx, object.Key)
				}
				if pin.Generation != g.Generation {
					return fmt.Errorf("read pin generation mismatch")
				}
				g.GC.keep(pin.Version)
				return nil
			}); err != nil {
				return nil, err
			}
		}
	} else {
		gc := g.GC
		if gc == nil || gc.ID != a.Txn {
			return nil, conflict("graph GC identity changed")
		}
		switch a.Operation {
		case "graph_gc_collect":
			if gc.Phase != "collect" || gc.Cursor != a.ExpectedCursor || a.At.IsZero() {
				return nil, conflict("graph GC collection checkpoint changed")
			}
			if !gc.Collected {
				pins, next, err := s.objectPage(ctx, s.pinPrefix(g.Tenant, g.Generation), gc.Cursor, 128)
				if err != nil {
					return nil, err
				}
				for _, object := range pins {
					var pin Pin
					if err := s.load(ctx, object.Key, &pin); err != nil {
						return nil, err
					}
					if pin.Generation != g.Generation {
						return nil, fmt.Errorf("read pin generation mismatch")
					}
					if !a.At.Before(pin.KeepUntil) || !pin.KeepUntil.After(g.HistoryCutoff) {
						if err := s.Objects.Delete(ctx, object.Key); err != nil {
							return nil, err
						}
					} else {
						gc.keep(pin.Version)
					}
				}
				gc.Cursor, gc.Collected = next, next == ""
			}
			if gc.Collected && g.Active == nil {
				gc.BaseVersion = g.Version
				gc.keep(g.Version)
				gc.Phase = "starting"
				gc.FenceAt = a.At
			}
		case "graph_gc_cancel_start":
			if gc.Phase != "starting" || gc.FenceAt.IsZero() || a.At.Before(gc.FenceAt.Add(30*time.Second)) || len(a.Error) > 1024 {
				return nil, conflict("graph GC startup is not eligible for cancellation")
			}
			gc.Phase = "cancel"
			gc.Error = a.Error
		case "graph_gc_prune":
			if gc.Phase != "prune" || gc.Cursor != a.ExpectedCursor {
				return nil, conflict("graph GC prune checkpoint changed")
			}
			prefix := path.Join(s.namespace(g.Tenant, g.Generation), "views") + "/"
			objects, next, err := s.objectPage(ctx, prefix, gc.Cursor, 128)
			if err != nil {
				return nil, err
			}
			for _, object := range objects {
				version, err := strconv.ParseInt(strings.TrimSuffix(path.Base(object.Key), ".json"), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid graph read view key")
				}
				if version <= gc.BaseVersion && !gc.Keeps(version) {
					if err := s.Objects.Delete(ctx, object.Key); err != nil {
						return nil, err
					}
				}
			}
			gc.Cursor = next
			if next == "" {
				gc.Phase = "starting" // Resume an old prune-first operation.
				if gc.Collected {
					gc.Phase = "running"
				}
			}
		case "graph_gc_started":
			if gc.Phase != "starting" {
				return nil, conflict("graph GC already started")
			}
			for _, id := range a.Participants {
				gc.Started[id] = true
			}
			all := true
			for _, started := range gc.Started {
				all = all && started
			}
			if all {
				gc.Phase = "running"
				if gc.Collected {
					gc.Phase = "prune"
				}
			}
		case "graph_gc_done":
			if gc.Phase != "running" && gc.Phase != "cancel" {
				return nil, conflict("graph GC is not running")
			}
			for _, id := range a.Participants {
				gc.Done[id] = true
			}
			all := true
			for _, done := range gc.Done {
				all = all && done
			}
			if all {
				g.LastGCAt = a.At
				g.GC = nil
			}
		default:
			return nil, invalid("unknown graph GC operation")
		}
	}
	return g, s.save(ctx, s.graphKey(g.Tenant), g)
}

// GCRoots is called while graph writes are briefly fenced during GC startup.
// Once every data partition has installed its mark state, new pages are added
// to that state by the same replicated prepare that creates them.
func (s Store) GCRoots(ctx context.Context, tenant, id string, partition uint8) ([]Root, error) {
	if storage.ValidateTenantID(tenant) != nil || !validTokenID(id) {
		return nil, invalid("invalid graph GC root scope")
	}
	g, err := s.GetGraph(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if g.GC == nil || g.GC.ID != id || g.GC.Phase != "starting" {
		return nil, conflict("graph GC root plan changed")
	}
	plan, err := s.GCRootPlan(ctx, g, []uint8{partition})
	return plan[partition], err
}

// GCRootPlan reads each immutable view once for a batch of data partitions.
func (s Store) GCRootPlan(ctx context.Context, g Graph, partitions []uint8) (map[uint8][]Root, error) {
	if g.GC == nil || g.GC.Phase != "starting" {
		return nil, conflict("graph GC root plan changed")
	}
	roots := make(map[uint8][]Root, len(partitions))
	seen := make(map[uint8]map[Root]bool, len(partitions))
	for _, id := range partitions {
		seen[id] = make(map[Root]bool)
	}
	for _, version := range g.GC.Versions {
		view := g
		var err error
		if version != g.Version {
			view, err = s.Snapshot(ctx, g.Tenant, g.Generation, version)
		}
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("%w: protected graph view is missing", ErrPartitionCorrupt)
		}
		if err != nil {
			return nil, err
		}
		for _, id := range partitions {
			root := view.Roots[id]
			if root.Hash != "" && !seen[id][root] {
				roots[id] = append(roots[id], root)
				seen[id][root] = true
			}
		}
	}
	return roots, nil
}
