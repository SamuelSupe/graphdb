package partition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type GraphGC struct {
	ID          string    `json:"id"`
	BaseVersion int64     `json:"base_version"`
	Phase       string    `json:"phase"`
	Versions    []int64   `json:"versions"`
	Cursor      string    `json:"cursor,omitempty"`
	Started     [256]bool `json:"started"`
	Done        [256]bool `json:"done"`
}

func (gc *GraphGC) BlocksWrites() bool { return gc != nil && gc.Phase != "running" }
func (gc *GraphGC) Keeps(version int64) bool {
	for _, v := range gc.Versions {
		if v == version {
			return true
		}
	}
	return false
}

func (s Store) applyGraphGC(ctx context.Context, g Graph, a Action) (any, error) {
	if a.Operation == "graph_gc_begin" {
		if g.State != "active" || g.Active != nil || g.Move != nil || g.GC != nil || !validTokenID(a.Txn) || a.At.IsZero() {
			return nil, conflict("graph is not ready for GC")
		}
		versions := map[int64]bool{g.Version: true}
		if err := s.walkObjects(ctx, s.pinPrefix(g.Tenant, g.Generation), func(object storage.ObjectInfo) error {
			var pin Pin
			if err := s.load(ctx, object.Key, &pin); err != nil {
				return err
			}
			if !a.At.Before(pin.KeepUntil) {
				return s.Objects.Delete(ctx, object.Key)
			}
			if pin.Generation != g.Generation {
				return fmt.Errorf("read pin generation mismatch")
			}
			versions[pin.Version] = true
			return nil
		}); err != nil {
			return nil, err
		}
		gc := &GraphGC{ID: a.Txn, BaseVersion: g.Version, Phase: "prune"}
		for version := range versions {
			gc.Versions = append(gc.Versions, version)
		}
		sort.Slice(gc.Versions, func(i, j int) bool { return gc.Versions[i] < gc.Versions[j] })
		g.GC = gc
	} else {
		gc := g.GC
		if gc == nil || gc.ID != a.Txn {
			return nil, conflict("graph GC identity changed")
		}
		switch a.Operation {
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
				if !gc.Keeps(version) {
					if err := s.Objects.Delete(ctx, object.Key); err != nil {
						return nil, err
					}
				}
			}
			gc.Cursor = next
			if next == "" {
				gc.Phase = "starting"
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
			}
		case "graph_gc_done":
			if gc.Phase != "running" {
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
	roots := make([]Root, 0, len(g.GC.Versions))
	for _, version := range g.GC.Versions {
		view, err := s.Snapshot(ctx, tenant, g.Generation, version)
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("protected graph view is missing")
		}
		if err != nil {
			return nil, err
		}
		roots = append(roots, view.Roots[partition])
	}
	return roots, nil
}
