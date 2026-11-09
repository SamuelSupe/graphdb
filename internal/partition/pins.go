package partition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const CursorLifetime = 5 * time.Minute
const MaxQueryLifetime = 2 * time.Minute

type Pin struct {
	ID         string    `json:"id"`
	Generation int64     `json:"generation"`
	Version    int64     `json:"version"`
	Query      string    `json:"query"`
	ExpiresAt  time.Time `json:"expires_at"`
	KeepUntil  time.Time `json:"keep_until"`
}
type PinnedView struct {
	Pin   Pin   `json:"pin"`
	Graph Graph `json:"graph"`
}

func (s Store) viewKey(tenant string, generation, version int64) string {
	return path.Join(s.namespace(tenant, generation), "views", strconv.FormatInt(version, 10)+".json")
}
func (s Store) pinPrefix(tenant string, generation int64) string {
	return path.Join(s.namespace(tenant, generation), "pins") + "/"
}
func (s Store) pinKey(tenant string, generation int64, id string) string {
	return s.pinPrefix(tenant, generation) + id + ".json"
}

func validTokenID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (s Store) saveView(ctx context.Context, g Graph) error {
	g.Active = nil
	g.Move = nil
	g.GC = nil
	g.Copy = nil
	return s.save(ctx, s.viewKey(g.Tenant, g.Generation, g.Version), g)
}

func (s Store) Snapshot(ctx context.Context, tenant string, generation, version int64) (Graph, error) {
	var g Graph
	err := s.load(ctx, s.viewKey(tenant, generation, version), &g)
	if err == nil && (g.Tenant != tenant || g.Generation != generation || g.Version != version || g.Format != FormatVersion) {
		err = fmt.Errorf("graph read view identity mismatch")
	}
	return g, err
}

func (s Store) createPin(ctx context.Context, g Graph, a Action) (PinnedView, error) {
	if g.State != "active" || !validTokenID(a.Txn) || !validDigest(a.Digest) || a.At.IsZero() {
		return PinnedView{}, invalid("invalid graph read pin")
	}
	if g.GC != nil && a.BaseVersion <= g.GC.BaseVersion && !g.GC.Keeps(a.BaseVersion) {
		return PinnedView{}, conflict("graph read view expired during garbage collection; retry at the current version")
	}
	if a.BaseVersion < 0 || a.BaseVersion > g.Version {
		return PinnedView{}, invalid("invalid graph read version")
	}
	var previous Pin
	if err := s.load(ctx, s.pinKey(g.Tenant, g.Generation, a.Txn), &previous); err == nil {
		pin := previous
		if pin.Query != a.Digest || pin.Version != a.BaseVersion {
			return PinnedView{}, conflict("read pin identity was reused")
		}
		return s.Pinned(ctx, g.Tenant, pin.ID, pin.Query, a.At)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return PinnedView{}, err
	}
	keys, err := s.Objects.List(ctx, s.pinPrefix(g.Tenant, g.Generation))
	if err != nil {
		return PinnedView{}, err
	}
	if len(keys) >= 4096 {
		active := 0
		for _, key := range keys {
			var pin Pin
			if err := s.load(ctx, key.Key, &pin); err != nil {
				return PinnedView{}, err
			}
			if a.At.After(pin.KeepUntil) {
				if err := s.Objects.Delete(ctx, key.Key); err != nil {
					return PinnedView{}, err
				}
			} else {
				active++
			}
		}
		if active >= 4096 {
			return PinnedView{}, conflict("graph read pin budget is exhausted")
		}
	}
	view, err := s.Snapshot(ctx, g.Tenant, g.Generation, a.BaseVersion)
	if err != nil {
		return PinnedView{}, err
	}
	pin := Pin{ID: a.Txn, Generation: g.Generation, Version: a.BaseVersion, Query: a.Digest, ExpiresAt: a.At.Add(CursorLifetime), KeepUntil: a.At.Add(CursorLifetime + MaxQueryLifetime)}
	if err := s.save(ctx, s.pinKey(g.Tenant, g.Generation, pin.ID), pin); err != nil {
		return PinnedView{}, err
	}
	view.Partitions = g.Partitions
	view.Groups = g.Groups
	view.State = g.State
	return PinnedView{Pin: pin, Graph: view}, nil
}

func (s Store) Pinned(ctx context.Context, tenant, id, query string, now time.Time) (PinnedView, error) {
	if storage.ValidateTenantID(tenant) != nil || !validTokenID(id) || !validDigest(query) {
		return PinnedView{}, invalid("invalid graph read pin")
	}
	g, err := s.GetGraph(ctx, tenant)
	if err != nil {
		return PinnedView{}, err
	}
	if g.State != "active" {
		return PinnedView{}, conflict("graph is not active")
	}
	var pin Pin
	err = s.load(ctx, s.pinKey(tenant, g.Generation, id), &pin)
	if errors.Is(err, storage.ErrNotFound) {
		return PinnedView{}, invalid("graph cursor expired or tenant generation changed")
	}
	if err != nil {
		return PinnedView{}, err
	}
	if pin.Generation != g.Generation || pin.Query != query || !now.Before(pin.ExpiresAt) {
		return PinnedView{}, invalid("graph cursor expired or does not match the query")
	}
	view, err := s.Snapshot(ctx, tenant, pin.Generation, pin.Version)
	if err != nil {
		return PinnedView{}, err
	}
	view.Partitions = g.Partitions
	view.Groups = g.Groups
	view.State = g.State
	return PinnedView{Pin: pin, Graph: view}, nil
}
