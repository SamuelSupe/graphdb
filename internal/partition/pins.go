package partition

import (
	"context"
	"crypto/sha256"
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
	g.ReadLease = nil
	g.HashBuckets = [1024]string{}
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

// One lease protects all concurrent readers of a published version. Its
// query-independent ID cannot collide across tenant generations.
func leaseID(g Graph) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%d", g.Tenant, g.Generation, g.Version)))
	return fmt.Sprintf("%x", sum[:16])
}

func (s Store) createPin(ctx context.Context, g Graph, a Action) (PinnedView, error) {
	shared := a.Purpose == "shared"
	if shared && a.Txn == "" {
		// The first lease is granted on the version current at apply time,
		// even if commits advanced since the router fetched its descriptor.
		a.BaseVersion = g.Version
		a.Txn = leaseID(g)
	}
	if g.State != "active" || !validTokenID(a.Txn) || a.At.IsZero() ||
		(!shared && !validDigest(a.Digest)) || (shared && a.Digest != "") ||
		a.BaseVersion < 0 || a.BaseVersion > g.Version {
		return PinnedView{}, invalid("invalid graph read pin")
	}
	key := s.pinKey(g.Tenant, g.Generation, a.Txn)
	var pin Pin
	err := s.load(ctx, key, &pin)
	switch {
	case err == nil:
		if pin.Query != a.Digest || pin.Version != a.BaseVersion || pin.Generation != g.Generation {
			return PinnedView{}, conflict("read pin identity was reused")
		}
		if (!a.At.Before(pin.ExpiresAt) || !pin.KeepUntil.After(g.HistoryCutoff)) && a.BaseVersion != g.Version {
			return PinnedView{}, invalid("graph cursor expired")
		}
		if pin.KeepUntil.After(g.HistoryCutoff) && a.At.Add(MaxQueryLifetime).Before(pin.ExpiresAt) {
			return s.Pinned(ctx, g.Tenant, pin.ID, pin.Query, a.At)
		}
	case errors.Is(err, storage.ErrNotFound):
		if a.BaseVersion != g.Version {
			return PinnedView{}, ErrVersionNotReady
		}
		if shared && a.Txn != leaseID(g) {
			return PinnedView{}, invalid("invalid graph read lease identity")
		}
		// Older routers can still create private pins during an upgrade.
		// Limit those without loading their bodies or blocking shared leases.
		if !shared {
			keys, _, err := s.objectPage(ctx, s.pinPrefix(g.Tenant, g.Generation), "", 4096)
			if err != nil {
				return PinnedView{}, err
			}
			if len(keys) == 4096 {
				return PinnedView{}, ErrReadPinLimit
			}
		}
		if err := s.saveView(ctx, g); err != nil {
			return PinnedView{}, err
		}
		pin = Pin{ID: a.Txn, Generation: g.Generation, Version: a.BaseVersion, Query: a.Digest}
	default:
		return PinnedView{}, err
	}
	if g.GC != nil && g.GC.Phase != "collect" && a.BaseVersion <= g.GC.BaseVersion && !g.GC.Keeps(a.BaseVersion) {
		return PinnedView{}, ErrVersionNotReady
	}
	pin.ExpiresAt = a.At.Add(CursorLifetime)
	pin.KeepUntil = pin.ExpiresAt.Add(MaxQueryLifetime)
	if err := s.save(ctx, key, pin); err != nil {
		return PinnedView{}, err
	}
	updateGraph := shared && g.Version == pin.Version
	if updateGraph {
		g.ReadLease = &pin
	}
	if g.GC != nil && g.GC.Phase == "collect" && !g.GC.Keeps(pin.Version) {
		g.GC.keep(pin.Version)
		updateGraph = true
	}
	if updateGraph {
		if err := s.save(ctx, s.graphKey(g.Tenant), g); err != nil {
			return PinnedView{}, err
		}
	}
	return s.Pinned(ctx, g.Tenant, pin.ID, pin.Query, a.At)
}

var ErrReadPinLimit = fmt.Errorf("%w: read pin budget is exhausted", ErrGraphBusy)

// AcquireReadView avoids a Raft write when the current descriptor already
// carries a lease with enough time to complete a query and issue its cursor.
func AcquireReadView(ctx context.Context, remote Remote, home Group, g Graph) (PinnedView, error) {
	if pin := g.ReadLease; pin != nil && pin.Version == g.Version && pin.Generation == g.Generation &&
		pin.KeepUntil.After(g.HistoryCutoff) && time.Now().Add(MaxQueryLifetime).Before(pin.ExpiresAt) {
		return PinnedView{Pin: *pin, Graph: g}, nil
	}
	var result PinnedView
	err := remote.Action(ctx, home, Action{Operation: "pin", Revision: ActionRevision, Purpose: "shared", Tenant: g.Tenant,
		Generation: g.Generation, At: time.Now().UTC()}, &result)
	return result, err
}

// createLegacyPin is used only when replaying the original protocol-5 log.
func (s Store) createLegacyPin(ctx context.Context, g Graph, a Action) (PinnedView, error) {
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
	if storage.ValidateTenantID(tenant) != nil || !validTokenID(id) || query != "" && !validDigest(query) {
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
	if pin.Generation != g.Generation || pin.Query != query || !pin.KeepUntil.After(g.HistoryCutoff) || !now.Before(pin.ExpiresAt) {
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
