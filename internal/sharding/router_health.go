package sharding

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
)

const placementFallbackTTL = 5 * time.Minute

func catalogUnavailable(err error) bool {
	var response *HTTPError
	if errors.As(err, &response) {
		return response.Status >= 500
	}
	return err != nil
}

func (r *Router) catalogRead(ctx context.Context, path string, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.catalogUnavailable.Load() {
		retryAt := r.catalogRetryAt.Load()
		now := time.Now().UnixNano()
		if now < retryAt || !r.catalogRetryAt.CompareAndSwap(retryAt, now+int64(time.Second)) {
			return &HTTPError{Status: http.StatusServiceUnavailable, Body: "catalog is temporarily unavailable"}
		}
	}
	probe, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := r.Client.JSON(probe, r.Catalog, http.MethodGet, path, nil, result)
	r.catalogUnavailable.Store(catalogUnavailable(err))
	if catalogUnavailable(err) {
		r.catalogRetryAt.Store(time.Now().Add(time.Second).UnixNano())
	} else {
		r.catalogRetryAt.Store(0)
		if err == nil {
			r.catalogLastSuccess.Store(time.Now().UnixNano())
		}
		var response *HTTPError
		if errors.As(err, &response) && (response.Status == http.StatusUnauthorized || response.Status == http.StatusForbidden) {
			r.catalogVerified.Store(false)
		}
	}
	return err
}

func (r *Router) retainedRoutes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	now := time.Now()
	for _, cached := range r.placements {
		if now.Before(cached.retainUntil) {
			count++
		}
	}
	return count
}

func (r *Router) health(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/v1/readiness" && r.draining.Load() {
		http.Error(w, "router is draining", http.StatusServiceUnavailable)
		return
	}
	// Readiness measures whether this router can serve known routes. Data
	// groups still fence every request and require their own Raft majority.
	ready := r.catalogVerified.Load() && r.retainedRoutes() > 0
	if request.URL.Path != "/v1/readiness" || !ready {
		var identity struct {
			Catalog bool `json:"catalog"`
		}
		err := r.catalogRead(request.Context(), "/cluster/identity", &identity)
		if err == nil && !identity.Catalog {
			r.catalogVerified.Store(false)
			err = fmt.Errorf("configured catalog group has the wrong role")
			ready = false
		}
		if err != nil && !(ready && catalogUnavailable(err)) {
			r.writeError(w, err)
			return
		}
		if err == nil {
			r.catalogVerified.Store(true)
		}
	}
	status := "ok"
	if r.catalogUnavailable.Load() {
		status = "degraded"
	}
	r.writeJSON(w, http.StatusOK, map[string]any{"status": status, "deployment": "sharded_raft", "draining": r.draining.Load(), "catalog_unavailable": r.catalogUnavailable.Load(), "retained_routes": r.retainedRoutes(), "build": buildinfo.Current()})
}
