package sharding

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type Router struct {
	Catalog            Shard
	Client             *Client
	mu                 sync.Mutex
	placements         map[string]cachedPlacement
	draining           atomic.Bool
	metrics            *observability.OperationMetrics
	catalogVerified    atomic.Bool
	catalogUnavailable atomic.Bool
	catalogRetryAt     atomic.Int64
	catalogLastSuccess atomic.Int64
}

type cachedPlacement struct {
	resolution  Resolution
	expires     time.Time
	retainUntil time.Time
}

func NewRouter(catalog Shard, token string) *Router {
	return &Router{Catalog: catalog, Client: NewClient(token), metrics: observability.NewOperationMetrics()}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Minute)
	defer cancel()
	request = request.WithContext(ctx)
	if request.URL.Path == "/metrics" || request.URL.Path == "/v1/diagnostics" {
		r.diagnostics(w, request)
		return
	}
	if request.URL.Path == "/v1/router/drain" || request.URL.Path == "/v1/router/resume" {
		if request.Method != http.MethodPost || subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+r.Client.Token)) != 1 {
			http.Error(w, "router maintenance requires POST and the router token", http.StatusUnauthorized)
			return
		}
		r.draining.Store(request.URL.Path == "/v1/router/drain")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if request.URL.Path == "/v1/cluster" || strings.HasPrefix(request.URL.Path, "/v1/cluster/") {
		r.admin(w, request)
		return
	}
	switch request.URL.Path {
	case "/v1/health", "/v1/readiness":
		r.health(w, request)
		return
	case "/v1/tenants":
		if request.Method == http.MethodGet {
			r.listTenants(w, request)
			return
		}
	case "/openapi.yaml":
		r.proxy(w, request, r.Catalog, 0, "")
		return
	}
	tenant := request.Header.Get("X-Tenant-ID")
	if strings.HasPrefix(request.URL.EscapedPath(), "/v1/tenants/") {
		first := strings.Split(strings.TrimPrefix(request.URL.EscapedPath(), "/v1/tenants/"), "/")[0]
		decoded, err := url.PathUnescape(first)
		if err != nil || (tenant != "" && tenant != decoded) {
			http.Error(w, "tenant header and lifecycle path must match", http.StatusBadRequest)
			return
		}
		tenant = decoded
	}
	create := request.Method == http.MethodPost && request.URL.Path == "/v1/tenants"
	clone := request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/clone")
	var target string
	if create || clone {
		body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, 1<<20))
		if err != nil {
			http.Error(w, "tenant request exceeds its body budget", http.StatusRequestEntityTooLarge)
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		var input struct {
			Tenant string `json:"tenant_id"`
			Target string `json:"target_tenant_id"`
		}
		if json.Unmarshal(body, &input) != nil {
			http.Error(w, "invalid tenant request", http.StatusBadRequest)
			return
		}
		if create {
			tenant = input.Tenant
		} else {
			target = input.Target
		}
	}
	if err := storage.ValidateTenantID(tenant); err != nil {
		http.Error(w, "a valid X-Tenant-ID or tenant lifecycle path is required", http.StatusBadRequest)
		return
	}
	request.Header.Set("X-Tenant-ID", tenant)
	resolution, err := r.resolve(ctx, tenant)
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.Status == http.StatusNotFound && (create || (request.Method == http.MethodPost && (request.URL.Path == "/v1/commits" || request.URL.Path == "/v1/ingest/batches" || request.URL.Path == "/v1/imports"))) {
		resolution, err = r.assign(ctx, tenant, "")
	}
	if err != nil {
		r.writeError(w, err)
		return
	}
	if clone {
		if storage.ValidateTenantID(target) != nil || target == tenant {
			http.Error(w, "a distinct clone target tenant is required", http.StatusBadRequest)
			return
		}
		targetResolution, err := r.assign(ctx, target, resolution.Shard.ID)
		if err != nil {
			r.writeError(w, err)
			return
		}
		request.Header.Set(TargetEpochHeader, fmt.Sprint(targetResolution.Placement.Epoch))
	}
	if strings.HasPrefix(request.URL.EscapedPath(), "/v1/ingest/writers/") {
		parts := strings.Split(strings.TrimPrefix(request.URL.EscapedPath(), "/v1/ingest/writers/"), "/")
		if len(parts) == 5 && parts[1] == "batches" {
			escaped := "/v1/ingest/batches/" + strings.Join(parts[2:], "/")
			request.URL.Path, _ = url.PathUnescape(escaped)
			request.URL.RawPath = escaped
		}
	}
	r.proxy(w, request, resolution.Shard, resolution.Placement.Epoch, tenant)
}

func (r *Router) resolve(ctx context.Context, tenant string) (resolution Resolution, err error) {
	finish := r.metrics.Start("placement_lookup")
	defer func() { finish(err) }()
	r.mu.Lock()
	cached := r.placements[tenant]
	r.mu.Unlock()
	if time.Now().Before(cached.expires) {
		r.metrics.Event("placement_cache_hit")
		return cached.resolution, nil
	}
	r.metrics.Event("placement_cache_miss")
	err = r.catalogRead(ctx, "/cluster/placement/"+url.PathEscape(tenant), &resolution)
	if err != nil && ctx.Err() == nil && r.catalogVerified.Load() && catalogUnavailable(err) && time.Now().Before(cached.retainUntil) {
		r.metrics.Event("placement_stale_fallback")
		return cached.resolution, nil
	}
	if err == nil && resolution.Placement.State != "active" {
		err = &HTTPError{Status: http.StatusServiceUnavailable, Body: "tenant assignment or migration is in progress"}
		r.forgetPlacement(tenant)
	}
	if err == nil {
		r.catalogVerified.Store(true)
		r.mu.Lock()
		if r.placements == nil {
			r.placements = make(map[string]cachedPlacement)
		}
		if len(r.placements) >= 4096 {
			oldest := ""
			var expiry time.Time
			for id, entry := range r.placements {
				if oldest == "" || entry.expires.Before(expiry) {
					oldest, expiry = id, entry.expires
				}
			}
			delete(r.placements, oldest)
		}
		now := time.Now()
		r.placements[tenant] = cachedPlacement{resolution: resolution, expires: now.Add(5 * time.Second), retainUntil: now.Add(placementFallbackTTL)}
		r.mu.Unlock()
	} else if !catalogUnavailable(err) {
		r.forgetPlacement(tenant)
	}
	return resolution, err
}

func (r *Router) forgetPlacement(tenant string) {
	r.metrics.Event("placement_cache_invalidation")
	r.mu.Lock()
	delete(r.placements, tenant)
	r.mu.Unlock()
}

func (r *Router) assign(ctx context.Context, tenant, shard string) (Resolution, error) {
	if err := r.Client.JSON(ctx, r.Catalog, http.MethodPost, "/cluster/action", Action{Operation: "assign", Tenant: tenant, Target: shard}, nil); err != nil {
		return Resolution{}, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		resolution, err := r.resolve(waitCtx, tenant)
		if err == nil {
			if shard != "" && resolution.Shard.ID != shard {
				return resolution, &HTTPError{Status: http.StatusConflict, Body: "clone target must be on the source shard; move it before cloning"}
			}
			return resolution, nil
		}
		select {
		case <-waitCtx.Done():
			return Resolution{}, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Router) proxy(w http.ResponseWriter, request *http.Request, shard Shard, epoch uint64, tenant string) {
	operation := "proxy_write"
	if request.Method == http.MethodGet || request.Method == http.MethodHead {
		operation = "proxy_read"
	}
	finish := r.metrics.Start(operation)
	origin, err := r.Client.Leader(request.Context(), shard)
	defer func() {
		if aborted := recover(); aborted != nil {
			failure := request.Context().Err()
			if failure == nil {
				failure = fmt.Errorf("proxy response aborted")
			}
			finish(failure)
			panic(aborted)
		}
		finish(err)
	}()
	if err != nil {
		r.writeError(w, err)
		return
	}
	target, err := url.Parse(origin)
	if err != nil {
		r.writeError(w, err)
		return
	}
	proxy := &httputil.ReverseProxy{Transport: r.Client.HTTP.Transport, FlushInterval: -1,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.SetURL(target)
			p.Out.URL.Path = "/cluster/data" + p.In.URL.Path
			if p.In.URL.RawPath != "" {
				p.Out.URL.RawPath = "/cluster/data" + p.In.URL.RawPath
			}
			p.Out.Header.Set("Authorization", "Bearer "+r.Client.Token)
			p.Out.Header.Set("X-Raft-Cluster", shard.ClusterID)
			p.Out.Header.Set(EpochHeader, fmt.Sprint(epoch))
			p.Out.Header.Set("X-Tenant-ID", tenant)
		},
		ModifyResponse: func(response *http.Response) error {
			switch {
			case response.StatusCode < 300:
				r.metrics.Event("proxy_response_2xx")
			case response.StatusCode < 400:
				r.metrics.Event("proxy_response_3xx")
			case response.StatusCode < 500:
				r.metrics.Event("proxy_response_4xx")
			default:
				r.metrics.Event("proxy_response_5xx")
			}
			if response.StatusCode >= 400 {
				err = &HTTPError{Status: response.StatusCode}
			}
			if response.StatusCode == http.StatusTooManyRequests {
				r.metrics.Event("proxy_backpressure")
			}
			if response.StatusCode == http.StatusConflict || response.StatusCode >= 500 {
				r.forgetPlacement(tenant)
			}
			if response.StatusCode == http.StatusServiceUnavailable {
				r.Client.ForgetLeader(shard)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, failure error) {
			err = failure
			r.metrics.Event("proxy_transport_failure")
			r.Client.ForgetLeader(shard)
			r.forgetPlacement(tenant)
			r.writeError(w, failure)
		},
	}
	proxy.ServeHTTP(w, request)
}

func (r *Router) admin(w http.ResponseWriter, request *http.Request) {
	if subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+r.Client.Token)) != 1 {
		http.Error(w, "cluster administration requires the router token", http.StatusUnauthorized)
		return
	}
	if request.Method == http.MethodGet {
		if request.URL.Path != "/v1/cluster" {
			http.NotFound(w, request)
			return
		}
		var state Catalog
		if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
			r.writeError(w, err)
			return
		}
		r.writeJSON(w, http.StatusOK, state)
		return
	}
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var action Action
	switch request.URL.Path {
	case "/v1/cluster/shards":
		var shard Shard
		if json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20)).Decode(&shard) != nil || shard.Validate() != nil {
			http.Error(w, "invalid shard definition", http.StatusBadRequest)
			return
		}
		action = Action{Operation: "register", Shard: &shard}
	case "/v1/cluster/placements", "/v1/cluster/moves":
		var input struct {
			Tenant string `json:"tenant_id"`
			Target string `json:"target"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, request.Body, 4096)).Decode(&input) != nil {
			http.Error(w, "invalid placement or move request", http.StatusBadRequest)
			return
		}
		operation := "assign"
		if request.URL.Path == "/v1/cluster/moves" {
			operation = "move"
		}
		action = Action{Operation: operation, Tenant: input.Tenant, Target: input.Target}
	default:
		if strings.HasPrefix(request.URL.Path, "/v1/cluster/shards/") {
			parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/v1/cluster/shards/"), "/")
			if len(parts) != 2 || ValidateIdentifier(parts[0]) != nil || (parts[1] != "drain" && parts[1] != "resume" && parts[1] != "unregister") {
				http.NotFound(w, request)
				return
			}
			action = Action{Operation: parts[1], Target: parts[0]}
			break
		}
		if !strings.HasPrefix(request.URL.Path, "/v1/cluster/moves/") || !strings.HasSuffix(request.URL.Path, "/cancel") {
			http.NotFound(w, request)
			return
		}
		tenant := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/v1/cluster/moves/"), "/cancel")
		action = Action{Operation: "cancel", Tenant: tenant}
	}
	var result json.RawMessage
	if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodPost, "/cluster/action", action, &result); err != nil {
		r.writeError(w, err)
		return
	}
	r.mu.Lock()
	r.placements = nil
	r.mu.Unlock()
	r.writeJSON(w, http.StatusAccepted, result)
}

func (r *Router) listTenants(w http.ResponseWriter, request *http.Request) {
	var state Catalog
	if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
		r.writeError(w, err)
		return
	}
	var tenants []storage.TenantInfo
	ids := make([]string, 0, len(state.Shards))
	for id := range state.Shards {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var result struct {
			Tenants []storage.TenantInfo `json:"tenants"`
		}
		if err := r.Client.JSON(request.Context(), state.Shards[id], http.MethodGet, "/cluster/data"+request.URL.RequestURI(), nil, &result); err != nil {
			r.writeError(w, err)
			return
		}
		for _, tenant := range result.Tenants {
			if placement, ok := state.Tenants[tenant.TenantID]; ok && placement.Shard == id {
				tenants = append(tenants, tenant)
			}
		}
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i].TenantID < tenants[j].TenantID })
	r.writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func (r *Router) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func (r *Router) writeError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	var response *HTTPError
	if errors.As(err, &response) {
		status = response.Status
		if json.Valid([]byte(response.Body)) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			io.WriteString(w, response.Body)
			return
		}
	}
	w.Header().Set("Retry-After", "1")
	r.writeJSON(w, status, map[string]any{"code": "shard_unavailable", "error": err.Error(), "message": err.Error(), "retryable": status >= 500})
}
