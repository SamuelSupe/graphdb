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

	"github.com/SamuelSupe/graphdb/v2/internal/adminui"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

type Router struct {
	Catalog              Shard
	Client               *Client
	AuthToken            string
	AdminToken           string
	RequireTenantScope   bool
	mu                   sync.Mutex
	placements           map[string]cachedPlacement
	placementEpochs      map[string]uint64
	placementGlobalEpoch uint64
	placementFlight      singleflight.Group
	proxies              map[string]*httputil.ReverseProxy
	draining             atomic.Bool
	metrics              *observability.OperationMetrics
	catalogVerified      atomic.Bool
	catalogStateMu       sync.Mutex
	catalogStateAt       int64
	catalogUnavailable   atomic.Bool
	catalogRetryAt       atomic.Int64
	catalogLastSuccess   atomic.Int64
}

type cachedPlacement struct {
	resolution  Resolution
	expires     time.Time
	retainUntil time.Time
}

func NewRouter(catalog Shard, token string) *Router {
	return NewRouterWithGraphLimits(catalog, token, DefaultGraphAdmissionLimits())
}

func NewRouterWithGraphLimits(catalog Shard, token string, limits GraphAdmissionLimits) *Router {
	client := NewClient(token)
	client.graphAdmission.limits = limits
	return &Router{Catalog: catalog, Client: client, metrics: observability.NewOperationMetrics()}
}

func hasPathTraversal(u *url.URL) bool {
	if u == nil {
		return false
	}
	if strings.Contains(u.Path, "\\") || strings.Contains(u.RawPath, "\\") {
		return true
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment == "." || segment == ".." {
			return true
		}
	}
	for _, rawSegment := range strings.Split(u.EscapedPath(), "/") {
		decoded, err := url.PathUnescape(rawSegment)
		if err != nil {
			return true
		}
		if decoded == "." || decoded == ".." || strings.Contains(decoded, "\\") {
			return true
		}
		if strings.Contains(decoded, "/") {
			for _, sub := range strings.Split(decoded, "/") {
				if sub == "." || sub == ".." {
					return true
				}
			}
		}
	}
	return false
}

func (r *Router) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if hasPathTraversal(request.URL) {
		http.Error(w, "invalid request path", http.StatusBadRequest)
		return
	}
	if request.URL.Path == "/ui" || strings.HasPrefix(request.URL.Path, "/ui/") {
		adminui.Handler().ServeHTTP(w, request)
		return
	}
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
	case "/openapi.yaml":
		r.proxy(w, request, r.Catalog, 0, "")
		return
	}
	authHeader := request.Header.Get("Authorization")
	bearerToken := ""
	if strings.HasPrefix(authHeader, "Bearer ") {
		bearerToken = strings.TrimPrefix(authHeader, "Bearer ")
	}
	operatorMatch := r.Client.Token != "" && subtle.ConstantTimeCompare([]byte(bearerToken), []byte(r.Client.Token)) == 1
	adminMatch := r.AdminToken != "" && subtle.ConstantTimeCompare([]byte(bearerToken), []byte(r.AdminToken)) == 1
	authMatch := r.AuthToken != "" && subtle.ConstantTimeCompare([]byte(bearerToken), []byte(r.AuthToken)) == 1
	isAdminCaller := operatorMatch || adminMatch
	if !publicDataRoute(request) {
		if !isAdminCaller {
			http.Error(w, "administrative routing requires the operator token", http.StatusForbidden)
			return
		}
	} else if r.AuthToken != "" || r.AdminToken != "" {
		if !authMatch && !isAdminCaller {
			w.Header().Set("WWW-Authenticate", `Bearer realm="graphdb"`)
			http.Error(w, "valid bearer token is required", http.StatusUnauthorized)
			return
		}
	}
	if isAdminCaller {
		request = request.WithContext(storage.WithAdminAuthorized(request.Context()))
	}
	if request.URL.Path == "/v1/tenants" && request.Method == http.MethodGet {
		if r.RequireTenantScope && request.Header.Get("X-Tenant-ID") == "" && !isAdminCaller {
			http.Error(w, "tenant scope header X-Tenant-ID is required", http.StatusForbidden)
			return
		}
		r.listTenants(w, request)
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
	clone := request.Method == http.MethodPost &&
		strings.HasPrefix(request.URL.Path, "/v1/tenants/") &&
		strings.HasSuffix(request.URL.Path, "/clone") &&
		strings.Count(strings.TrimPrefix(request.URL.Path, "/v1/tenants/"), "/") == 1
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
	if scope := request.Header.Get("X-Tenant-ID"); scope != "" && (scope != tenant || (clone && scope != target)) {
		http.Error(w, "tenant scope does not authorize the requested tenant", http.StatusForbidden)
		return
	}
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
		if resolution.Placement.Mode == "partitioned" {
			r.writeJSON(w, http.StatusNotImplemented, map[string]any{"code": "operation_disabled", "error": "partitioned graph clone is not yet available"})
			return
		}
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
	if resolution.Placement.Mode == "partitioned" && r.serveGraph(w, request, resolution, tenant) {
		return
	}
	r.proxy(w, request, resolution.Shard, resolution.Placement.Epoch, tenant)
}

func (r *Router) resolve(ctx context.Context, tenant string) (resolution Resolution, err error) {
	finish := r.metrics.Start("placement_lookup")
	defer func() { finish(err) }()
	if err := ctx.Err(); err != nil {
		return Resolution{}, err
	}
	r.mu.Lock()
	cached := r.placements[tenant]
	r.mu.Unlock()
	if time.Now().Before(cached.expires) {
		r.metrics.Event("placement_cache_hit")
		return cached.resolution, nil
	}
	r.metrics.Event("placement_cache_miss")
	flight := r.placementFlight.DoChan(tenant, func() (any, error) {
		r.mu.Lock()
		startGlobalEpoch := r.placementGlobalEpoch
		startTenantEpoch := r.placementEpochs[tenant]
		r.mu.Unlock()
		lookupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var res Resolution
		lookupErr := r.catalogRead(lookupCtx, "/cluster/placement/"+url.PathEscape(tenant), &res)
		if lookupErr != nil {
			if catalogUnavailable(lookupErr) {
				r.mu.Lock()
				stale := r.placements[tenant]
				now := time.Now()
				if r.catalogVerified.Load() && now.Before(stale.retainUntil) {
					// Extend short grace window so fallback does not hammer the catalog on every request.
					grace := now.Add(time.Second)
					if grace.After(stale.retainUntil) {
						grace = stale.retainUntil
					}
					if grace.After(stale.expires) {
						stale.expires = grace
						r.placements[tenant] = stale
					}
					r.mu.Unlock()
					r.metrics.Event("placement_stale_fallback")
					return stale.resolution, nil
				}
				r.mu.Unlock()
			} else {
				r.forgetPlacement(tenant)
			}
			return Resolution{}, lookupErr
		}
		if res.Placement.State != "active" {
			r.forgetPlacement(tenant)
			return Resolution{}, &HTTPError{Status: http.StatusServiceUnavailable, Body: "tenant assignment or migration is in progress"}
		}
		r.catalogVerified.Store(true)
		r.mu.Lock()
		if r.placementGlobalEpoch == startGlobalEpoch && r.placementEpochs[tenant] == startTenantEpoch {
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
			r.placements[tenant] = cachedPlacement{resolution: res, expires: now.Add(5 * time.Second), retainUntil: now.Add(placementFallbackTTL)}
		}
		r.mu.Unlock()
		return res, nil
	})
	select {
	case <-ctx.Done():
		return Resolution{}, ctx.Err()
	case result := <-flight:
		if result.Err != nil {
			return Resolution{}, result.Err
		}
		return result.Val.(Resolution), nil
	}
}

func (r *Router) forgetPlacement(tenant string) {
	r.metrics.Event("placement_cache_invalidation")
	r.mu.Lock()
	if r.placementEpochs == nil {
		r.placementEpochs = make(map[string]uint64)
	}
	if _, exists := r.placementEpochs[tenant]; !exists && len(r.placementEpochs) >= 4096 {
		// Bound memory under arbitrary/unknown X-Tenant-ID probing while keeping
		// monotonic epoch fencing for all in-flight lookups.
		clear(r.placementEpochs)
		clear(r.placements)
		r.placementGlobalEpoch++
	} else {
		r.placementEpochs[tenant]++
		delete(r.placements, tenant)
	}
	r.mu.Unlock()
	r.placementFlight.Forget(tenant)
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

type proxyCallKey struct{}

type proxyCallState struct {
	shard  Shard
	epoch  uint64
	tenant string
	isRead bool
	err    error
}

func (r *Router) reverseProxyFor(origin string, target *url.URL) *httputil.ReverseProxy {
	r.mu.Lock()
	if p, ok := r.proxies[origin]; ok {
		r.mu.Unlock()
		return p
	}
	p := &httputil.ReverseProxy{
		Transport:     r.Client.HTTP.Transport,
		FlushInterval: -1,
		Rewrite: func(p *httputil.ProxyRequest) {
			state, _ := p.In.Context().Value(proxyCallKey{}).(*proxyCallState)
			p.SetURL(target)
			p.Out.Host = p.In.Host
			p.Out.URL.Path = "/cluster/data" + p.In.URL.Path
			if p.In.URL.RawPath != "" {
				p.Out.URL.RawPath = "/cluster/data" + p.In.URL.RawPath
			}
			p.Out.Header.Set("Authorization", "Bearer "+r.Client.Token)
			if storage.IsAdminAuthorized(p.In.Context()) {
				p.Out.Header.Set("X-GraphDB-Raft-Admin", "1")
			} else {
				p.Out.Header.Del("X-GraphDB-Raft-Admin")
			}
			if state != nil {
				p.Out.Header.Set("X-Raft-Cluster", state.shard.ClusterID)
				p.Out.Header.Set(EpochHeader, fmt.Sprint(state.epoch))
				if !strings.HasPrefix(p.In.URL.Path, "/v1/tenants") {
					p.Out.Header.Set("X-Tenant-ID", state.tenant)
				}
			}
			if !readRequest(p.In) {
				PreventMutationReplay(p.Out)
			}
		},
		ModifyResponse: func(response *http.Response) error {
			state, _ := response.Request.Context().Value(proxyCallKey{}).(*proxyCallState)
			if state != nil {
				if response.StatusCode >= 500 && response.StatusCode != http.StatusGatewayTimeout && !isApplication503(response) {
					r.forgetPlacement(state.tenant)
				}
				if response.StatusCode == http.StatusServiceUnavailable && !isApplication503(response) {
					r.Client.ForgetLeader(state.shard)
				}
				if response.StatusCode == http.StatusConflict && state.tenant != "" {
					if err := r.migrationResponse(response.Request.Context(), response, state.tenant); err != nil {
						return err
					}
				}
			}
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
			if response.StatusCode >= 400 && state != nil {
				state.err = &HTTPError{Status: response.StatusCode}
			}
			if response.StatusCode == http.StatusTooManyRequests {
				r.metrics.Event("proxy_backpressure")
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, failure error) {
			state, _ := req.Context().Value(proxyCallKey{}).(*proxyCallState)
			if state != nil {
				state.err = failure
				if !errors.Is(failure, context.Canceled) {
					r.Client.ForgetLeader(state.shard)
					r.forgetPlacement(state.tenant)
				}
			}
			r.metrics.Event("proxy_transport_failure")
			isRead := readRequest(req)
			if state != nil {
				isRead = state.isRead
			}
			if !isRead {
				message := "the write response was lost; check its status or reuse the same idempotency key"
				r.writeJSON(w, http.StatusBadGateway, map[string]any{"code": "write_outcome_unknown", "error": message, "message": message, "retryable": false})
			} else {
				r.writeError(w, failure)
			}
		},
	}
	if r.proxies == nil {
		r.proxies = make(map[string]*httputil.ReverseProxy)
	}
	if len(r.proxies) >= 256 {
		clear(r.proxies)
	}
	r.proxies[origin] = p
	r.mu.Unlock()
	return p
}

func (r *Router) proxy(w http.ResponseWriter, request *http.Request, shard Shard, epoch uint64, tenant string) {
	isRead := readRequest(request)
	operation := "proxy_write"
	if isRead {
		operation = "proxy_read"
	}
	finish := r.metrics.Start(operation)
	origin, err := r.Client.Leader(request.Context(), shard)
	state := &proxyCallState{shard: shard, epoch: epoch, tenant: tenant, isRead: isRead}
	defer func() {
		if aborted := recover(); aborted != nil {
			failure := request.Context().Err()
			if failure == nil {
				failure = fmt.Errorf("proxy response aborted")
			}
			finish(failure)
			panic(aborted)
		}
		if err == nil {
			err = state.err
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
	proxy := r.reverseProxyFor(origin, target)
	if request.ContentLength != 0 {
		// HTTP/1 would close the incoming body at the first response flush,
		// while the proxy transport can still be reading that same body.
		_ = http.NewResponseController(w).EnableFullDuplex()
	}
	proxy.ServeHTTP(w, request.WithContext(context.WithValue(request.Context(), proxyCallKey{}, state)))
}

func (r *Router) migrationResponse(ctx context.Context, response *http.Response, tenant string) error {
	// Another router can start a move while this router still has a cached
	// active placement. Refresh only explicit fencing rejections; never replay
	// the data request or turn an ordinary business conflict into a retry.
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil {
		return err
	}
	response.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(body), response.Body), response.Body}
	var rejection struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	}
	parsed := len(body) <= 4096 && json.Unmarshal(body, &rejection) == nil
	if !parsed || rejection.Code == "" || rejection.Code == "shard_epoch_changed" || rejection.Code == "graph_mode_changed" || rejection.Code == "graph_layout_changed" {
		r.forgetPlacement(tenant)
	}
	if !parsed || rejection.Code != "shard_epoch_changed" || !rejection.Retryable {
		return nil
	}
	r.forgetPlacement(tenant)
	var resolution Resolution
	if r.catalogRead(ctx, "/cluster/placement/"+url.PathEscape(tenant), &resolution) != nil || resolution.Placement.State == "active" {
		return nil
	}
	response.Body.Close()
	body = []byte(`{"code":"shard_unavailable","error":"tenant assignment or migration is in progress","message":"tenant assignment or migration is in progress","retryable":true}` + "\n")
	response.StatusCode = http.StatusServiceUnavailable
	response.Status = "503 Service Unavailable"
	response.Header.Set("Content-Type", "application/json")
	response.Header.Set("Content-Length", fmt.Sprint(len(body)))
	response.Header.Set("Retry-After", "1")
	response.ContentLength = int64(len(body))
	response.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

func (r *Router) admin(w http.ResponseWriter, request *http.Request) {
	if subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+r.Client.Token)) != 1 {
		http.Error(w, "cluster administration requires the router token", http.StatusUnauthorized)
		return
	}
	if r.graphAdmin(w, request) {
		return
	}
	if request.Method == http.MethodGet {
		if request.URL.Path != "/v1/cluster" {
			http.NotFound(w, request)
			return
		}
		options, err := storage.ParseTenantListOptions(request.URL.Query())
		if err != nil {
			r.writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		var state Catalog
		if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
			r.writeError(w, err)
			return
		}
		if options.Limit > 0 {
			ids := make([]string, 0, len(state.Tenants))
			for id := range state.Tenants {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			page, total, next := storage.PageTenantIDs(ids, options)
			placements := make(map[string]Placement, len(page))
			for _, id := range page {
				placements[id] = state.Tenants[id]
			}
			r.writeJSON(w, http.StatusOK, map[string]any{"version": state.Version, "shards": state.Shards, "tenants": placements, "total": total, "next_cursor": next})
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
	if action.Tenant != "" {
		r.forgetPlacement(action.Tenant)
	} else {
		r.mu.Lock()
		r.placementGlobalEpoch++
		r.placements = nil
		clear(r.placementEpochs)
		r.mu.Unlock()
	}
	r.writeJSON(w, http.StatusAccepted, result)
}

func (r *Router) listTenants(w http.ResponseWriter, request *http.Request) {
	options, err := storage.ParseTenantListOptions(request.URL.Query())
	if err != nil {
		r.writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	var state Catalog
	if err := r.Client.JSON(request.Context(), r.Catalog, http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
		r.writeError(w, err)
		return
	}
	if options.Limit > 0 {
		r.listTenantPage(w, request, state, options)
		return
	}
	var tenants []storage.TenantInfo
	ids := make([]string, 0, len(state.Shards))
	for id := range state.Shards {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	privatePath := "/cluster/data/v1/tenants"
	if options.IncludeLegacy {
		privatePath += "?include_legacy=true"
	}
	var headers []http.Header
	if storage.IsAdminAuthorized(request.Context()) {
		headers = []http.Header{{"X-GraphDB-Raft-Admin": {"1"}}}
	}
	perShard := make([][]storage.TenantInfo, len(ids))
	group, gctx := errgroup.WithContext(request.Context())
	group.SetLimit(8)
	for i, id := range ids {
		group.Go(func() error {
			var result struct {
				Tenants []storage.TenantInfo `json:"tenants"`
			}
			if err := r.Client.JSON(gctx, state.Shards[id], http.MethodGet, privatePath, nil, &result, headers...); err != nil {
				return err
			}
			filtered := make([]storage.TenantInfo, 0, len(result.Tenants))
			for _, tenant := range result.Tenants {
				if placement, ok := state.Tenants[tenant.TenantID]; ok && placement.Shard == id {
					filtered = append(filtered, tenant)
				}
			}
			perShard[i] = filtered
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		r.writeError(w, err)
		return
	}
	for _, list := range perShard {
		tenants = append(tenants, list...)
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i].TenantID < tenants[j].TenantID })
	r.writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func (r *Router) listTenantPage(w http.ResponseWriter, request *http.Request, state Catalog, options storage.TenantListOptions) {
	ctx := request.Context()
	registryPath := "/cluster/tenant-ids"
	if options.IncludeLegacy {
		registryPath += "?include_legacy=true"
	}
	active := func(p Placement) bool { return p.State == "" || p.State == "active" }
	shardIDs := make([]string, 0, len(state.Shards))
	for shardID := range state.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	sort.Strings(shardIDs)
	shardResults := make([][]string, len(shardIDs))
	shardGroup, shardCtx := errgroup.WithContext(ctx)
	shardGroup.SetLimit(8)
	for i, shardID := range shardIDs {
		shard := state.Shards[shardID]
		shardGroup.Go(func() error {
			var storedIDs []string
			if err := r.Client.JSON(shardCtx, shard, http.MethodGet, registryPath, nil, &storedIDs); err != nil {
				var response *HTTPError
				if !errors.As(err, &response) || response.Status != http.StatusNotFound {
					return err
				}
				var fallback []string
				for id, placement := range state.Tenants {
					if placement.Shard == shardID && active(placement) {
						fallback = append(fallback, id)
					}
				}
				shardResults[i] = fallback
				return nil
			}
			var matched []string
			for _, id := range storedIDs {
				if placement := state.Tenants[id]; placement.Shard == shardID && active(placement) {
					matched = append(matched, id)
				}
			}
			shardResults[i] = matched
			return nil
		})
	}
	if err := shardGroup.Wait(); err != nil {
		r.writeError(w, err)
		return
	}
	var ids []string
	for _, list := range shardResults {
		ids = append(ids, list...)
	}
	sort.Strings(ids)
	page, total, next := storage.PageTenantIDs(ids, options)
	type pageEntry struct {
		info storage.TenantInfo
		ok   bool
		skip bool
	}
	entries := make([]pageEntry, len(page))
	pageGroup, pageCtx := errgroup.WithContext(ctx)
	pageGroup.SetLimit(8)
	for i, id := range page {
		p := state.Tenants[id]
		pageGroup.Go(func() error {
			var info storage.TenantInfo
			reqHeader := http.Header{"X-Tenant-ID": {id}, EpochHeader: {fmt.Sprint(p.Epoch)}}
			if storage.IsAdminAuthorized(request.Context()) {
				reqHeader.Set("X-GraphDB-Raft-Admin", "1")
			}
			err := r.Client.JSON(pageCtx, state.Shards[p.Shard], http.MethodGet, "/cluster/data/v1/tenants/"+url.PathEscape(id), nil, &info, reqHeader)
			var response *HTTPError
			if errors.As(err, &response) && (response.Status == http.StatusNotFound || response.Status == http.StatusConflict) {
				entries[i] = pageEntry{skip: true}
				return nil
			}
			if err != nil {
				return err
			}
			entries[i] = pageEntry{info: info, ok: true}
			return nil
		})
	}
	if err := pageGroup.Wait(); err != nil {
		r.writeError(w, err)
		return
	}
	items := make([]storage.TenantInfo, 0, len(page))
	for _, entry := range entries {
		if entry.skip {
			total--
			continue
		}
		if entry.ok {
			items = append(items, entry.info)
		}
	}
	r.writeJSON(w, http.StatusOK, storage.TenantInfoPage{Tenants: items, Total: total, NextCursor: next})
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

// Preserve the router's data and discovery API while requiring the original
// caller's operator token before adding our private shard credentials.
func publicDataRoute(r *http.Request) bool {
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/v1/entities", "/v1/entities/stream", "/v1/edges", "/v1/edges/stream", "/v1/export/snapshot", "/v1/export/snapshot/stream", "/v1/ci-types", "/v1/entity-types", "/v1/relation-types", "/v1/relation-schemas", "/v1/source-policy", "/v1/tenant-config", "/v1/backup-automation", "/v1/query/templates":
			return true
		}
		for _, prefix := range []string{"/v1/entities/", "/v1/ingest/batches/", "/v1/ingest/writers/"} {
			if strings.HasPrefix(r.URL.Path, prefix) {
				return true
			}
		}
	}
	if r.Method == http.MethodPost {
		switch r.URL.Path {
		case "/v1/commits", "/v1/ingest/batches", "/v1/imports":
			return true
		}
		return readRequest(r)
	}
	return false
}

func readRequest(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	case http.MethodPost:
		switch r.URL.Path {
		case "/v1/query", "/v1/query/stream", "/v1/query/graphql", "/v1/query/gql", "/v1/query/gql/stream":
			return true
		}
		// Executing a saved template reads; POST /v1/query/templates saves one.
		return strings.HasPrefix(r.URL.Path, "/v1/query/templates/")
	}
	return false
}
