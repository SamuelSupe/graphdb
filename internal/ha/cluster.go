package ha

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type Cluster struct {
	cancel           context.CancelFunc
	background       sync.WaitGroup
	closeOnce        sync.Once
	closeErr         error
	Node             *replication.Node
	App              *Application
	WAL              bool
	FlushInterval    time.Duration
	FlushMaxRequests int
	FlushMaxBytes    int64
}

func New(cfg config.Config, store *storage.TenantStore, files *storage.FileStore) *Cluster {
	store.ReplicationMode = true
	files.RequireReplicatedWrites()
	return &Cluster{App: &Application{Store: store, Files: files, MaxSnapshotBytes: cfg.Raft.MaxSnapshotBytes, MaxPendingBytes: cfg.IngestQueueMemoryBytes, FlushInterval: cfg.IngestFlushInterval}, WAL: cfg.IngestMode == "wal", FlushInterval: cfg.IngestFlushInterval, FlushMaxRequests: cfg.IngestFlushMaxRequests, FlushMaxBytes: cfg.IngestFlushMaxBytes}
}

func (c *Cluster) Start(ctx context.Context, cfg config.RaftConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	node, err := replication.Open(ctx, replication.Config{ID: cfg.ID, ClusterID: cfg.ClusterID, Dir: cfg.Dir, Peers: cfg.Peers, Bootstrap: cfg.Bootstrap, Token: cfg.Token, Tick: cfg.Tick, SnapshotEntries: cfg.SnapshotEntries, MaxSnapshotBytes: cfg.MaxSnapshotBytes}, c.App)
	if err != nil {
		cancel()
		return err
	}
	c.Node = node
	c.background.Add(1)
	go func() { defer c.background.Done(); c.RunBackground(ctx) }()
	return nil
}

func newCommand(kind string) (command, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return command{}, err
	}
	return command{ID: hex.EncodeToString(id[:]), At: time.Now().UTC(), Kind: kind}, nil
}
func (c *Cluster) propose(ctx context.Context, cmd command) ([]byte, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	return c.Node.Propose(ctx, data)
}

func (c *Cluster) ServeRoute(w http.ResponseWriter, r *http.Request, mutation, runtimeOnly bool, next http.Handler) {
	if storage.IsReplicatedContext(r.Context()) {
		next.ServeHTTP(w, r)
		return
	}
	switch r.URL.Path {
	case "/v1/health", "/openapi.yaml", "/metrics":
		next.ServeHTTP(w, r)
		return
	case "/v1/readiness":
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := c.Node.ReadBarrier(ctx); err != nil {
			c.writeError(w, err)
			return
		}
		next.ServeHTTP(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	if err := c.Node.ReadBarrier(ctx); err != nil {
		c.writeError(w, err)
		return
	}
	tenant := r.Header.Get("X-Tenant-ID")
	if strings.HasPrefix(r.URL.EscapedPath(), "/v1/tenants/") {
		segments := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/v1/tenants/"), "/")
		if len(segments) > 0 {
			tenant, _ = url.PathUnescape(segments[0])
		}
	}
	var expectedGeneration int64
	if raw := r.Header.Get("X-GraphDB-Read-Generation"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 || tenant == "" {
			http.Error(w, "invalid tenant read generation", http.StatusBadRequest)
			return
		}
		expectedGeneration = parsed
	}
	if !mutation || runtimeOnly {
		c.App.mu.RLock()
		defer c.App.mu.RUnlock()
		if tenant != "" {
			generation, err := c.App.Store.ReplicationTenantGeneration(ctx, tenant)
			if err != nil {
				c.writeError(w, err)
				return
			}
			if expectedGeneration > 0 && generation != expectedGeneration {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]any{"code": "tenant_generation_changed", "error": "tenant has been replaced; obtain a new read token", "message": "tenant has been replaced; obtain a new read token", "retryable": false})
				return
			}
			w.Header().Set("X-GraphDB-Tenant-Generation", fmt.Sprint(generation))
		}
		if c.WAL && (strings.HasPrefix(r.URL.Path, "/v1/ingest/batches/") || strings.HasPrefix(r.URL.Path, "/v1/ingest/writers/")) {
			c.batchStatus(w, r)
			return
		}
		next.ServeHTTP(w, r)
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	cmd, err := newCommand("http")
	if err != nil {
		c.writeError(w, err)
		return
	}
	cmd.Method = r.Method
	cmd.URI = r.URL.RequestURI()
	cmd.Body = body
	cmd.Header = make(http.Header)
	for _, name := range []string{"X-Tenant-ID", "Content-Type"} {
		if value := r.Header.Get(name); value != "" {
			cmd.Header.Set(name, value)
		}
	}
	cmd.Tenant = tenant
	cmd.ExpectedGeneration = expectedGeneration
	if r.URL.Path == "/v1/ingest/batches" {
		var request storage.IngestRequest
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		request, err = storage.PrepareIngestRequest(cmd.Tenant, request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cmd.Body, err = json.Marshal(request)
		if err != nil {
			c.writeError(w, err)
			return
		}
		if c.WAL {
			cmd.Kind = "accept"
		}
	}
	data, err := c.propose(ctx, cmd)
	if err != nil {
		c.writeError(w, err)
		return
	}
	if cmd.Kind == "accept" {
		if strings.Contains(strings.ToLower(r.Header.Get("Prefer")), "wait=committed") {
			if err := c.waitCommitted(ctx, w, data); err != nil {
				c.writeError(w, err)
			}
			return
		}
		writeResult(w, data)
		return
	}
	writeResult(w, data)
}

func (c *Cluster) writeError(w http.ResponseWriter, err error) {
	code := http.StatusServiceUnavailable
	if errors.Is(err, context.DeadlineExceeded) {
		code = http.StatusGatewayTimeout
	}
	w.Header().Set("X-GraphDB-Leader-ID", fmt.Sprint(c.Node.LeaderID()))
	w.Header().Set("Retry-After", "1")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "message": err.Error(), "code": "raft_unavailable", "retryable": true, "leader_id": c.Node.LeaderID()})
}

func (c *Cluster) Close() error {
	c.closeOnce.Do(func() { c.cancel(); c.background.Wait(); c.closeErr = c.Node.Close() })
	return c.closeErr
}
func (c *Cluster) Status() map[string]any { return c.Node.Status() }
