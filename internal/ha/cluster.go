package ha

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
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
	config           config.RaftConfig
	diskPolicy       storage.DiskSpacePolicy
	shards           *sharding.Client
	metrics          *observability.OperationMetrics
}

func New(cfg config.Config, store *storage.TenantStore, files *storage.FileStore) *Cluster {
	store.ReplicationMode = true
	return &Cluster{diskPolicy: storage.DiskSpacePolicy{MinFreeBytes: cfg.DiskMinFreeBytes, MinFreePercent: cfg.DiskMinFreePercent}, App: &Application{Store: store, Files: files, MaxSnapshotBytes: cfg.Raft.MaxSnapshotBytes, MaxPendingBytes: cfg.IngestQueueMemoryBytes, FlushInterval: cfg.IngestFlushInterval, ShardID: cfg.Raft.ShardID, Catalog: cfg.Raft.Catalog}, WAL: cfg.IngestMode == "wal", FlushInterval: cfg.IngestFlushInterval, FlushMaxRequests: cfg.IngestFlushMaxRequests, FlushMaxBytes: cfg.IngestFlushMaxBytes}
}

func (c *Cluster) Start(ctx context.Context, cfg config.RaftConfig) error {
	c.metrics = observability.NewOperationMetrics()
	if _, err := c.App.Applied(); err != nil {
		return err
	}
	if err := c.App.Files.RequireReplicatedWrites(); err != nil {
		return err
	}
	role := ""
	if cfg.Catalog {
		role = "catalog"
	} else if cfg.ShardID != "" {
		role = "shard:" + cfg.ShardID
	}
	if err := c.App.Files.ConfigureReplicationRole(role); err != nil {
		return err
	}
	c.config = cfg
	c.shards = sharding.NewClient(cfg.Token)
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	node, err := replication.Open(ctx, replication.Config{Protocol: cfg.Protocol, ID: cfg.ID, ClusterID: cfg.ClusterID, Dir: cfg.Dir, Peers: cfg.Peers, Bootstrap: cfg.Bootstrap, Token: cfg.Token, Tick: cfg.Tick, SnapshotEntries: cfg.SnapshotEntries, MaxSnapshotBytes: cfg.MaxSnapshotBytes, AllowLegacyProtocol: cfg.AllowLegacyProtocol, StreamSnapshots: cfg.StreamSnapshots, SnapshotPreflight: func(ctx context.Context, bytes int64) error {
		return storage.CheckDiskSpace(ctx, cfg.Dir, c.diskPolicy, bytes)
	}}, c.App)
	if err != nil {
		cancel()
		return err
	}
	c.Node = node
	cfg.Protocol = node.Status()["protocol_version"].(int)
	c.config = cfg
	c.background.Add(1)
	go func() { defer c.background.Done(); c.RunBackground(ctx) }()
	if cfg.Protocol >= 2 {
		c.background.Add(1)
		go func() { defer c.background.Done(); c.runTaskBackground(ctx) }()
	}
	if cfg.Catalog {
		c.background.Add(1)
		go func() { defer c.background.Done(); c.runShardMigrations(ctx) }()
	}
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
	cmd.Role = c.App.replicationRole()
	c.App.mu.RLock()
	if cmd.Kind == "http" || cmd.Kind == "accept" || cmd.Kind == "flush" || cmd.Kind == "task" {
		policy := storage.BackpressureConfig{}
		if c.App.Store.Backpressure != nil {
			policy = c.App.Store.Backpressure.Config()
		}
		cmd.Backpressure = &policy
	}
	if cmd.Kind == "accept" {
		budget := c.App.MaxPendingBytes
		cmd.QueueBudget = &budget
	}
	if cmd.Kind == "http" {
		namespace := c.App.Store.Backups.Namespace()
		cmd.BackupNamespace = &namespace
	}
	c.App.mu.RUnlock()
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
	if runtimeOnly {
		if c.App.Catalog && r.URL.Path != "/metrics" && r.URL.Path != "/v1/diagnostics" {
			http.NotFound(w, r)
			return
		}
		// Query cancellation belongs to this process and must remain usable
		// while a committed mutation waits for a reader or a replica catches up.
		next.ServeHTTP(w, r)
		return
	}
	switch r.URL.Path {
	case "/v1/health", "/openapi.yaml", "/metrics":
		next.ServeHTTP(w, r)
		return
	case "/v1/readiness":
		if c.Node.Draining() {
			c.writeError(w, replication.ErrUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := c.Node.QuorumBarrier(ctx); err != nil {
			if errors.Is(err, replication.ErrNotLeader) {
				c.forward(w, r.WithContext(ctx), nil, false)
				return
			}
			c.writeError(w, err)
			return
		}
		next.ServeHTTP(w, r)
		return
	}
	// The server's body read deadline also runs while application is blocked.
	// Consume bounded input before waiting, retaining it for read handlers.
	var body []byte
	if r.Body != nil && r.Body != http.NoBody {
		var err error
		body, err = readBody(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		if !mutation {
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	if err := c.Node.ReadBarrier(ctx); err != nil {
		if errors.Is(err, replication.ErrNotLeader) {
			c.forward(w, r.WithContext(ctx), body, false)
			return
		}
		c.writeError(w, err)
		return
	}
	tenant := r.Header.Get("X-Tenant-ID")
	if c.App.Catalog {
		http.Error(w, "catalog groups only expose cluster administration on their private listener", http.StatusNotFound)
		return
	}
	if strings.HasPrefix(r.URL.EscapedPath(), "/v1/tenants/") {
		segments := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/v1/tenants/"), "/")
		if len(segments) > 0 {
			tenant, _ = url.PathUnescape(segments[0])
		}
	}
	routeEpoch, valid := c.checkShardRequest(ctx, w, r, tenant)
	if !valid {
		return
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
	if !mutation {
		graphRead := tenant != "" && (strings.HasPrefix(r.URL.Path, "/v1/query") && r.URL.Path != "/v1/query/templates" ||
			strings.HasPrefix(r.URL.Path, "/v1/entities") || strings.HasPrefix(r.URL.Path, "/v1/edges") || strings.HasPrefix(r.URL.Path, "/v1/export/snapshot"))
		if graphRead {
			c.App.readers.RLock()
			defer c.App.readers.RUnlock()
		}
		c.App.mu.RLock()
		locked := true
		defer func() {
			if locked {
				c.App.mu.RUnlock()
			}
		}()
		if c.App.ShardID != "" && tenant != "" {
			valid, err := c.App.checkOwnership(ctx, tenant, routeEpoch)
			if err != nil || !valid {
				http.Error(w, "tenant ownership changed before reading", http.StatusConflict)
				return
			}
		}
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
		if graphRead {
			readCtx, release, err := c.App.Store.ReadViewContext(r.Context(), tenant)
			if err != nil {
				c.writeError(w, err)
				return
			}
			defer release()
			r = r.WithContext(readCtx)
			c.App.mu.RUnlock()
			locked = false
		}
		next.ServeHTTP(w, r)
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
	for _, name := range []string{"X-Tenant-ID", "Content-Type", "Idempotency-Key", sharding.TargetEpochHeader} {
		if value := r.Header.Get(name); value != "" {
			cmd.Header.Set(name, value)
		}
	}
	cmd.Tenant = tenant
	cmd.RouteEpoch = routeEpoch
	cmd.ExpectedGeneration = expectedGeneration
	if r.URL.Path == "/v1/commits" || r.URL.Path == "/v1/ingest/batches" || r.URL.Path == "/v1/imports" {
		if err := c.checkAdmissionDiskSpace(ctx, int64(len(body))*4); err != nil {
			c.writeError(w, err)
			return
		}
	}
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
		if errors.Is(err, replication.ErrNotLeader) {
			c.forward(w, r.WithContext(ctx), body, false)
			return
		}
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
	var pressure *storage.BackpressureError
	if errors.As(err, &pressure) {
		w.Header().Set("Retry-After", fmt.Sprint(max(1, int(pressure.RetryAfter.Seconds()))))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]any{"error": "write backpressure", "message": "write backpressure", "code": "write_backpressure", "retryable": true, "reasons": pressure.Reasons, "retry_after_ms": pressure.RetryAfter.Milliseconds()})
		return
	}
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
	c.closeOnce.Do(func() {
		c.cancel()
		c.background.Wait()
		c.shards.HTTP.CloseIdleConnections()
		c.closeErr = c.Node.Close()
	})
	return c.closeErr
}
func (c *Cluster) Status() map[string]any {
	status := c.Node.Status()
	status["ingest_queue"] = c.App.queueObservation.Load()
	if c.App.Catalog {
		status["catalog"] = c.App.catalogObservation.Load()
	}
	return status
}

func (c *Cluster) checkAdmissionDiskSpace(ctx context.Context, additional int64) error {
	if err := c.App.Store.CheckWriteDiskSpace(ctx, additional); err != nil {
		return err
	}
	return storage.CheckDiskSpace(ctx, c.config.Dir, c.diskPolicy, additional)
}
