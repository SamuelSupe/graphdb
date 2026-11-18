package ha

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type slowRaftReadKey struct{}

type slowRaftReads struct {
	*storage.FileStore
	entered chan<- struct{}
}

func (s slowRaftReads) ListPage(ctx context.Context, prefix, after string, limit int) ([]storage.ObjectInfo, string, error) {
	if strings.HasSuffix(prefix, "/tenants/tenant-a/") && ctx.Value(slowRaftReadKey{}) == "usage" {
		s.entered <- struct{}{}
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	return s.FileStore.ListPage(ctx, prefix, after, limit)
}

func TestHAGCCompletesWithSlowReadsThroughHAProxy(t *testing.T) {
	haproxy, err := exec.LookPath("haproxy")
	if err != nil {
		t.Skip("requires HAProxy")
	}
	entered := make(chan struct{}, 1)
	withSlowReads := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if read := r.Header.Get("X-Test-Slow-Read"); read != "" {
				r = r.WithContext(context.WithValue(r.Context(), slowRaftReadKey{}, read))
			}
			next.ServeHTTP(w, r)
		})
	}
	group := newTestCluster(t, true, func(app *Application, next http.Handler) http.Handler {
		app.Store.Objects = slowRaftReads{app.Files, entered}
		return withSlowReads(next)
	})
	for i := range group.nodes {
		group.stop(i)
	}
	for i, node := range group.nodes {
		node.cfg.Raft.Protocol = 3
		node.cfg.Raft.StreamSnapshots = true
		group.start(i)
	}
	leader := group.leader(-1)
	group.mustRequest(leader, http.MethodPost, "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, http.MethodPost, "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:kept","kind":"host"}]}}`, http.StatusOK)

	admission := httpapi.NewQueryAdmission(1, 0, 0)
	addresses := make([]string, len(group.nodes))
	for i, node := range group.nodes {
		api := &httpapi.Server{Store: node.store, Mode: "all", Cluster: node.cluster, UsageCacheTTL: time.Nanosecond, Admission: admission}
		server := httptest.NewServer(withSlowReads(api.Handler()))
		t.Cleanup(server.Close)
		addresses[i] = server.Listener.Addr().String()
	}
	proxy := startGCReadTestProxy(t, haproxy, addresses, leader)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	type result struct {
		read   string
		status int
		header http.Header
		body   []byte
		err    error
	}
	fetch := func(method, path, body, read string, follower bool) result {
		req, err := http.NewRequestWithContext(ctx, method, proxy+path, strings.NewReader(body))
		if err != nil {
			return result{read: read, err: err}
		}
		req.Header.Set("X-Tenant-ID", "tenant-a")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Test-Slow-Read", read)
		if follower {
			req.Header.Set("X-Test-Follower", "true")
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return result{read: read, err: err}
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		return result{read, res.StatusCode, res.Header, raw, err}
	}
	const match = `{"op":"match","kind":"host"}`
	if warm := fetch(http.MethodGet, "/v1/tenant-usage", "", "", false); warm.status != http.StatusOK || warm.err != nil {
		t.Fatalf("warm tenant usage: %+v", warm)
	}

	release, err := admission.Acquire(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	releaseQuery := sync.OnceFunc(release)
	defer releaseQuery()
	results := make(chan result, 2)
	go func() { results <- fetch(http.MethodGet, "/v1/tenant-usage", "", "usage", false) }()
	go func() {
		res := fetch(http.MethodPost, "/v1/query", match, "", false)
		res.read = "query"
		results <- res
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("tenant usage did not reach storage")
	}
	app := group.nodes[leader].cluster.App
	for {
		app.readMu.Lock()
		active := len(app.activeReads)
		app.readMu.Unlock()
		if active == 2 {
			break
		}
		select {
		case res := <-results:
			t.Fatalf("read returned before GC: %s status=%d error=%v body=%q", res.read, res.status, res.err, res.body)
		case <-ctx.Done():
			t.Fatal("both reads did not reach the Raft read barrier")
		case <-time.After(10 * time.Millisecond):
		}
	}

	started := time.Now()
	gc := fetch(http.MethodPost, "/v1/tasks", `{"type":"gc","params":{"keep_snapshots":2}}`, "", false)
	var task storage.Task
	if gc.err != nil || gc.status != http.StatusAccepted || json.Unmarshal(gc.body, &task) != nil || task.ID == "" {
		t.Fatalf("GC admission: status=%d error=%v body=%q", gc.status, gc.err, gc.body)
	}
	for range 2 {
		select {
		case res := <-results:
			var apiError httpapi.ErrorResponse
			if res.err != nil || res.status != http.StatusServiceUnavailable || res.header.Get("Retry-After") != "1" ||
				json.Unmarshal(res.body, &apiError) != nil || apiError.Code != httpapi.ErrorCodeRaftUnavailable || !apiError.Retryable ||
				!strings.Contains(apiError.Message, "Raft maintenance interrupted the read") {
				t.Errorf("%s through HAProxy: status=%d error=%v body=%q", res.read, res.status, res.err, res.body)
			}
		case <-ctx.Done():
			t.Fatal("cancelled read retained its connection")
		}
	}
	for {
		gc = fetch(http.MethodGet, "/v1/tasks/"+task.ID, "", "", false)
		if gc.err != nil || gc.status != http.StatusOK || json.Unmarshal(gc.body, &task) != nil {
			t.Fatalf("GC status: status=%d error=%v body=%q", gc.status, gc.err, gc.body)
		}
		if task.Status == storage.TaskStatusSucceeded {
			t.Logf("GC completed after %s with two interrupted reads through HAProxy", time.Since(started))
			break
		}
		if task.Status == storage.TaskStatusFailed {
			t.Fatalf("GC failed: %s", gc.body)
		}
		time.Sleep(25 * time.Millisecond)
	}
	releaseQuery()
	if res := fetch(http.MethodPost, "/v1/query", match, "", true); res.err != nil || res.status != http.StatusOK || !strings.Contains(string(res.body), "host:kept") {
		t.Fatalf("query after GC: status=%d error=%v body=%q", res.status, res.err, res.body)
	}
	if res := fetch(http.MethodGet, "/v1/tenant-usage", "", "", false); res.err != nil || res.status != http.StatusOK || !json.Valid(res.body) {
		t.Fatalf("tenant usage after GC: status=%d error=%v body=%q", res.status, res.err, res.body)
	}
}

func startGCReadTestProxy(t *testing.T, binary string, addresses []string, leader int) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	config := fmt.Sprintf(`global
    nbthread 1
defaults
    mode http
    timeout connect 2s
    timeout client 10s
    timeout server 10s
frontend graphdb
    bind %s
    use_backend follower if { req.hdr(X-Test-Follower) -m str true }
    default_backend raft
backend raft
    balance first
    server leader %s maxconn 32
    server replica1 %s backup
    server replica2 %s backup
backend follower
    server follower %s
`, address, addresses[leader], addresses[(leader+1)%3], addresses[(leader+2)%3], addresses[(leader+1)%3])
	filename := filepath.Join(t.TempDir(), "haproxy.cfg")
	if err := os.WriteFile(filename, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-db", "-f", filename)
	logFile, err := os.Create(filepath.Join(t.TempDir(), "haproxy.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = logFile.Close()
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond); err == nil {
			conn.Close()
			return "http://" + address
		}
		time.Sleep(25 * time.Millisecond)
	}
	log, _ := os.ReadFile(logFile.Name())
	t.Fatalf("HAProxy did not listen at %s: %s", address, log)
	return ""
}
