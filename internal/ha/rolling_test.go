package ha

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestHARollingDrainPreservesReadWriteService(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:initial","kind":"host"}]}}`, http.StatusOK)
	checkpoint, _ := group.nodes[leader].files.ReplicationCheckpoint()
	for i := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The gateway can still send a request to the old leader while its health
	// check changes. These requests must wait for handoff, then reach the leader.
	var writers sync.WaitGroup
	failures := make(chan string, 100)
	for worker := 0; worker < 2; worker++ {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			for i := 0; i < 12; i++ {
				body := fmt.Sprintf(`{"mutations":{"upsert_entities":[{"id":"host:%d:%d","kind":"host"}]}}`, worker, i)
				response := group.request(leader, "POST", "/v1/commits", body, 20*time.Second)
				if response.Code != http.StatusOK {
					failures <- fmt.Sprint(response.Code, ": ", response.Body.String())
				}
				response = group.request(leader, "GET", "/v1/entities/host:initial", "", 20*time.Second)
				if response.Code != http.StatusOK {
					failures <- fmt.Sprint(response.Code, ": ", response.Body.String())
				}
			}
		}(worker)
	}
	if err := group.nodes[leader].cluster.Node.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if !group.nodes[leader].cluster.Node.Draining() || group.nodes[leader].cluster.Node.LeaderID() == uint64(leader+1) {
		t.Fatal("drain did not hand off leadership")
	}
	group.mustRequest(leader, "GET", "/v1/readiness", "", http.StatusServiceUnavailable)
	writers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	newLeader := group.leader(-1)
	if got := group.manifest(newLeader).Version; got != 25 {
		t.Fatalf("writes were lost or duplicated: version %d, want 25", got)
	}
	group.stop(leader)
	group.mustRequest(newLeader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:during-restart","kind":"host"}]}}`, http.StatusOK)
	group.start(leader)
	checkpoint, _ = group.nodes[newLeader].files.ReplicationCheckpoint()
	group.waitApplied(leader, checkpoint.Index)
	if group.nodes[leader].cluster.Node.Draining() {
		t.Fatal("restart retained process-local draining state")
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:during-restart", "", http.StatusOK)

	other := (newLeader + 1) % 3
	group.nodes[other].blocked.Store(true)
	defer group.nodes[other].blocked.Store(false)
	if err := group.nodes[newLeader].cluster.Node.Drain(ctx); err == nil {
		t.Fatal("drain accepted an unavailable voting peer")
	}
	if group.nodes[newLeader].cluster.Node.Draining() {
		t.Fatal("failed preflight removed a healthy leader from service")
	}
}

func TestHADrainWaitsForCommittedProposal(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	group := newTestCluster(t, false, func(app *Application, next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/commits" && app.Store.InstanceID != "" {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			}
			next.ServeHTTP(w, r)
		})
	})
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response <- group.request(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:held","kind":"host"}]}}`)
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := group.nodes[leader].cluster.Node.Drain(ctx)
	cancel()
	if err == nil {
		t.Error("drain succeeded before an in-flight proposal completed")
	}
	close(release)
	if result := <-response; result.Code != http.StatusOK {
		t.Fatalf("drain interrupted confirmed write: %d %s", result.Code, result.Body.String())
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:held", "", http.StatusOK)

	request := httptest.NewRequest("POST", "/raft/message", bytes.NewReader(nil))
	request.Header.Set("Authorization", "Bearer "+group.nodes[leader].cfg.Raft.Token)
	request.Header.Set("X-Raft-Cluster", group.nodes[leader].cfg.Raft.ClusterID)
	for _, protocol := range []string{"", "3"} {
		request.Header.Set("X-GraphDB-Raft-Protocol", protocol)
		w := httptest.NewRecorder()
		group.nodes[leader].cluster.PrivateHandler().ServeHTTP(w, request)
		if w.Code != http.StatusUpgradeRequired {
			t.Fatalf("unqualified protocol %q accepted: %d", protocol, w.Code)
		}
	}
}

func TestHADrainRequiresFreshReplicationOnRemainingVoters(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	checkpoint, _ := group.nodes[leader].files.ReplicationCheckpoint()
	for i := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
	}
	draining, broken := (leader+1)%3, (leader+2)%3
	// Health HTTP remains reachable and all checkpoints initially match. The
	// leader's only working replication peer is the node about to be stopped.
	group.nodes[broken].blockedMessages.Store(true)
	defer group.nodes[broken].blockedMessages.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := group.nodes[draining].cluster.Node.Drain(ctx); err == nil {
		t.Fatal("drain would leave a leader with no working replication peer")
	}
	if group.nodes[draining].cluster.Node.Draining() {
		t.Fatal("unsafe drain removed the only healthy voting peer")
	}
}
