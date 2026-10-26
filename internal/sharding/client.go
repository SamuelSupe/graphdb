package sharding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"golang.org/x/sync/singleflight"
)

type Client struct {
	Token          string
	HTTP           *http.Client
	mu             sync.Mutex
	leaders        map[string]cachedLeader
	metrics        *observability.OperationMetrics
	lastDiscovery  atomic.Int64
	discovery      singleflight.Group
	discoveryEpoch atomic.Uint64
}

type cachedLeader struct {
	origin     string
	definition string
	expires    time.Time
}

type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("shard HTTP %d: %s", e.Status, e.Body) }

func NewClient(token string) *Client {
	return &Client{Token: token, metrics: observability.NewOperationMetrics(), HTTP: &http.Client{
		Transport:     &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, MaxIdleConnsPerHost: 16, IdleConnTimeout: time.Minute, ResponseHeaderTimeout: 10 * time.Minute},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (c *Client) Leader(ctx context.Context, shard Shard) (origin string, err error) {
	finish := c.metrics.Start("leader_lookup")
	defer func() { finish(err) }()
	definition, err := json.Marshal(shard.Peers)
	if err != nil {
		return "", err
	}
	key := shard.ClusterID + "/" + shard.ID
	c.mu.Lock()
	cached := c.leaders[key]
	c.mu.Unlock()
	if cached.definition == string(definition) && time.Now().Before(cached.expires) {
		c.metrics.Event("leader_cache_hit")
		return cached.origin, nil
	}
	c.metrics.Event("leader_cache_miss")
	epoch := c.discoveryEpoch.Load()
	flight := c.discovery.DoChan(fmt.Sprintf("%s/%s/%d", key, definition, epoch), func() (any, error) {
		// One bounded discovery serves independent callers; one caller's cancellation
		// must not abort every waiter. A forgotten result cannot repopulate the cache.
		discoveryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		finishDiscovery := c.metrics.Start("leader_discovery")
		origin, err := c.discoverLeader(discoveryCtx, shard, cached.origin)
		finishDiscovery(err)
		if err != nil {
			return "", err
		}
		c.mu.Lock()
		if c.discoveryEpoch.Load() == epoch {
			if len(c.leaders) >= 1024 || c.leaders == nil {
				c.leaders = make(map[string]cachedLeader)
			}
			c.leaders[key] = cachedLeader{origin: origin, definition: string(definition), expires: time.Now().Add(2 * time.Second)}
		}
		c.mu.Unlock()
		c.lastDiscovery.Store(time.Now().UnixNano())
		return origin, nil
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result := <-flight:
		if result.Err != nil {
			return "", result.Err
		}
		return result.Val.(string), nil
	}
}
func (c *Client) discoverLeader(ctx context.Context, shard Shard, preferred string) (string, error) {
	ids := make([]uint64, 0, len(shard.Peers))
	for id := range shard.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, id := range ids {
		if strings.TrimRight(shard.Peers[id], "/") == preferred {
			ids[0], ids[i] = ids[i], ids[0]
			break
		}
	}
	if len(ids) == 0 {
		return "", fmt.Errorf("no peers for shard %s", shard.ID)
	}
	results := make(chan string, len(ids))
	probe := func(id uint64) {
		probeCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer cancel()
		response, err := c.request(probeCtx, shard, shard.Peers[id], http.MethodGet, "/raft/status", nil)
		if err != nil {
			results <- ""
			return
		}
		defer response.Body.Close()
		var status struct {
			LeaderID uint64            `json:"leader_id"`
			Peers    map[uint64]string `json:"peers"`
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&status)
		if err != nil || response.StatusCode != http.StatusOK {
			results <- ""
			return
		}
		origin := shard.Peers[status.LeaderID]
		if origin == "" {
			origin = status.Peers[status.LeaderID]
		}
		u, err := url.Parse(origin)
		if err != nil || status.LeaderID == 0 || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			results <- ""
			return
		}
		results <- strings.TrimRight(origin, "/")
	}
	go probe(ids[0])
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	received := 0
	select {
	case origin := <-results:
		received++
		if origin != "" {
			return origin, nil
		}
	case <-timer.C:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	// Hedge a slow first peer, with at most four further probes in flight.
	work := make(chan uint64, len(ids)-1)
	for _, id := range ids[1:] {
		work <- id
	}
	close(work)
	for range min(4, len(ids)-1) {
		go func() {
			for id := range work {
				probe(id)
			}
		}()
	}
	for received < len(ids) {
		select {
		case origin := <-results:
			received++
			if origin != "" {
				return origin, nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "", fmt.Errorf("no leader discovered for shard %s", shard.ID)
}

func (c *Client) ForgetLeader(shard Shard) {
	c.metrics.Event("leader_cache_invalidation")
	c.mu.Lock()
	c.discoveryEpoch.Add(1)
	delete(c.leaders, shard.ClusterID+"/"+shard.ID)
	c.mu.Unlock()
}

func (c *Client) request(ctx context.Context, shard Shard, origin, method, path string, data []byte, headers ...http.Header) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(origin, "/")+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for _, values := range headers {
		for key, value := range values {
			r.Header[key] = append([]string(nil), value...)
		}
	}
	r.Header.Set("Authorization", "Bearer "+c.Token)
	r.Header.Set("X-Raft-Cluster", shard.ClusterID)
	r.Header.Set("Content-Type", "application/json")
	if method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions {
		PreventMutationReplay(r)
	}
	return c.HTTP.Do(r)
}

// PreventMutationReplay leaves idempotency headers and payloads intact but stops
// net/http from replaying a mutation after losing a response on a reused socket.
// Even an empty body must be non-rewindable: a header alone does not guarantee
// that the receiving endpoint deduplicates the operation.
func PreventMutationReplay(r *http.Request) {
	r.GetBody = nil
	if r.Body == nil || r.Body == http.NoBody {
		r.Body = io.NopCloser(bytes.NewReader(nil))
		r.ContentLength = -1
	}
}

func (c *Client) Do(ctx context.Context, shard Shard, method, path string, body any, headers ...http.Header) (response *http.Response, err error) {
	operation := "write_request"
	if method == http.MethodGet {
		operation = "read_request"
	}
	finish := c.metrics.Start(operation)
	defer func() {
		observed := err
		if observed == nil && response != nil && response.StatusCode >= 400 {
			observed = &HTTPError{Status: response.StatusCode}
		}
		finish(observed)
	}()
	var data []byte
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	origin, err := c.Leader(ctx, shard)
	if err != nil {
		return nil, err
	}
	// Once sent, a mutation may have committed even if its response is lost.
	// The caller retries by operation identity, never by blindly replaying HTTP.
	response, err = c.request(ctx, shard, origin, method, path, data, headers...)
	if err != nil || response.StatusCode == http.StatusServiceUnavailable {
		c.ForgetLeader(shard)
		if method == http.MethodGet && ctx.Err() == nil {
			c.metrics.Event("read_retry")
			if response != nil {
				response.Body.Close()
			}
			origin, discoverErr := c.Leader(ctx, shard)
			if discoverErr != nil {
				return nil, discoverErr
			}
			return c.request(ctx, shard, origin, method, path, data, headers...)
		}
	}
	return response, err
}

func (c *Client) JSON(ctx context.Context, shard Shard, method, path string, body, result any, headers ...http.Header) error {
	response, err := c.Do(ctx, shard, method, path, body, headers...)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
	if err != nil {
		return err
	}
	if response.StatusCode >= 400 {
		return &HTTPError{Status: response.StatusCode, Body: string(data)}
	}
	if result != nil {
		return json.Unmarshal(data, result)
	}
	return nil
}
