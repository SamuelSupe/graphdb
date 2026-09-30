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
	"time"
)

type Client struct {
	Token string
	HTTP  *http.Client
}

type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("shard HTTP %d: %s", e.Status, e.Body) }

func NewClient(token string) *Client {
	return &Client{Token: token, HTTP: &http.Client{
		Transport:     &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, MaxIdleConnsPerHost: 16, IdleConnTimeout: time.Minute, ResponseHeaderTimeout: time.Minute},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (c *Client) Leader(ctx context.Context, shard Shard) (string, error) {
	ids := make([]uint64, 0, len(shard.Peers))
	for id := range shard.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		probeCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		response, err := c.request(probeCtx, shard, shard.Peers[id], http.MethodGet, "/raft/status", nil)
		if err != nil {
			cancel()
			continue
		}
		var status struct {
			LeaderID uint64            `json:"leader_id"`
			Peers    map[uint64]string `json:"peers"`
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&status)
		response.Body.Close()
		cancel()
		if err == nil && response.StatusCode == http.StatusOK && shard.Peers[status.LeaderID] != "" {
			return strings.TrimRight(shard.Peers[status.LeaderID], "/"), nil
		}
		if err == nil && response.StatusCode == http.StatusOK {
			origin := status.Peers[status.LeaderID]
			u, parseErr := url.Parse(origin)
			if parseErr == nil && status.LeaderID > 0 && u.Scheme == "http" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && (u.Path == "" || u.Path == "/") {
				return strings.TrimRight(origin, "/"), nil
			}
		}
	}
	return "", fmt.Errorf("no leader discovered for shard %s", shard.ID)
}

func (c *Client) request(ctx context.Context, shard Shard, origin, method, path string, data []byte) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(origin, "/")+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+c.Token)
	r.Header.Set("X-Raft-Cluster", shard.ClusterID)
	r.Header.Set("Content-Type", "application/json")
	return c.HTTP.Do(r)
}

func (c *Client) Do(ctx context.Context, shard Shard, method, path string, body any) (*http.Response, error) {
	var data []byte
	var err error
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
	return c.request(ctx, shard, origin, method, path, data)
}

func (c *Client) JSON(ctx context.Context, shard Shard, method, path string, body, result any) error {
	response, err := c.Do(ctx, shard, method, path, body)
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
