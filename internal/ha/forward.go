package ha

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type forwardHopKey struct{}

const (
	forwardHopHeader = "X-GraphDB-Raft-Forward-Hop"
	raftAdminHeader  = "X-GraphDB-Raft-Admin"
)

func (c *Cluster) forward(w http.ResponseWriter, r *http.Request, body []byte, private, mutation bool) {
	hops, _ := r.Context().Value(forwardHopKey{}).(int)
	if hops >= 3 {
		http.Error(w, "Raft forwarding limit reached", http.StatusServiceUnavailable)
		return
	}
	address, err := c.Node.LeaderAddress(r.Context())
	if err != nil {
		c.writeError(w, err)
		return
	}
	target, err := url.Parse(address)
	if err != nil {
		c.writeError(w, err)
		return
	}
	r = r.Clone(r.Context())
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	if !private {
		// A leader can change after local routing. The forwarded response owns
		// the routing headers, including the generation of any accepted batch.
		w.Header().Del("X-GraphDB-Shard-ID")
		w.Header().Del(sharding.EpochHeader)
		w.Header().Del("X-GraphDB-Tenant-Generation")
	}
	proxy := &httputil.ReverseProxy{Transport: c.shards.HTTP.Transport, FlushInterval: -1,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.SetURL(target)
			p.Out.Host = p.In.Host
			if !private {
				p.Out.URL.Path = "/cluster/data" + p.In.URL.Path
				if p.In.URL.RawPath != "" {
					p.Out.URL.RawPath = "/cluster/data" + p.In.URL.RawPath
				}
			}
			p.Out.Header.Set("Authorization", "Bearer "+c.config.Token)
			p.Out.Header.Set("X-Raft-Cluster", c.config.ClusterID)
			p.Out.Header.Set(forwardHopHeader, strconv.Itoa(hops+1))
			if storage.IsAdminAuthorized(p.In.Context()) {
				p.Out.Header.Set(raftAdminHeader, "1")
			} else {
				p.Out.Header.Del(raftAdminHeader)
			}
			if mutation {
				sharding.PreventMutationReplay(p.Out)
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			if mutation {
				err = fmt.Errorf("%w: %v", replication.ErrOutcomeUnknown, err)
			}
			c.writeError(w, err)
		},
	}
	if body == nil && r.ContentLength != 0 {
		// Keep a streamed request body available to the transport during an
		// early response flush. Buffered public requests already own a copy.
		_ = http.NewResponseController(w).EnableFullDuplex()
	}
	// A transport failure or a submitted proposal may have committed. Only
	// requests rejected before proposal admission enter this forwarding path.
	proxy.ServeHTTP(w, r)
}

func forwardedContext(r *http.Request) context.Context {
	hops, err := strconv.Atoi(r.Header.Get(forwardHopHeader))
	if err != nil || hops < 0 {
		hops = 0
	}
	return context.WithValue(r.Context(), forwardHopKey{}, hops)
}
