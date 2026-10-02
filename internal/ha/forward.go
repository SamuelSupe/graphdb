package ha

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
)

type forwardHopKey struct{}

const forwardHopHeader = "X-GraphDB-Raft-Forward-Hop"

func (c *Cluster) forward(w http.ResponseWriter, r *http.Request, body []byte, private bool) {
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
	proxy := &httputil.ReverseProxy{Transport: c.shards.HTTP.Transport, FlushInterval: -1,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.SetURL(target)
			if !private {
				p.Out.URL.Path = "/cluster/data" + p.In.URL.Path
				if p.In.URL.RawPath != "" {
					p.Out.URL.RawPath = "/cluster/data" + p.In.URL.RawPath
				}
			}
			p.Out.Header.Set("Authorization", "Bearer "+c.config.Token)
			p.Out.Header.Set("X-Raft-Cluster", c.config.ClusterID)
			p.Out.Header.Set(forwardHopHeader, strconv.Itoa(hops+1))
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) { c.writeError(w, err) },
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
