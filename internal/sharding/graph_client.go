package sharding

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type GraphRemote struct{ Client *Client }

func (p GraphRemote) Transaction(ctx context.Context, g partition.Group, tenant string, generation int64, identity string) (partition.Transaction, error) {
	var result partition.Transaction
	err := p.Client.JSON(ctx, graphShard(g), http.MethodGet, "/cluster/graph/transactions/"+url.PathEscape(tenant)+"/"+url.PathEscape(identity)+"?generation="+strconv.FormatInt(generation, 10), nil, &result)
	return result, graphRemoteError(err)
}

func (p GraphRemote) Pin(ctx context.Context, g partition.Group, tenant string, generation, version int64, digest, id string) (partition.PinnedView, error) {
	var result partition.PinnedView
	if id != "" {
		err := p.Client.JSON(ctx, graphShard(g), http.MethodGet, "/cluster/graph/pins/"+url.PathEscape(tenant)+"/"+url.PathEscape(id)+"?query="+url.QueryEscape(digest), nil, &result)
		return result, graphRemoteError(err)
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return result, err
	}
	err := p.Action(ctx, g, partition.Action{Operation: "pin", Tenant: tenant, Generation: generation, BaseVersion: version, Digest: digest, Txn: hex.EncodeToString(random[:])}, &result)
	return result, err
}

func graphShard(g partition.Group) Shard {
	return Shard{ID: g.ID, ClusterID: g.ClusterID, Peers: g.Peers}
}

func graphRemoteError(err error) error {
	if err == nil {
		return nil
	}
	var response *HTTPError
	if errors.As(err, &response) {
		var body struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal([]byte(response.Body), &body)
		switch body.Code {
		case "reader_not_fresh":
			return errors.Join(partition.ErrVersionNotReady, err)
		case "graph_busy":
			return errors.Join(partition.ErrGraphBusy, err)
		case "graph_layout_changed":
			return errors.Join(storage.ErrConflict, partition.ErrPlacementChanged, err)
		case "partition_corrupt":
			return errors.Join(partition.ErrPartitionCorrupt, err)
		}
		switch response.Status {
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
			return errors.Join(storage.ErrInvalidArgument, err)
		case http.StatusConflict:
			return errors.Join(storage.ErrConflict, err)
		case http.StatusNotFound:
			return errors.Join(storage.ErrNotFound, err)
		case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
			return errors.Join(partition.ErrShardUnavailable, err)
		}
		return err
	}
	return errors.Join(partition.ErrShardUnavailable, err)
}

type cachedGraph struct {
	graph partition.Graph
	etag  string
}

func (p GraphRemote) Describe(ctx context.Context, g partition.Group, tenant string) (partition.Graph, error) {
	key := g.ClusterID + "/" + g.ID + "/" + tenant
	p.Client.mu.Lock()
	cached := p.Client.graphDescriptors[key]
	p.Client.mu.Unlock()
	headers := http.Header{}
	if cached.etag != "" {
		headers.Set("If-None-Match", cached.etag)
	}
	response, err := p.Client.Do(ctx, graphShard(g), http.MethodGet, "/cluster/graph/descriptor/"+url.PathEscape(tenant), nil, headers)
	if err != nil {
		return partition.Graph{}, graphRemoteError(err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified {
		return cached.graph, nil
	}
	if response.StatusCode >= 400 {
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			return partition.Graph{}, graphRemoteError(err)
		}
		return partition.Graph{}, graphRemoteError(&HTTPError{Status: response.StatusCode, Body: string(body)})
	}
	var result partition.Graph
	if err := json.NewDecoder(io.LimitReader(response.Body, 32<<20)).Decode(&result); err != nil {
		return result, err
	}
	if etag := response.Header.Get("ETag"); etag != "" {
		p.Client.mu.Lock()
		if p.Client.graphDescriptors == nil || len(p.Client.graphDescriptors) >= 128 {
			p.Client.graphDescriptors = make(map[string]cachedGraph)
		}
		p.Client.graphDescriptors[key] = cachedGraph{graph: result, etag: etag}
		p.Client.mu.Unlock()
	}
	return result, nil
}

func (p GraphRemote) Input(ctx context.Context, g partition.Group, tenant string, generation int64, input partition.Input) ([]byte, error) {
	var result struct {
		Data []byte `json:"data"`
	}
	err := p.Client.JSON(ctx, graphShard(g), http.MethodPost, "/cluster/graph/input", partition.Action{Tenant: tenant, Generation: generation, Inputs: []partition.Input{input}}, &result)
	return result.Data, graphRemoteError(err)
}
func (p GraphRemote) Read(ctx context.Context, g partition.Group, r partition.ReadRequest) (partition.ReadResult, error) {
	var result partition.ReadResult
	err := p.Client.JSON(ctx, graphShard(g), http.MethodPost, "/cluster/graph/read", r, &result)
	return result, graphRemoteError(err)
}
func (p GraphRemote) ReadBatch(ctx context.Context, g partition.Group, requests []partition.ReadRequest) ([]partition.ReadResult, error) {
	var result []partition.ReadResult
	err := p.Client.JSON(ctx, graphShard(g), http.MethodPost, "/cluster/graph/read-batch", requests, &result)

	return result, graphRemoteError(err)
}

func (p GraphRemote) Action(ctx context.Context, g partition.Group, a partition.Action, result any) error {
	return graphRemoteError(p.Client.JSON(ctx, graphShard(g), http.MethodPost, "/cluster/graph/action", a, result))
}
