package sharding

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
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
	var response *HTTPError
	if errors.As(err, &response) {
		switch response.Status {
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
			return errors.Join(storage.ErrInvalidArgument, err)
		case http.StatusConflict:
			return errors.Join(storage.ErrConflict, err)
		case http.StatusNotFound:
			return errors.Join(storage.ErrNotFound, err)
		}
	}
	return err
}

func (p GraphRemote) Describe(ctx context.Context, g partition.Group, tenant string) (partition.Graph, error) {
	var result partition.Graph
	err := p.Client.JSON(ctx, graphShard(g), http.MethodGet, "/cluster/graph/descriptor/"+url.PathEscape(tenant), nil, &result)
	return result, graphRemoteError(err)
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
func (p GraphRemote) Action(ctx context.Context, g partition.Group, a partition.Action, result any) error {
	return graphRemoteError(p.Client.JSON(ctx, graphShard(g), http.MethodPost, "/cluster/graph/action", a, result))
}
