package partition

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type ReadRequest struct {
	Tenant     string   `json:"tenant_id"`
	Generation int64    `json:"generation"`
	Partition  uint8    `json:"partition"`
	Epoch      uint64   `json:"epoch"`
	Root       Root     `json:"root"`
	Keys       []string `json:"keys,omitempty"`
	Prefix     string   `json:"prefix,omitempty"`
	Start      string   `json:"start,omitempty"`
	Limit      int      `json:"limit,omitempty"`
}
type ReadResult struct {
	Rows      []Row    `json:"rows"`
	Next      string   `json:"next,omitempty"`
	Remaining []string `json:"remaining,omitempty"`
}

// Read serves a root selected by the trusted graph coordinator, never a
// client-provided root. Page and byte limits apply before returning any batch.
func (s Store) Read(ctx context.Context, r ReadRequest) (ReadResult, error) {
	if storage.ValidateTenantID(r.Tenant) != nil || r.Generation <= 0 || r.Epoch == 0 || len(r.Keys) > 128 || r.Limit < 0 || r.Limit > 256 {
		return ReadResult{}, invalid("invalid fragment read scope or budget")
	}
	state, err := s.Data(ctx, r.Tenant, r.Generation, r.Partition)
	if err != nil {
		return ReadResult{}, err
	}
	if state.Epoch != r.Epoch || state.State != "active" && state.State != "frozen" {
		return ReadResult{}, conflict("partition epoch changed")
	}
	tree := s.Tree(r.Tenant, r.Generation, r.Partition)
	if len(r.Keys) > 1 {
		tree.cache = &treeReadCache{}
	}
	out := ReadResult{Rows: []Row{}}
	bytes := 0
	if len(r.Keys) > 0 {
		for i, key := range r.Keys {
			if len(key) > 4096 {
				return ReadResult{}, invalid("fragment key exceeds budget")
			}
			value, found, err := tree.Get(ctx, r.Root, key)
			if err != nil {
				return ReadResult{}, err
			}
			if !found {
				continue
			}
			if bytes+len(key)+len(value) > 4<<20 {
				out.Remaining = r.Keys[i:]
				break
			}
			out.Rows = append(out.Rows, Row{Key: key, Value: value})
			bytes += len(key) + len(value)
		}
		return out, nil
	}
	limit := r.Limit
	if limit == 0 {
		limit = 128
	}
	err = tree.Scan(ctx, r.Root, r.Prefix, r.Start, func(row Row) (bool, error) {
		if len(out.Rows) == limit || bytes+len(row.Key)+len(row.Value) > 4<<20 {
			out.Next = row.Key
			return false, nil
		}
		out.Rows = append(out.Rows, row)
		bytes += len(row.Key) + len(row.Value)
		return true, nil
	})
	return out, err
}

type Remote interface {
	Transaction(context.Context, Group, string, int64, string) (Transaction, error)
	Pin(context.Context, Group, string, int64, int64, string, string) (PinnedView, error)
	Describe(context.Context, Group, string) (Graph, error)
	Input(context.Context, Group, string, int64, Input) ([]byte, error)
	Read(context.Context, Group, ReadRequest) (ReadResult, error)
	Action(context.Context, Group, Action, any) error
}

func (g Graph) GroupFor(bucket uint8) (Group, error) {
	id := g.Partitions[bucket].Group
	group, ok := g.Groups[id]
	if !ok || group.ID != id || group.ClusterID == "" || len(group.Peers) < 3 {
		return Group{}, fmt.Errorf("partition %02x has an unavailable group", bucket)
	}
	return group, nil
}

type View struct {
	Graph  Graph
	Remote Remote
	Budget *ReadBudget
}

var ErrVersionNotReady = errors.New("graph has not reached the requested minimum version")
var ErrQuotaExceeded = errors.New("graph quota exceeded")

var ErrReadBudget = errors.New("distributed read budget exceeded")

type ReadBudget struct {
	mu       sync.Mutex
	Requests int
	Bytes    int64
}

func (v View) Read(ctx context.Context, bucket uint8, r ReadRequest) (ReadResult, error) {
	if v.Budget != nil {
		v.Budget.mu.Lock()
		v.Budget.Requests--
		remaining := v.Budget.Requests
		v.Budget.mu.Unlock()
		if remaining < 0 {
			return ReadResult{}, ErrReadBudget
		}
	}
	group, err := v.Graph.GroupFor(bucket)
	if err != nil {
		return ReadResult{}, err
	}
	r.Tenant = v.Graph.Tenant
	r.Generation = v.Graph.Generation
	r.Partition = bucket
	r.Epoch = v.Graph.Partitions[bucket].Epoch
	r.Root = v.Graph.Roots[bucket]
	result, err := v.Remote.Read(ctx, group, r)
	if err == nil && v.Budget != nil {
		bytes := int64(0)
		for _, row := range result.Rows {
			bytes += int64(len(row.Key) + len(row.Value))
		}
		v.Budget.mu.Lock()
		v.Budget.Bytes -= bytes
		remaining := v.Budget.Bytes
		v.Budget.mu.Unlock()
		if remaining < 0 {
			return ReadResult{}, ErrReadBudget
		}
	}
	return result, err
}

func (v View) Get(ctx context.Context, bucket uint8, key string, value any) (bool, error) {
	result, err := v.Read(ctx, bucket, ReadRequest{Keys: []string{key}})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, nil
	}
	if len(result.Rows) != 1 || result.Rows[0].Key != key {
		return false, fmt.Errorf("fragment returned an unexpected key")
	}
	return true, decodeRecord(result.Rows[0], value)
}
