package partition

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type ReadRequest struct {
	Kind       string   `json:"kind,omitempty"`
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
const ReadBatchParts = 32
const fragmentReadBytes = 4 << 20

func (s Store) Read(ctx context.Context, r ReadRequest) (ReadResult, error) {
	return s.read(ctx, r, fragmentReadBytes)
}

func (s Store) ReadBatch(ctx context.Context, requests []ReadRequest) ([]ReadResult, error) {
	if len(requests) == 0 || len(requests) > ReadBatchParts {
		return nil, invalid("invalid fragment read batch size")
	}
	results := make([]ReadResult, 0, len(requests))
	remaining := fragmentReadBytes
	for _, request := range requests {
		if request.Tenant != requests[0].Tenant || request.Generation != requests[0].Generation {
			return nil, invalid("fragment batch crosses a graph generation")
		}
		if remaining < MaxValueBytes+4096 {
			break
		}
		result, err := s.read(ctx, request, remaining)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
		remaining -= result.rawBytes()
	}
	return results, nil
}

func (r ReadResult) rawBytes() int {
	size := 0
	for _, row := range r.Rows {
		size += len(row.Key) + len(row.Value)
	}
	return size
}

func (s Store) read(ctx context.Context, r ReadRequest, byteLimit int) (ReadResult, error) {
	if storage.ValidateTenantID(r.Tenant) != nil || r.Generation <= 0 || r.Epoch == 0 || len(r.Keys) > 128 || r.Limit < 0 || r.Limit > 256 || r.Kind != "" && (len(r.Keys) != 0 || r.Prefix != Key("entity_scan")) {
		return ReadResult{}, invalid("invalid fragment read scope or budget")
	}
	state, err := s.Data(ctx, r.Tenant, r.Generation, r.Partition)
	if err != nil {
		return ReadResult{}, err
	}
	if state.Epoch != r.Epoch || state.State != "active" && state.State != "frozen" {
		return ReadResult{}, errors.Join(storage.ErrConflict, ErrPlacementChanged)
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
			if bytes+len(key)+len(value) > byteLimit {
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
	scanned := 0
	err = tree.Scan(ctx, r.Root, r.Prefix, r.Start, func(row Row) (bool, error) {
		if r.Kind != "" {
			if scanned == 2048 {
				out.Next = row.Key
				return false, nil
			}
			scanned++
			var entry entityScanEntry
			if err := decodeRecord(row, &entry); err != nil {
				return false, fmt.Errorf("%w: malformed entity directory record: %v", ErrPartitionCorrupt, err)
			}
			if entry.Kind != r.Kind {
				return true, nil
			}
		}
		if len(out.Rows) == limit || bytes+len(row.Key)+len(row.Value) > byteLimit {
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
var ErrPlacementChanged = errors.New("partition placement changed")
var ErrShardUnavailable = errors.New("partition group unavailable")
var ErrGraphBusy = errors.New("graph is busy")
var ErrPartitionCorrupt = errors.New("partition data is corrupt")

var ErrQuotaExceeded = errors.New("graph quota exceeded")

var ErrReadBudget = errors.New("distributed read budget exceeded")

type ReadBudget struct {
	mu       sync.Mutex
	Requests int
	Bytes    int64
}

func (b *ReadBudget) charge(requests, bytes int) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Requests -= requests
	b.Bytes -= int64(bytes)
	if b.Requests < 0 || b.Bytes < 0 {
		return ErrReadBudget
	}
	return nil
}

func (v View) readScope(bucket uint8, r ReadRequest) ReadRequest {
	r.Tenant = v.Graph.Tenant
	r.Generation = v.Graph.Generation
	r.Partition = bucket
	r.Epoch = v.Graph.Partitions[bucket].Epoch
	r.Root = v.Graph.Roots[bucket]
	return r
}

func (v View) Read(ctx context.Context, bucket uint8, r ReadRequest) (ReadResult, error) {
	if err := v.Budget.charge(1, 0); err != nil {
		return ReadResult{}, err
	}
	group, err := v.Graph.GroupFor(bucket)
	if err != nil {
		return ReadResult{}, err
	}
	result, err := v.Remote.Read(ctx, group, v.readScope(bucket, r))
	if err == nil {
		err = v.Budget.charge(0, result.rawBytes())
	}
	return result, err
}

type batchReader interface {
	ReadBatch(context.Context, Group, []ReadRequest) ([]ReadResult, error)
}

// readBatch preserves input order while combining small immutable fragment
// reads into one RPC per group and counting the actual RPCs against the budget.
func (v View) readBatch(ctx context.Context, requests []ReadRequest) ([]ReadResult, error) {
	results := make([]ReadResult, len(requests))
	remote, ok := v.Remote.(batchReader)
	if !ok {
		for i, request := range requests {
			result, err := v.Read(ctx, request.Partition, request)
			if err != nil {
				return nil, err
			}
			results[i] = result
		}
		return results, nil
	}
	indices := map[string][]int{}
	groups := map[string]Group{}
	for i, request := range requests {
		group, err := v.Graph.GroupFor(request.Partition)
		if err != nil {
			return nil, err
		}
		indices[group.ID] = append(indices[group.ID], i)
		groups[group.ID] = group
	}
	for id, pending := range indices {
		for len(pending) > 0 {
			if err := v.Budget.charge(1, 0); err != nil {
				return nil, err
			}
			batch := make([]ReadRequest, min(ReadBatchParts, len(pending)))
			for i := range batch {
				request := requests[pending[i]]
				batch[i] = v.readScope(request.Partition, request)
			}
			received, err := remote.ReadBatch(ctx, groups[id], batch)
			if err != nil {
				return nil, err
			}
			if len(received) == 0 || len(received) > len(batch) {
				return nil, fmt.Errorf("%w: fragment batch made no progress", ErrPartitionCorrupt)
			}
			for i, result := range received {
				if err := v.Budget.charge(0, result.rawBytes()); err != nil {
					return nil, err
				}
				results[pending[i]] = result
			}
			pending = pending[len(received):]
		}
	}
	return results, nil
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
