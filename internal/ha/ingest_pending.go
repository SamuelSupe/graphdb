package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const ingestQueueCheckpointProtocol = 13

type pendingAcceptance struct {
	alias        string
	partitioned  bool
	indexedTxn   bool
	tenant       string
	generation   int64
	index        uint64
	requestBytes int64
	objectBytes  int64
}

func (a *Application) pendingInfo(record acceptedRequest, objectBytes int64) (pendingAcceptance, error) {
	request, err := json.Marshal(record.Request)
	var alias string
	if !record.Partitioned && record.Request.IdempotencyKey != "" {
		alias = a.ingestAliasKey(record.Tenant, record.Generation, record.Request)
	}
	return pendingAcceptance{alias: alias, partitioned: record.Partitioned, indexedTxn: record.ReceiptRetention > 0, tenant: record.Tenant, generation: record.Generation,
		index: record.Index, requestBytes: int64(len(request)), objectBytes: objectBytes}, err
}

func (a *Application) resetPending() {
	a.pending = nil
	a.pendingIngestAliases = nil
	a.pendingBytes = 0
	a.pendingIndex = pendingReceiptIndex{}
	a.resetTenantReceipts()
}

// The application lock protects this index. Keep only active queue metadata;
// payloads and completed idempotency records remain in the durable store.
func (a *Application) ensurePending(ctx context.Context) error {
	if a.pending != nil {
		return nil
	}
	var hint *storage.ReplicationIngestQueue
	if a.Files != nil {
		checkpoint, err := a.Files.ReplicationCheckpoint()
		if err != nil {
			return err
		}
		hint = checkpoint.IngestQueue
	}
	var keys []string
	var err error
	if hint != nil {
		keys, err = a.pendingCheckpointKeys(ctx, hint)
		if err != nil && !errors.Is(err, errPendingIndex) {
			return err
		}
	}
	indexed := hint != nil && err == nil
	if !indexed {
		objects, err := a.Store.Objects.List(ctx, a.ingestPrefix())
		if err != nil {
			return err
		}
		keys = make([]string, 0, len(objects))
		for _, object := range objects {
			keys = append(keys, object.Key)
		}
	}
	pending := make(map[string]pendingAcceptance)
	aliases := make(map[string][]string)
	var size int64
	for _, key := range keys {
		record, bytes, err := loadAcceptanceWithSize(ctx, a.Store.Objects, key)
		if err != nil {
			return err
		}
		if record.State != "accepted" {
			if indexed {
				return fmt.Errorf("pending checkpoint references a terminal receipt %q", key)
			}
			continue
		}
		if !indexed && a.Files != nil {
			name := path.Base(key)
			if !validPendingName(name) || key != a.ingestPrefix()+name {
				return fmt.Errorf("invalid pending receipt key %q", key)
			}
		}
		info, err := a.pendingInfo(record, bytes)
		if err != nil {
			return err
		}
		pending[key] = info
		if info.alias != "" {
			aliases[info.alias] = append(aliases[info.alias], key)
		}
		size += info.objectBytes
	}
	if indexed && len(pending) != hint.Count {
		return fmt.Errorf("pending queue differs from replication checkpoint")
	}
	a.pendingIndex = pendingReceiptIndex{}
	if indexed && hint.Spilled {
		a.pendingIndex.group(pending)
		clear(a.pendingIndex.dirty)
	}
	a.pendingIndex.current = indexed
	a.pending, a.pendingBytes, a.pendingIngestAliases = pending, size, aliases
	a.observePending()
	return nil
}

func (a *Application) pendingIngestAlias(ctx context.Context, tenant string, generation int64, request storage.IngestRequest) (ingestAlias, bool, error) {
	var key string
	var index uint64
	for _, candidate := range a.pendingIngestAliases[a.ingestAliasKey(tenant, generation, request)] {
		if current := a.pending[candidate].index; current != 0 && (key == "" || current < index) {
			key, index = candidate, current
		}
	}
	if key == "" {
		return ingestAlias{}, false, nil
	}
	record, err := loadAcceptance(ctx, a.Store.Objects, key)
	return ingestAlias{Key: key, Digest: ingestAliasDigest(record.Request), Index: index, Epoch: record.AcceptedEpoch}, err == nil, err
}

// Return with the read lock held, including after rebuilding the durable queue.
func (a *Application) lockPending(ctx context.Context) error {
	for {
		a.mu.RLock()
		if a.pending != nil {
			return nil
		}
		a.mu.RUnlock()
		a.mu.Lock()
		err := a.ensurePending(ctx)
		a.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

func (a *Application) pendingSnapshot(ctx context.Context) (map[string]pendingAcceptance, error) {
	if err := a.lockPending(ctx); err != nil {
		return nil, err
	}
	defer a.mu.RUnlock()
	return maps.Clone(a.pending), nil
}

func (a *Application) pendingFlushGroups(ctx context.Context, tenant string) (map[string][]pendingFlushRequest, error) {
	if err := a.lockPending(ctx); err != nil {
		return nil, err
	}
	defer a.mu.RUnlock()
	var groups map[string][]pendingFlushRequest
	for key, record := range a.pending {
		if record.partitioned || (tenant != "" && record.tenant != tenant) {
			continue
		}
		if groups == nil {
			groups = make(map[string][]pendingFlushRequest)
		}
		groups[record.tenant] = append(groups[record.tenant], pendingFlushRequest{key, record})
	}
	return groups, nil
}
