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

type pendingAcceptance struct {
	partitioned  bool
	indexedTxn   bool
	tenant       string
	generation   int64
	index        uint64
	requestBytes int64
	objectBytes  int64
}

func pendingInfo(record acceptedRequest, objectBytes int64) (pendingAcceptance, error) {
	request, err := json.Marshal(record.Request)
	return pendingAcceptance{partitioned: record.Partitioned, indexedTxn: record.ReceiptRetention > 0, tenant: record.Tenant, generation: record.Generation,
		index: record.Index, requestBytes: int64(len(request)), objectBytes: objectBytes}, err
}

func (a *Application) resetPending() {
	a.pending = nil
	a.pendingBytes = 0
	a.pendingIndex = pendingReceiptIndex{}
}

// The application lock protects this index. Keep only active queue metadata;
// payloads and completed idempotency records remain in the durable store.
func (a *Application) ensurePending(ctx context.Context) error {
	if a.pending != nil {
		return nil
	}
	var hint *storage.ReplicationIngestQueue
	if a.Files != nil && a.ShardID != "" {
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
		if !indexed && a.ShardID != "" && a.Files != nil {
			name := path.Base(key)
			if !validPendingName(name) || key != a.ingestPrefix()+name {
				return fmt.Errorf("invalid pending receipt key %q", key)
			}
		}
		info, err := pendingInfo(record, bytes)
		if err != nil {
			return err
		}
		pending[key] = info
		size += info.objectBytes
	}
	if hint != nil && len(pending) != hint.Count {
		return fmt.Errorf("pending queue differs from replication checkpoint")
	}
	a.pendingIndex = pendingReceiptIndex{}
	if indexed && hint.Spilled {
		a.pendingIndex.group(pending)
		clear(a.pendingIndex.dirty)
	}
	a.pendingIndex.current = indexed
	a.pending, a.pendingBytes = pending, size
	a.observePending()
	return nil
}

func (a *Application) pendingSnapshot(ctx context.Context) (map[string]pendingAcceptance, error) {
	a.mu.RLock()
	if a.pending != nil {
		pending := maps.Clone(a.pending)
		a.mu.RUnlock()
		return pending, nil
	}
	a.mu.RUnlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.ensurePending(ctx); err != nil {
		return nil, err
	}
	return maps.Clone(a.pending), nil
}
