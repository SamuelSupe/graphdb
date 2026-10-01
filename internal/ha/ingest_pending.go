package ha

import (
	"context"
	"encoding/json"
	"maps"
)

type pendingAcceptance struct {
	tenant       string
	generation   int64
	index        uint64
	requestBytes int64
	objectBytes  int64
}

func pendingInfo(record acceptedRequest, objectBytes int64) (pendingAcceptance, error) {
	request, err := json.Marshal(record.Request)
	return pendingAcceptance{tenant: record.Tenant, generation: record.Generation,
		index: record.Index, requestBytes: int64(len(request)), objectBytes: objectBytes}, err
}

// The application lock protects this index. Keep only active queue metadata;
// payloads and completed idempotency records remain in the durable store.
func (a *Application) ensurePending(ctx context.Context) error {
	if a.pending != nil {
		return nil
	}
	objects, err := a.Store.Objects.List(ctx, a.ingestPrefix())
	if err != nil {
		return err
	}
	pending := make(map[string]pendingAcceptance)
	var size int64
	for _, object := range objects {
		record, err := a.accepted(ctx, object.Key)
		if err != nil {
			return err
		}
		if record.State != "accepted" {
			continue
		}
		info, err := pendingInfo(record, object.Size)
		if err != nil {
			return err
		}
		pending[object.Key] = info
		size += info.objectBytes
	}
	a.pending, a.pendingBytes = pending, size
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
