package ha

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Small queues fit in the existing checkpoint write. Large queues share at
// most 256 journaled objects, rewriting only buckets with changed members.
// Stay in this mode until drained to avoid oscillating around the threshold.
const inlinePendingLimit = 64

var errPendingIndex = errors.New("invalid pending receipt index")

type pendingReceiptIndex struct {
	current bool
	buckets map[string]map[string]bool
	dirty   map[string]bool
}

func (i *pendingReceiptIndex) update(key string, present bool) {
	i.current = false
	if i.buckets == nil {
		return
	}
	name := path.Base(key)
	bucket := name[:2] // ingestKey is a SHA-256 digest followed by .json.
	if present {
		if i.buckets[bucket] == nil {
			i.buckets[bucket] = make(map[string]bool)
		}
		i.buckets[bucket][name] = true
	} else {
		delete(i.buckets[bucket], name)
	}
	i.dirty[bucket] = true
}

func (i *pendingReceiptIndex) group(pending map[string]pendingAcceptance) {
	i.buckets = make(map[string]map[string]bool)
	i.dirty = make(map[string]bool)
	for key := range pending {
		i.update(key, true)
	}
}

func (a *Application) pendingMemberPrefix() string {
	return path.Join(a.Store.Prefix, "control", "replication-ingest-pending") + "/"
}

func validPendingName(name string) bool {
	digest, ok := strings.CutSuffix(name, ".json")
	if !ok || len(digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func (a *Application) pendingCheckpointKeys(ctx context.Context, hint *storage.ReplicationIngestQueue) ([]string, error) {
	names := hint.Keys
	if hint.Count < 0 || hint.Spilled && len(names) != 0 || !hint.Spilled && len(names) > inlinePendingLimit {
		return nil, errPendingIndex
	}
	if hint.Spilled {
		prefix := a.pendingMemberPrefix()
		objects, err := a.Store.Objects.List(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			bucket := strings.TrimPrefix(object.Key, prefix)
			data, err := a.Store.Objects.Get(ctx, object.Key)
			if err != nil {
				return nil, err
			}
			var members map[string]bool
			if len(bucket) != 2 || json.Unmarshal(data, &members) != nil {
				return nil, errPendingIndex
			}
			for name, present := range members {
				if !present || !strings.HasPrefix(name, bucket) {
					return nil, errPendingIndex
				}
				names = append(names, name)
			}
		}
	}
	if len(names) != hint.Count {
		return nil, errPendingIndex
	}
	keys := make([]string, 0, len(names))
	for _, name := range names {
		if !validPendingName(name) {
			return nil, errPendingIndex
		}
		keys = append(keys, a.ingestPrefix()+name)
	}
	return keys, nil
}

// Tenant transfers can replace canonical receipts outside saveAccepted.
func (a *Application) invalidatePendingCheckpoint(ctx context.Context) error {
	a.resetPending()
	return a.Files.SetReplicationIngestQueue(ctx, nil)
}

func (a *Application) checkpointPending(ctx context.Context) error {
	index := &a.pendingIndex
	if index.current {
		return nil
	}
	hint := &storage.ReplicationIngestQueue{Count: len(a.pending)}
	if index.buckets == nil && hint.Count <= inlinePendingLimit {
		for key := range a.pending {
			hint.Keys = append(hint.Keys, path.Base(key))
		}
		slices.Sort(hint.Keys)
	} else {
		prefix := a.pendingMemberPrefix()
		if index.buckets == nil {
			index.group(a.pending)
			// An older writer can leave buckets behind after dropping the hint.
			objects, err := a.Store.Objects.List(ctx, prefix)
			if err != nil {
				return err
			}
			for _, object := range objects {
				index.dirty[strings.TrimPrefix(object.Key, prefix)] = true
			}
		}
		for bucket := range index.dirty {
			key := prefix + bucket
			members := index.buckets[bucket]
			if len(members) == 0 {
				if err := a.Store.Objects.Delete(ctx, key); err != nil {
					return err
				}
				delete(index.buckets, bucket)
			} else {
				data, err := json.Marshal(members)
				if err != nil {
					return fmt.Errorf("encode pending receipt bucket: %w", err)
				}
				if err := a.Store.Objects.Put(ctx, key, data); err != nil {
					return err
				}
			}
		}
		clear(index.dirty)
		hint.Spilled = hint.Count > 0
		if !hint.Spilled {
			*index = pendingReceiptIndex{}
		}
	}
	if err := a.Files.SetReplicationIngestQueue(ctx, hint); err != nil {
		return err
	}
	index.current = true
	return nil
}
