package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const partitionReceiptPage = 64
const partitionReceiptBytes = 4 << 20

// Compatibility sweeps can wait for a lifecycle change after the first pass.
func (c *Cluster) runReceiptSweep(ctx context.Context, name string, stepDelay, interval time.Duration, wake <-chan struct{}, step func(context.Context, string) (string, error)) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	tick := timer.C
	var changed <-chan struct{}
	var scan context.Context
	cursor := ""
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-changed:
			timer.Reset(time.Second)
			tick, changed = timer.C, nil
			continue
		case <-tick:
		}
		if c.Node.LeaderID() != c.config.ID {
			cursor, scan = "", nil
			timer.Reset(time.Second)
			continue
		}
		if scan == nil {
			scan = storage.WithFileListPageCache(ctx)
		}
		operation, cancel := context.WithTimeout(scan, 3*time.Second)
		next, err := step(operation, cursor)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn(name+" failed", "group", c.App.ShardID, "error", err, "retry_after", backoff)
			}
			timer.Reset(backoff)
			backoff = min(time.Minute, backoff*2)
			continue
		}
		cursor, backoff = next, time.Second
		if cursor == "" {
			scan = nil
			if interval == 0 {
				if wake == nil {
					return
				}
				tick, changed = nil, wake
				continue
			}
			timer.Reset(interval)
		} else {
			timer.Reset(stepDelay)
		}
	}
}

func (c *Cluster) partitionReceiptCleanupStep(ctx context.Context, cursor string) (next string, err error) {
	finish := c.metrics.Start("partition_receipt_cleanup")
	defer func() { finish(err) }()
	if err := c.Node.ReadBarrier(ctx); err != nil {
		return "", err
	}
	objects, next, err := c.partitionReceiptPage(ctx, c.App.ingestPrefix(), cursor)
	if err != nil {
		return "", err
	}
	cmd, err := newCommand("partition_ingest_compact")
	if err != nil {
		return "", err
	}
	for _, object := range objects {
		c.App.mu.RLock()
		_, redundant, readErr := c.App.redundantPartitionIngest(ctx, object.Key)
		c.App.mu.RUnlock()
		if readErr != nil {
			return "", readErr
		}
		if redundant {
			cmd.IDs = append(cmd.IDs, object.Key)
		}
	}
	if len(cmd.IDs) == 0 {
		return next, nil
	}
	return next, c.proposeReceiptMaintenance(ctx, cmd)
}

func (c *Cluster) partitionReceiptPage(ctx context.Context, prefix, cursor string) ([]storage.ObjectInfo, string, error) {
	objects, next, err := c.App.Files.ListPage(ctx, prefix, cursor, partitionReceiptPage)
	if err != nil {
		return nil, "", err
	}
	var size int64
	for i, object := range objects {
		if i > 0 && size+object.Size > partitionReceiptBytes {
			return objects[:i], objects[i-1].Key, nil
		}
		size += object.Size
	}
	return objects, next, nil
}

func (c *Cluster) proposeReceiptMaintenance(ctx context.Context, cmd command) error {
	data, err := c.propose(ctx, cmd)
	if err != nil {
		return err
	}
	var result httpResult
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	if result.Status != http.StatusOK {
		return fmt.Errorf("%s: %s", cmd.Kind, result.Body)
	}
	return nil
}

func (a *Application) redundantPartitionIngest(ctx context.Context, key string) (partition.Transaction, bool, error) {
	record, err := a.accepted(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return partition.Transaction{}, false, nil
	}
	if err != nil {
		return partition.Transaction{}, false, err
	}
	if !record.Partitioned || record.State != "committed" && record.State != "failed" || record.Result == nil ||
		key != a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation) {
		return partition.Transaction{}, false, nil
	}
	txn, err := a.partitionStore().Transaction(ctx, record.Tenant, record.Generation, partitionIngestIdentity(key))
	if errors.Is(err, storage.ErrNotFound) {
		return partition.Transaction{}, false, nil
	}
	if err != nil {
		return partition.Transaction{}, false, err
	}
	if txn.Purpose != "ingest" || txn.State != "committed" && txn.State != "aborted" ||
		len(txn.Inputs) != 0 || len(txn.Participants) != 0 || !reflect.DeepEqual(txn.IngestResult, record.Result) {
		return partition.Transaction{}, false, nil
	}
	g, err := a.partitionStore().GetGraph(ctx, record.Tenant)
	if errors.Is(err, storage.ErrNotFound) {
		return partition.Transaction{}, false, nil
	}
	return txn, err == nil && g.Generation == record.Generation && (g.Active == nil || g.Active.ID != txn.ID), err
}

func (a *Application) compactPartitionIngest(ctx context.Context, cmd command) ([]byte, error) {
	if a.ShardID == "" || a.Catalog || len(cmd.IDs) == 0 || len(cmd.IDs) > partitionReceiptPage {
		return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid partition receipt cleanup batch"})
	}
	count := 0
	for _, key := range cmd.IDs {
		ref := partitionIngestReference(key)
		if len(ref) != 64 || key != a.ingestPrefix()+ref+".json" {
			return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid partition receipt cleanup identity"})
		}
		txn, redundant, err := a.redundantPartitionIngest(ctx, key)
		if err != nil {
			return nil, err
		}
		if redundant {
			if err := a.partitionStore().RetireIngestDecision(ctx, txn); err != nil {
				return nil, err
			}
			count++
		}
	}
	return resultJSON(http.StatusOK, map[string]any{"compacted": count})
}
