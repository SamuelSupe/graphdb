package ha

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (a *Application) ordinaryIngestReceipt(ctx context.Context, tenant string, generation int64, source, collector, batch string) (acceptedRequest, error) {
	stored, err := a.Store.GetReplicatedIngestReceipt(ctx, tenant, generation, source, collector, batch)
	if err != nil {
		return acceptedRequest{}, err
	}
	record := sourceIngestReceipt(tenant, generation, stored)
	record.Request = storage.IngestRequest{Source: source, CollectorID: collector, BatchID: batch}
	record.Index, record.Digest = stored.ReplicatedReceipt.Index, stored.ReplicatedReceipt.Digest
	record.Result.SkipReason = stored.ReplicatedReceipt.SkipReason
	return record, nil
}

func (c *Cluster) ordinaryReceiptCleanupStep(ctx context.Context, cursor string) (string, error) {
	if err := c.Node.ReadBarrier(ctx); err != nil {
		return "", err
	}
	prefix := c.App.ingestPrefix()
	managedPrefix := c.App.managedReceiptPrefix()
	if strings.HasPrefix(cursor, managedPrefix) {
		prefix = managedPrefix
	}
	objects, next, err := c.partitionReceiptPage(ctx, prefix, cursor)
	if err != nil {
		return "", err
	}
	var compact, reclaim []string
	deadline, limited := ctx.Deadline()
	for i, object := range objects {
		if i > 0 && limited && time.Until(deadline) < time.Second {
			next = objects[i-1].Key
			break
		}
		c.App.mu.RLock()
		record, size, current, readErr := c.App.readOrdinaryReceipt(ctx, object.Key)
		if readErr == nil && current > record.Generation {
			var obsolete bool
			obsolete, readErr = c.App.obsoleteOrdinaryReceipt(ctx, record)
			if obsolete {
				reclaim = append(reclaim, object.Key)
			}
		} else if readErr == nil && current != 0 && c.App.ShardID == "" && record.ReceiptRetention <= 0 {
			var bound bool
			bound, readErr = c.App.bindOrdinaryIngest(ctx, record, size, current, false)
			if bound {
				compact = append(compact, object.Key)
			}
		}
		c.App.mu.RUnlock()
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if readErr != nil {
			slog.Warn("ordinary ingest receipt could not be verified", "key", object.Key, "error", readErr)
		}
	}
	for _, cleanup := range []struct {
		kind string
		ids  []string
	}{{"ordinary_ingest_reclaim", reclaim}, {"ordinary_ingest_compact", compact}} {
		if len(cleanup.ids) == 0 {
			continue
		}
		cmd, err := newCommand(cleanup.kind)
		if err != nil {
			return "", err
		}
		cmd.IDs = cleanup.ids
		if err := c.proposeReceiptMaintenance(ctx, cmd); err != nil {
			return "", err
		}
	}
	if next == "" && prefix != managedPrefix {
		next = managedPrefix
	}
	return next, nil
}

func (a *Application) readOrdinaryReceipt(ctx context.Context, key string) (acceptedRequest, int64, int64, error) {
	record, size, err := loadAcceptanceWithSize(ctx, a.Store.Objects, key)
	if errors.Is(err, storage.ErrNotFound) {
		return acceptedRequest{}, 0, 0, nil
	}
	if err != nil {
		return acceptedRequest{}, 0, 0, err
	}
	if record.Partitioned || record.Generation < 1 || record.Index == 0 ||
		storage.ValidateTenantID(record.Tenant) != nil || storage.ValidateIngestStorageIdentifiers(record.Request) != nil {
		return acceptedRequest{}, 0, 0, nil
	}
	canonical := a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation)
	if key != canonical && (record.receiptExpiresAt().IsZero() || key != a.managedReceiptKey(canonical)) {
		return acceptedRequest{}, 0, 0, nil
	}
	generation, err := a.Store.ReplicationTenantGeneration(ctx, record.Tenant)
	return record, size, generation, err
}

func (a *Application) bindOrdinaryIngest(ctx context.Context, record acceptedRequest, size, generation int64, publish bool) (bool, error) {
	if record.ReceiptRetention > 0 || generation == 0 || generation != record.Generation || record.State != "committed" && record.State != "failed" ||
		record.Result == nil || (record.State == "failed") != (record.Result.Failed > 0) ||
		record.Error != "" || record.Result.BatchID != record.Request.BatchID {
		return false, nil
	}
	return a.Store.BindReplicatedIngestReceipt(ctx, storage.IngestBatchRecord{
		TenantID: record.Tenant, Request: record.Request, Result: *record.Result,
		StartedAt: record.AcceptedAt, FinishedAt: record.FinishedAt,
		ReplicatedReceipt: &storage.ReplicatedIngestReceipt{
			Generation: record.Generation, Index: record.Index, Digest: record.Digest, SkipReason: record.Result.SkipReason,
		},
	}, size, publish)
}

func (a *Application) obsoleteOrdinaryReceipt(ctx context.Context, record acceptedRequest) (bool, error) {
	if record.State != "accepted" && record.State != "committed" && record.State != "failed" {
		return false, nil
	}
	if a.ShardID == "" {
		return true, nil
	}
	// A migration can install its source's generation over a local tombstone.
	owner, err := a.ownership(ctx, record.Tenant)
	return owner.State == "active", err
}

func validOrdinaryReceiptKeys(prefix, managedPrefix string, keys []string) bool {
	if len(keys) == 0 || len(keys) > partitionReceiptPage {
		return false
	}
	for _, key := range keys {
		if managedPrefix != "" && validManagedReceiptKey(managedPrefix, key) {
			continue
		}
		if name, ok := strings.CutPrefix(key, prefix); !ok || !validPendingName(name) {
			return false
		}
	}
	return true
}

func (a *Application) compactOrdinaryIngest(ctx context.Context, cmd command) ([]byte, error) {
	if a.Catalog || a.ShardID != "" || !validOrdinaryReceiptKeys(a.ingestPrefix(), "", cmd.IDs) {
		return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid ordinary receipt cleanup batch"})
	}
	count := 0
	for _, key := range cmd.IDs {
		record, size, current, err := a.readOrdinaryReceipt(ctx, key)
		if err != nil {
			return nil, err
		}
		bound, err := a.bindOrdinaryIngest(ctx, record, size, current, true)
		if err != nil {
			return nil, err
		}
		if bound {
			if err := a.Store.Objects.Delete(ctx, key); err != nil {
				return nil, err
			}
			a.untrackTenantReceipt(key)
			count++
		}
	}
	return resultJSON(http.StatusOK, map[string]any{"compacted": count})
}

func (a *Application) reclaimOrdinaryIngest(ctx context.Context, cmd command) ([]byte, error) {
	if a.Catalog || !validOrdinaryReceiptKeys(a.ingestPrefix(), a.managedReceiptPrefix(), cmd.IDs) {
		return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid obsolete receipt cleanup batch"})
	}
	count := 0
	for _, key := range cmd.IDs {
		record, _, current, err := a.readOrdinaryReceipt(ctx, key)
		if err != nil {
			return nil, err
		}
		if current <= record.Generation {
			continue
		}
		obsolete, err := a.obsoleteOrdinaryReceipt(ctx, record)
		if err != nil {
			return nil, err
		}
		if !obsolete {
			continue
		}
		canonical := a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation)
		if record.State == "accepted" && a.pending == nil {
			checkpoint, err := a.Files.ReplicationCheckpoint()
			if err != nil {
				return nil, err
			}
			if checkpoint.IngestQueue != nil {
				if err := a.ensurePending(ctx); err != nil {
					return nil, err
				}
			}
		}
		if err := a.Store.Objects.Delete(ctx, key); err != nil {
			return nil, err
		}
		a.untrackTenantReceipt(key)
		if record.ReceiptRetention > 0 {
			alias, err := a.receiptAliasKey(ctx, record, canonical)
			if err != nil {
				return nil, err
			}
			if alias != "" {
				if err := a.Store.Objects.Delete(ctx, alias); err != nil {
					return nil, err
				}
			}
		}
		a.updatePendingAcceptance(canonical, false, pendingAcceptance{})
		count++
	}
	return resultJSON(http.StatusOK, map[string]any{"reclaimed": count})
}
