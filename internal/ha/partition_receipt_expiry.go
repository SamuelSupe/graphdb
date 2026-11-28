package ha

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (r acceptedRequest) receiptExpiresAt() time.Time {
	if r.ReceiptRetention <= 0 || r.FinishedAt.IsZero() || r.Result == nil || (r.State != "committed" && r.State != "failed") {
		return time.Time{}
	}
	return r.FinishedAt.Add(r.ReceiptRetention)
}

func (a *Application) managedReceiptPrefix() string {
	return path.Join(a.Store.Prefix, "control", "partition-ingest-receipts") + "/"
}

func (a *Application) managedReceiptKey(key string) string {
	ref := partitionIngestReference(key)
	if len(ref) != 64 || key != a.ingestPrefix()+ref+".json" {
		return ""
	}
	return a.managedReceiptPrefix() + ref[:2] + "/" + ref + ".json"
}

func validManagedReceiptKey(prefix, key string) bool {
	ref := partitionIngestReference(key)
	if len(ref) != 64 || key != prefix+ref[:2]+"/"+ref+".json" {
		return false
	}
	var digest [32]byte
	_, err := hex.Decode(digest[:], []byte(ref))
	return err == nil
}

type expiringPartitionReceipt struct {
	Key   string `json:"key"`
	Index uint64 `json:"accepted_lsn"`
	Epoch uint64 `json:"accepted_epoch,omitempty"`
}

func (c *Cluster) receiptExpiryCommand() string {
	if c.App.ShardID == "" {
		return "ordinary_ingest_expire"
	}
	if c.config.Protocol >= ingestQueueCheckpointProtocol {
		return "shard_ingest_expire"
	}
	return "partition_ingest_expire"
}

func (c *Cluster) receiptExpiryStep(ctx context.Context, cursor string) (next string, err error) {
	finish := c.metrics.Start(c.receiptExpiryMetric())
	defer func() { finish(err) }()
	if err := c.Node.ReadBarrier(ctx); err != nil {
		return "", err
	}
	objects, next, err := c.partitionReceiptPage(ctx, c.App.managedReceiptPrefix(), cursor)
	if err != nil || len(objects) == 0 {
		return next, err
	}
	cmd, err := newCommand(c.receiptExpiryCommand())
	if err != nil {
		return "", err
	}
	var candidates []expiringPartitionReceipt
	for _, object := range objects {
		// Listing runs outside the application lock. Only the Raft command can
		// delete a receipt, after rechecking its deadline and acceptance LSN.
		record, err := loadAcceptance(ctx, c.App.Store.Objects, object.Key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return "", err
		}
		if expiry := record.receiptExpiresAt(); !expiry.IsZero() && !cmd.At.Before(expiry) {
			candidates = append(candidates, expiringPartitionReceipt{Key: object.Key, Index: record.Index, Epoch: record.AcceptedEpoch})
		}
	}
	if len(candidates) == 0 {
		return next, nil
	}
	_, err = c.proposeReceiptExpiry(ctx, cmd, candidates)
	return next, err
}

func (c *Cluster) proposeReceiptExpiry(ctx context.Context, cmd command, candidates []expiringPartitionReceipt) ([]string, error) {
	var err error
	cmd.Body, err = json.Marshal(candidates)
	if err != nil {
		return nil, err
	}
	data, err := c.propose(ctx, cmd)
	if err != nil {
		return nil, err
	}
	var response httpResult
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	if response.Status != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", cmd.Kind, response.Body)
	}
	var result struct {
		Deferred []string `json:"deferred"`
	}
	if err := json.Unmarshal(response.Body, &result); err != nil {
		return nil, err
	}
	return result.Deferred, nil
}

func (a *Application) expireIngestReceipts(ctx context.Context, cmd command) ([]byte, error) {
	sharded := cmd.Kind == "shard_ingest_expire"
	partitioned := cmd.Kind == "partition_ingest_expire"
	var candidates []expiringPartitionReceipt
	if (partitioned || sharded) != (a.ShardID != "") || a.Catalog || json.Unmarshal(cmd.Body, &candidates) != nil || len(candidates) == 0 || len(candidates) > partitionReceiptPage {
		return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid receipt expiry batch"})
	}
	for _, candidate := range candidates {
		if candidate.Index == 0 || !validManagedReceiptKey(a.managedReceiptPrefix(), candidate.Key) {
			return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid receipt expiry identity"})
		}
	}
	count := 0
	seen := make(map[string]bool)
	var deletes []string
	var deferred []string
	var tenantReceipts []storage.IngestBatchRecord
	for _, candidate := range candidates {
		if seen[candidate.Key] {
			continue
		}
		record, err := loadAcceptance(ctx, a.Store.Objects, candidate.Key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		expiry := record.receiptExpiresAt()
		canonical := a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation)
		if (!sharded && record.Partitioned != partitioned) || record.Index != candidate.Index || record.AcceptedEpoch != candidate.Epoch ||
			expiry.IsZero() || cmd.At.Before(expiry) || a.managedReceiptKey(canonical) != candidate.Key ||
			storage.ValidateTenantID(record.Tenant) != nil || record.Generation < 1 || storage.ValidateIngestStorageIdentifiers(record.Request) != nil {
			continue
		}
		if sharded && !record.Partitioned {
			owner, err := a.ownership(ctx, record.Tenant)
			if err != nil {
				return nil, err
			}
			if owner.State != "active" {
				deferred = append(deferred, candidate.Key)
				continue
			}
		}
		if !record.Partitioned {
			tenantReceipts = append(tenantReceipts, storage.IngestBatchRecord{
				TenantID: record.Tenant, Request: record.Request,
				ReplicatedReceipt: &storage.ReplicatedIngestReceipt{
					Generation: record.Generation, Index: record.Index, Epoch: record.AcceptedEpoch, Digest: record.Digest,
				},
			})
		}
		seen[candidate.Key] = true
		alias, err := a.receiptAliasKey(ctx, record, canonical)
		if err != nil {
			return nil, err
		}
		if alias != "" {
			deletes = append(deletes, alias)
		}
		deletes = append(deletes, candidate.Key)
		count++
	}
	if err := a.Store.ReclaimReplicatedIngestReceipts(ctx, tenantReceipts); err != nil {
		return nil, err
	}
	if err := a.Files.JournalReplicatedObjects(ctx, deletes); err != nil {
		return nil, err
	}
	for _, key := range deletes {
		if err := a.Store.Objects.Delete(ctx, key); err != nil {
			return nil, err
		}
		a.untrackTenantReceipt(key)
	}
	if len(deferred) == 0 {
		return resultJSON(http.StatusOK, map[string]any{"expired": count})
	}
	return resultJSON(http.StatusOK, map[string]any{"expired": count, "deferred": deferred})
}

func (a *Application) receiptAliasKey(ctx context.Context, record acceptedRequest, canonical string) (string, error) {
	if record.Request.IdempotencyKey == "" {
		return "", nil
	}
	alias, found, err := a.ingestAlias(ctx, record.Tenant, record.Generation, record.Request)
	if err != nil || !found || alias.Key != canonical || alias.Index != record.Index || alias.Epoch != record.AcceptedEpoch {
		return "", err
	}
	return a.ingestAliasKey(record.Tenant, record.Generation, record.Request), nil
}
