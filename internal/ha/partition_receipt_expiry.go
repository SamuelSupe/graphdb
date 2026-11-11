package ha

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (r acceptedRequest) receiptExpiresAt() time.Time {
	if !r.Partitioned || r.ReceiptRetention <= 0 || r.FinishedAt.IsZero() || r.Result == nil || (r.State != "committed" && r.State != "failed") {
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

type expiringPartitionReceipt struct {
	Key   string `json:"key"`
	Index uint64 `json:"accepted_lsn"`
}

func (c *Cluster) partitionReceiptExpiryStep(ctx context.Context, cursor string) (next string, err error) {
	finish := c.metrics.Start("partition_receipt_expiry")
	defer func() { finish(err) }()
	if err := c.Node.ReadBarrier(ctx); err != nil {
		return "", err
	}
	objects, next, err := c.partitionReceiptPage(ctx, c.App.managedReceiptPrefix(), cursor)
	if err != nil || len(objects) == 0 {
		return next, err
	}
	cmd, err := newCommand("partition_ingest_expire")
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
			candidates = append(candidates, expiringPartitionReceipt{Key: object.Key, Index: record.Index})
		}
	}
	if len(candidates) == 0 {
		return next, nil
	}
	cmd.Body, err = json.Marshal(candidates)
	if err != nil {
		return "", err
	}
	return next, c.proposeReceiptMaintenance(ctx, cmd)
}

func (a *Application) expirePartitionIngest(ctx context.Context, cmd command) ([]byte, error) {
	var candidates []expiringPartitionReceipt
	if a.ShardID == "" || a.Catalog || json.Unmarshal(cmd.Body, &candidates) != nil || len(candidates) == 0 || len(candidates) > partitionReceiptPage {
		return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid partition receipt expiry batch"})
	}
	for _, candidate := range candidates {
		ref := partitionIngestReference(candidate.Key)
		if _, err := hex.DecodeString(ref); err != nil || len(ref) != 64 || candidate.Index == 0 ||
			candidate.Key != a.managedReceiptPrefix()+ref[:2]+"/"+ref+".json" {
			return resultJSON(http.StatusBadRequest, map[string]any{"error": "invalid partition receipt expiry identity"})
		}
	}
	count := 0
	for _, candidate := range candidates {
		record, err := loadAcceptance(ctx, a.Store.Objects, candidate.Key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		expiry := record.receiptExpiresAt()
		canonical := a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation)
		if record.Index != candidate.Index || expiry.IsZero() || cmd.At.Before(expiry) || a.managedReceiptKey(canonical) != candidate.Key {
			continue
		}
		if record.Request.IdempotencyKey != "" {
			alias, found, err := a.partitionIngestAlias(ctx, record.Tenant, record.Generation, record.Request)
			if err != nil {
				return nil, err
			}
			if found && alias.Key == canonical && alias.Index == record.Index {
				key := a.partitionIngestAliasKey(record.Tenant, record.Generation, record.Request)
				if err := a.Store.Objects.Delete(ctx, key); err != nil {
					return nil, err
				}
			}
		}
		if err := a.Store.Objects.Delete(ctx, candidate.Key); err != nil {
			return nil, err
		}
		count++
	}
	return resultJSON(http.StatusOK, map[string]any{"expired": count})
}
