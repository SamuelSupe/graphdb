package ha

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	bolt "go.etcd.io/bbolt"
)

const receiptExpiryTimeBytes = 12
const maxReceiptExpiryChanges = 4096

var errReceiptExpiryIndex = errors.New("local partition receipt expiry index")

// This index is disposable. Raft receipts and their acceptance positions
// remain authoritative, including after failover or snapshot installation.
type partitionReceiptExpiryIndex struct {
	db      *bolt.DB
	prefix  string
	ready   bool
	rebuild bool

	mu      sync.Mutex
	changes map[string]struct{}
	rescan  bool
}

func openPartitionReceiptExpiryIndex(dir, prefix string) (*partitionReceiptExpiryIndex, error) {
	file, err := os.CreateTemp(dir, ".tmp-receipt-expiry-")
	if err != nil {
		return nil, err
	}
	name := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(name)
		return nil, err
	}
	db, err := bolt.Open(name, 0600, &bolt.Options{NoSync: true, NoFreelistSync: true})
	if err != nil {
		_ = os.Remove(name)
		return nil, err
	}
	// An unlinked index stays usable until the last descriptor closes.
	if err := os.Remove(name); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &partitionReceiptExpiryIndex{db: db, prefix: prefix}, nil
}

func (i *partitionReceiptExpiryIndex) changed(key string) {
	if !strings.HasPrefix(key, i.prefix) {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.rescan {
		return
	}
	if _, exists := i.changes[key]; exists {
		return
	}
	if len(i.changes) >= maxReceiptExpiryChanges {
		i.changes, i.rescan = nil, true
		return
	}
	if i.changes == nil {
		i.changes = make(map[string]struct{})
	}
	i.changes[key] = struct{}{}
}

func (i *partitionReceiptExpiryIndex) takeChanges() (keys map[string]struct{}, rescan, more bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	rescan, i.rescan = i.rescan, false
	if len(i.changes) <= partitionReceiptPage {
		keys, i.changes = i.changes, nil
		return keys, rescan, false
	}
	keys = make(map[string]struct{}, partitionReceiptPage)
	for key := range i.changes {
		keys[key] = struct{}{}
		delete(i.changes, key)
		if len(keys) == partitionReceiptPage {
			break
		}
	}
	return keys, rescan, true
}

func receiptExpiryTimeKey(t time.Time) [receiptExpiryTimeBytes]byte {
	var key [receiptExpiryTimeBytes]byte
	// Flip the sign bit so dates before the epoch sort before positive dates.
	binary.BigEndian.PutUint64(key[:], uint64(t.Unix())^(1<<63))
	binary.BigEndian.PutUint32(key[8:], uint32(t.Nanosecond()))
	return key
}

type partitionReceiptDeadline struct {
	at    time.Time
	index uint64
	epoch uint64
}

func (i *partitionReceiptExpiryIndex) reset() error {
	err := i.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{[]byte("deadline"), []byte("receipt")} {
			if tx.Bucket(name) != nil {
				if err := tx.DeleteBucket(name); err != nil {
					return err
				}
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return errors.Join(errReceiptExpiryIndex, err)
	}
	i.ready = false
	return nil
}

func (i *partitionReceiptExpiryIndex) update(records map[string]partitionReceiptDeadline) error {
	if len(records) == 0 {
		return nil
	}
	err := i.db.Update(func(tx *bolt.Tx) error {
		deadlines, receipts := tx.Bucket([]byte("deadline")), tx.Bucket([]byte("receipt"))
		for key, record := range records {
			name := []byte(key)
			if previous := receipts.Get(name); previous != nil {
				if err := deadlines.Delete(append(bytes.Clone(previous), name...)); err != nil {
					return err
				}
				if err := receipts.Delete(name); err != nil {
					return err
				}
			}
			if record.at.IsZero() {
				continue
			}
			when := receiptExpiryTimeKey(record.at)
			var position [16]byte
			binary.BigEndian.PutUint64(position[:], record.index)
			binary.BigEndian.PutUint64(position[8:], record.epoch)
			if err := deadlines.Put(append(bytes.Clone(when[:]), name...), position[:]); err != nil {
				return err
			}
			if err := receipts.Put(name, when[:]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return errors.Join(errReceiptExpiryIndex, err)
	}
	return nil
}

func (i *partitionReceiptExpiryIndex) due(at time.Time) ([]expiringPartitionReceipt, error) {
	cutoff := receiptExpiryTimeKey(at)
	var due []expiringPartitionReceipt
	err := i.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte("deadline")).Cursor()
		for key, pos := cursor.First(); key != nil && len(due) < partitionReceiptPage; key, pos = cursor.Next() {
			if len(key) <= receiptExpiryTimeBytes || len(pos) != 16 {
				return errors.New("invalid expiry index entry")
			}
			if bytes.Compare(key[:receiptExpiryTimeBytes], cutoff[:]) > 0 {
				break
			}
			due = append(due, expiringPartitionReceipt{Key: string(key[receiptExpiryTimeBytes:]),
				Index: binary.BigEndian.Uint64(pos), Epoch: binary.BigEndian.Uint64(pos[8:])})
		}
		return nil
	})
	if err != nil {
		return nil, errors.Join(errReceiptExpiryIndex, err)
	}
	return due, nil
}

func (i *partitionReceiptExpiryIndex) refresh(ctx context.Context, objects storage.ObjectStore, keys map[string]struct{}) error {
	records := make(map[string]partitionReceiptDeadline, len(keys))
	for key := range keys {
		if !validManagedReceiptKey(i.prefix, key) {
			continue
		}
		record, err := loadAcceptance(ctx, objects, key)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			records[key] = partitionReceiptDeadline{}
		case err != nil:
			return err
		case record.Index == 0:
			records[key] = partitionReceiptDeadline{}
		default:
			records[key] = partitionReceiptDeadline{at: record.receiptExpiresAt(), index: record.Index, epoch: record.AcceptedEpoch}
		}
	}
	return i.update(records)
}

func (c *Cluster) indexedPartitionReceiptExpiryStep(ctx context.Context, i *partitionReceiptExpiryIndex, cursor string) (next string, err error) {
	finish := c.metrics.Start(c.receiptExpiryMetric())
	defer func() { finish(err) }()
	changes, overflow, more := i.takeChanges()
	i.rebuild = i.rebuild || overflow
	if (i.ready && i.rebuild) || (!i.ready && cursor == "") {
		if err := i.reset(); err != nil {
			return "", err
		}
		storage.ResetFileListPageCache(ctx)
		i.rebuild = false
		cursor = ""
	}
	if err := i.refresh(ctx, c.App.Files, changes); err != nil {
		// The worker dropped this batch of notifications already. Rebuild
		// from authoritative receipts before trusting the index again.
		i.mu.Lock()
		i.rescan = true
		i.mu.Unlock()
		return "", err
	}
	if !i.ready {
		objects, following, err := c.partitionReceiptPage(ctx, i.prefix, cursor)
		if err != nil {
			return "", err
		}
		keys := make(map[string]struct{}, len(objects))
		for _, object := range objects {
			keys[object.Key] = struct{}{}
		}
		if err := i.refresh(ctx, c.App.Files, keys); err != nil {
			return "", err
		}
		next, i.ready = following, following == ""
	}

	at := time.Now().UTC()
	due, err := i.due(at)
	if err != nil {
		return "", err
	}
	if len(due) != 0 {
		if err := c.expireIndexedPartitionReceipts(ctx, i, at, due); err != nil {
			return "", err
		}
	}
	if next == "" && (i.rebuild || more || len(due) == partitionReceiptPage) {
		return i.prefix, nil // Stay on the short cadence to drain the backlog.
	}
	return next, nil
}

func (c *Cluster) expireIndexedPartitionReceipts(ctx context.Context, i *partitionReceiptExpiryIndex, at time.Time, due []expiringPartitionReceipt) error {
	cmd, err := newCommand(c.receiptExpiryCommand())
	if err != nil {
		return err
	}
	cmd.At = at
	deferred, err := c.proposeReceiptExpiry(ctx, cmd, due)
	if err != nil {
		return err
	}
	// Retire candidates even when Raft rejected their stale acceptance
	// positions. File changes during the proposal are queued for the next step.
	processed := make(map[string]partitionReceiptDeadline, len(due))
	for _, record := range due {
		processed[record.Key] = partitionReceiptDeadline{}
		if slices.Contains(deferred, record.Key) {
			processed[record.Key] = partitionReceiptDeadline{at: at.Add(2 * time.Second), index: record.Index, epoch: record.Epoch}
		}
	}
	return i.update(processed)
}

func (c *Cluster) receiptExpiryMetric() string {
	if c.App.ShardID == "" {
		return "ordinary_receipt_expiry"
	}
	return "partition_receipt_expiry"
}

func (c *Cluster) runReceiptExpiry(ctx context.Context) {
	const stepDelay = 200 * time.Millisecond
	legacyInterval := time.Minute
	retention := c.receiptRetention
	if ordinary := c.ordinaryReceiptRetention; ordinary > 0 && (retention == 0 || ordinary < retention) {
		retention = ordinary
	}
	if retention > 0 {
		legacyInterval = min(legacyInterval, max(time.Second, retention/4))
	}
	index, err := openPartitionReceiptExpiryIndex(c.config.Dir, c.App.managedReceiptPrefix())
	if err == nil {
		indexCtx, stop := context.WithCancel(ctx)
		removeListener := c.App.Files.OnChange(index.changed)
		c.runReceiptSweep(indexCtx, "ingest receipt expiry", stepDelay, time.Second, nil,
			func(operation context.Context, cursor string) (string, error) {
				next, stepErr := c.indexedPartitionReceiptExpiryStep(operation, index, cursor)
				if errors.Is(stepErr, errReceiptExpiryIndex) {
					err = stepErr
					stop()
				}
				return next, stepErr
			})
		stop()
		removeListener()
		_ = index.db.Close()
	}
	if ctx.Err() == nil {
		slog.Warn("ingest receipt expiry using scan", "group", c.App.ShardID, "error", err)
		c.runReceiptSweep(ctx, "ingest receipt expiry", stepDelay, legacyInterval, nil, c.receiptExpiryStep)
	}
}
