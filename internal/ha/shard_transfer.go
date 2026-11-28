package ha

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

var errInvalidTenantTransfer = errors.New("invalid tenant transfer")

func (a *Application) initEmptyTenantReceipts() {
	if a.ShardID == "" {
		return
	}
	a.tenantReceiptsMu.Lock()
	if a.tenantReceipts == nil {
		a.tenantReceipts = make(map[string]map[string]struct{})
		a.tenantReceiptOwners = make(map[string]string)
	}
	a.tenantReceiptsMu.Unlock()
}

type tenantReceiptDelta struct {
	kind   uint8
	tenant string
	key    string
}

const (
	tenantReceiptDeltaTrack uint8 = iota + 1
	tenantReceiptDeltaUntrackKey
	tenantReceiptDeltaUntrackTenant
	tenantReceiptDeltaReset
)

func applyTenantReceiptTrack(byTenant map[string]map[string]struct{}, owners map[string]string, tenant, key string) {
	if prev, ok := owners[key]; ok && prev != tenant {
		delete(byTenant[prev], key)
		if len(byTenant[prev]) == 0 {
			delete(byTenant, prev)
		}
	}
	if byTenant[tenant] == nil {
		byTenant[tenant] = make(map[string]struct{})
	}
	byTenant[tenant][key] = struct{}{}
	owners[key] = tenant
}

func applyTenantReceiptUntrackKey(byTenant map[string]map[string]struct{}, owners map[string]string, key string) {
	if tenant, ok := owners[key]; ok {
		delete(owners, key)
		delete(byTenant[tenant], key)
		if len(byTenant[tenant]) == 0 {
			delete(byTenant, tenant)
		}
	}
}

func applyTenantReceiptUntrackTenant(byTenant map[string]map[string]struct{}, owners map[string]string, tenant string) {
	for key := range byTenant[tenant] {
		delete(owners, key)
	}
	delete(byTenant, tenant)
}

func (a *Application) resetTenantReceipts() {
	a.tenantReceiptsMu.Lock()
	a.tenantReceipts = nil
	a.tenantReceiptOwners = nil
	if a.tenantReceiptScanning {
		a.tenantReceiptDeltas = append(a.tenantReceiptDeltas, tenantReceiptDelta{kind: tenantReceiptDeltaReset})
	}
	a.tenantReceiptsMu.Unlock()
}

func (a *Application) trackTenantReceipt(tenant, key string) {
	if a.ShardID == "" {
		return
	}
	a.tenantReceiptsMu.Lock()
	defer a.tenantReceiptsMu.Unlock()
	if a.tenantReceipts == nil {
		if a.tenantReceiptScanning {
			a.tenantReceiptDeltas = append(a.tenantReceiptDeltas, tenantReceiptDelta{kind: tenantReceiptDeltaTrack, tenant: tenant, key: key})
		}
		return
	}
	applyTenantReceiptTrack(a.tenantReceipts, a.tenantReceiptOwners, tenant, key)
}

func (a *Application) untrackTenantReceipt(key string) {
	a.tenantReceiptsMu.Lock()
	defer a.tenantReceiptsMu.Unlock()
	if a.tenantReceipts == nil {
		if a.tenantReceiptScanning {
			a.tenantReceiptDeltas = append(a.tenantReceiptDeltas, tenantReceiptDelta{kind: tenantReceiptDeltaUntrackKey, key: key})
		}
		return
	}
	applyTenantReceiptUntrackKey(a.tenantReceipts, a.tenantReceiptOwners, key)
}

func (a *Application) untrackTenantReceiptsForTenant(tenant string) {
	a.tenantReceiptsMu.Lock()
	defer a.tenantReceiptsMu.Unlock()
	if a.tenantReceipts == nil {
		if a.tenantReceiptScanning {
			a.tenantReceiptDeltas = append(a.tenantReceiptDeltas, tenantReceiptDelta{kind: tenantReceiptDeltaUntrackTenant, tenant: tenant})
		}
		return
	}
	applyTenantReceiptUntrackTenant(a.tenantReceipts, a.tenantReceiptOwners, tenant)
}

func (a *Application) ensureTenantReceipts(ctx context.Context) error {
	for {
		a.tenantReceiptsMu.Lock()
		if a.tenantReceipts != nil {
			a.tenantReceiptsMu.Unlock()
			return nil
		}
		if a.tenantReceiptScanning {
			wait := a.tenantReceiptScanWait
			a.tenantReceiptsMu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		wait := make(chan struct{})
		a.tenantReceiptScanning = true
		a.tenantReceiptScanWait = wait
		a.tenantReceiptDeltas = nil
		a.tenantReceiptsMu.Unlock()

		ordinaryPrefix := a.ingestPrefix()
		byTenant := make(map[string]map[string]struct{})
		owners := make(map[string]string)
		var scanErr error
		for _, prefix := range []string{ordinaryPrefix, a.managedReceiptPrefix()} {
			objects, err := a.Files.List(ctx, prefix)
			if err != nil {
				scanErr = err
				break
			}
			for _, object := range objects {
				record, err := loadAcceptance(ctx, a.Store.Objects, object.Key)
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				if err != nil {
					scanErr = err
					break
				}
				if prefix == ordinaryPrefix || (record.AcceptedEpoch != 0 && !record.Partitioned) {
					applyTenantReceiptTrack(byTenant, owners, record.Tenant, object.Key)
				}
			}
			if scanErr != nil {
				break
			}
		}

		a.tenantReceiptsMu.Lock()
		deltas := a.tenantReceiptDeltas
		a.tenantReceiptDeltas = nil
		a.tenantReceiptScanning = false
		a.tenantReceiptScanWait = nil
		if scanErr != nil {
			close(wait)
			a.tenantReceiptsMu.Unlock()
			return scanErr
		}
		resetSeen := false
		for _, d := range deltas {
			if d.kind == tenantReceiptDeltaReset {
				resetSeen = true
				break
			}
		}
		if resetSeen {
			close(wait)
			a.tenantReceiptsMu.Unlock()
			continue
		}
		for _, d := range deltas {
			switch d.kind {
			case tenantReceiptDeltaTrack:
				applyTenantReceiptTrack(byTenant, owners, d.tenant, d.key)
			case tenantReceiptDeltaUntrackKey:
				applyTenantReceiptUntrackKey(byTenant, owners, d.key)
			case tenantReceiptDeltaUntrackTenant:
				applyTenantReceiptUntrackTenant(byTenant, owners, d.tenant)
			}
		}
		if a.tenantReceipts == nil {
			a.tenantReceipts = byTenant
			a.tenantReceiptOwners = owners
		}
		close(wait)
		a.tenantReceiptsMu.Unlock()
		return nil
	}
}

func (a *Application) visitTenantIngestReceipts(ctx context.Context, tenant string, visit func(storage.ObjectInfo, acceptedRequest) error) error {
	if err := a.ensureTenantReceipts(ctx); err != nil {
		return err
	}
	a.tenantReceiptsMu.Lock()
	keys := make([]string, 0, len(a.tenantReceipts[tenant]))
	for key := range a.tenantReceipts[tenant] {
		keys = append(keys, key)
	}
	a.tenantReceiptsMu.Unlock()
	sort.Strings(keys)
	ordinaryPrefix := a.ingestPrefix()
	for _, key := range keys {
		record, size, err := loadAcceptanceWithSize(ctx, a.Store.Objects, key)
		if errors.Is(err, storage.ErrNotFound) {
			a.untrackTenantReceipt(key)
			continue
		}
		if err != nil {
			return err
		}
		if record.Tenant == tenant && (strings.HasPrefix(key, ordinaryPrefix) || (record.AcceptedEpoch != 0 && !record.Partitioned)) {
			if err := visit(storage.ObjectInfo{Key: key, Size: size}, record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Application) tenantTransfer(ctx context.Context, tenant, move string, epoch uint64) ([]byte, error) {
	owner, err := a.ownership(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if owner.State != "frozen" || owner.Epoch != epoch || owner.MoveID != move {
		return nil, fmt.Errorf("source is not frozen for this migration")
	}
	transfer := sharding.Transfer{Tenant: tenant, MoveID: move}
	objects, err := a.Files.List(ctx, path.Join(a.Store.Prefix, "tenants", tenant)+"/")
	if err != nil {
		return nil, err
	}
	tombstone, err := a.Files.Get(ctx, a.purgeKey(tenant))
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	tombstonedTenant := false
	if len(objects) == 0 && len(tombstone) > 0 {
		complete, err := storage.IsCompleteTenantPurgeTombstone(ctx, tenant, tombstone)
		if err != nil {
			return nil, err
		}
		tombstonedTenant = complete
	}
	if !tombstonedTenant {
		if err := storage.ValidateTenantMigrationSource(ctx, a.Store, tenant, nil); err != nil {
			return nil, err
		}
	}
	var size int64
	appendObject := func(key string, data []byte) error {
		size += int64(len(data))*4/3 + int64(len(key)) + 64
		if size > a.MaxSnapshotBytes {
			return fmt.Errorf("tenant transfer exceeds the configured snapshot budget")
		}
		transfer.Objects = append(transfer.Objects, sharding.Object{Key: key, Data: data})
		return nil
	}
	for _, object := range objects {
		data, err := a.Files.Get(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		if err := appendObject(object.Key, data); err != nil {
			return nil, err
		}
	}
	generation, err := a.Store.ReplicationTenantGeneration(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if err := a.visitTenantIngestReceipts(ctx, tenant, func(object storage.ObjectInfo, record acceptedRequest) error {
		if record.AcceptedEpoch != 0 && generation != record.Generation {
			return nil
		}
		data, err := a.Files.Get(ctx, object.Key)
		if err != nil {
			return err
		}
		return appendObject(object.Key, data)
	}); err != nil {
		return nil, err
	}
	data, _ := json.Marshal(generation)
	if err := appendObject(a.generationKey(tenant), data); err != nil {
		return nil, err
	}
	if err := appendObject(a.purgeKey(tenant), tombstone); err != nil {
		return nil, err
	}
	sort.Slice(transfer.Objects, func(i, j int) bool { return transfer.Objects[i].Key < transfer.Objects[j].Key })
	encoded, err := json.Marshal(transfer)
	if int64(len(encoded)) > a.MaxSnapshotBytes {
		return nil, fmt.Errorf("tenant transfer exceeds the configured snapshot budget")
	}
	return encoded, err
}

func (a *Application) generationKey(tenant string) string {
	return a.ingestGenerationPrefix() + tenant + ".json"
}

func (a *Application) ingestGenerationPrefix() string {
	return path.Join(a.Store.Prefix, "control/tenant-generations") + "/"
}
func (a *Application) purgeKey(tenant string) string {
	return path.Join(a.Store.Prefix, "control/tenant-purges", url.PathEscape(tenant)+".parquet")
}

func (a *Application) installTenantTransfer(ctx context.Context, action sharding.Action, owner sharding.Ownership) (err error) {
	invalid := func(message string) error { return fmt.Errorf("%w: %s", errInvalidTenantTransfer, message) }
	if action.Bytes <= 0 || action.Parts <= 0 || int64(action.Parts) != (action.Bytes+sharding.ChunkBytes-1)/sharding.ChunkBytes {
		return invalid("invalid migration size or chunk count")
	}
	if owner.NextPart != action.Parts || (owner.TransferBytes != 0 && owner.TransferBytes != action.Bytes) || (owner.Digest != "" && owner.Digest != action.Digest) {
		return invalid("migration size, chunk count or snapshot identity changed")
	}
	// Legacy in-flight transfers have no per-chunk binding. Reject them before
	// cutover rather than consuming local corruption as a business rejection.
	if len(owner.ChunkDigests) != action.Parts {
		return invalid("migration lacks chunk integrity metadata; cancel and restart transfer")
	}
	for _, digest := range owner.ChunkDigests {
		if digest == "" {
			return invalid("migration lacks chunk integrity metadata; cancel and restart transfer")
		}
	}
	reader := func() *restoreReader {
		return &restoreReader{ctx: ctx, app: a, manifest: restoreManifest{Bytes: action.Bytes}, partDigests: owner.ChunkDigests, partKey: func(part int64) string { return a.transferKey(action.Tenant, action.MoveID, int(part)) }}
	}
	sourceReader := reader()
	digest := sha256.New()
	size, err := io.Copy(digest, sourceReader)
	err = errors.Join(err, sourceReader.Close())
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("persisted migration chunk is missing: %w", err)
	}
	if err != nil {
		return err
	}
	if size != action.Bytes || fmt.Sprintf("%x", digest.Sum(nil)) != action.Digest {
		return invalid("migration snapshot checksum mismatch")
	}
	view, err := a.Files.CaptureObjectView(ctx, nil, max(a.MaxSnapshotBytes, action.Bytes))
	if err != nil {
		return err
	}
	defer view.Close()
	objects := view.Store
	sourceReader = reader()
	defer func() {
		closeErr := sourceReader.Close()
		if sourceReader.readError != nil {
			// JSON validation can hide a local read/integrity failure. Preserve
			// the replica fault even when the decoder reports invalid input.
			err = fmt.Errorf("persisted migration input failed: %w", errors.Join(sourceReader.readError, closeErr))
		} else {
			err = errors.Join(err, closeErr)
		}
	}()
	decoder := json.NewDecoder(sourceReader)
	expect := func(want any) error {
		token, err := decoder.Token()
		if err != nil || token != want {
			return invalid("invalid migration JSON structure")
		}
		return nil
	}
	if err := expect(json.Delim('{')); err != nil {
		return err
	}
	if err := expect("tenant_id"); err != nil {
		return err
	}
	var tenant, move string
	if err := decoder.Decode(&tenant); err != nil || tenant != action.Tenant {
		return invalid("migration snapshot tenant mismatch")
	}
	if err := expect("move_id"); err != nil {
		return err
	}
	if err := decoder.Decode(&move); err != nil || move != action.MoveID {
		return invalid("migration snapshot identity mismatch")
	}
	if err := expect("objects"); err != nil {
		return err
	}
	if err := expect(json.Delim('[')); err != nil {
		return err
	}
	tenantPrefix := path.Join(a.Store.Prefix, "tenants", action.Tenant) + "/"
	seen := make(map[string]bool)
	keys := []string{}
	managedGenerations := make(map[string]int64)
	managedAliases := make(map[string]bool)
	var generation int64
	for decoder.More() {
		var object sharding.Object
		if err := decoder.Decode(&object); err != nil {
			return invalid("invalid migration object")
		}
		if path.Clean(object.Key) != object.Key || seen[object.Key] {
			return invalid("invalid or repeated migration object")
		}
		seen[object.Key] = true
		keys = append(keys, object.Key)
		if object.Key == a.generationKey(action.Tenant) {
			if json.Unmarshal(object.Data, &generation) != nil || generation < 1 {
				return invalid("invalid tenant incarnation")
			}
		}
		switch {
		case strings.HasPrefix(object.Key, tenantPrefix), object.Key == a.generationKey(action.Tenant), object.Key == a.purgeKey(action.Tenant):
		case strings.HasPrefix(object.Key, a.ingestPrefix()):
			var record acceptedRequest
			if json.Unmarshal(object.Data, &record) != nil || record.Tenant != action.Tenant || record.State == "accepted" || a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation) != object.Key {
				return invalid("migration contains an invalid accepted request")
			}
		case validManagedReceiptKey(a.managedReceiptPrefix(), object.Key):
			if !action.ManagedReceiptTransfer {
				return invalid("managed WAL receipt migration requires destination Raft protocol 13")
			}
			var record acceptedRequest
			if json.Unmarshal(object.Data, &record) != nil || record.Tenant != action.Tenant || record.Partitioned ||
				record.AcceptedEpoch == 0 || record.AcceptedEpoch >= action.Epoch || record.Index == 0 || record.receiptExpiresAt().IsZero() ||
				storage.ValidateIngestStorageIdentifiers(record.Request) != nil ||
				a.managedReceiptKey(a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation)) != object.Key {
				return invalid("migration contains an invalid managed WAL receipt")
			}
			if record.AliasDigest != "" {
				digest, err := hex.DecodeString(record.AliasDigest)
				key := a.ingestAliasKey(record.Tenant, record.Generation, record.Request)
				if err != nil || len(digest) != sha256.Size || record.Request.IdempotencyKey == "" || managedAliases[key] {
					return invalid("migration contains an invalid managed WAL alias")
				}
				managedAliases[key] = true
			}
			managedGenerations[object.Key] = record.Generation
		default:
			return invalid("migration contains another tenant or control namespace")
		}
		if err := objects.Put(ctx, object.Key, object.Data); err != nil {
			return err
		}
	}
	if err := expect(json.Delim(']')); err != nil {
		return err
	}
	if err := expect(json.Delim('}')); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return invalid("migration JSON has trailing data")
	}
	if !seen[a.generationKey(action.Tenant)] || !seen[a.purgeKey(action.Tenant)] {
		return invalid("migration is missing tenant incarnation metadata")
	}
	hasTenantObjects := false
	for _, key := range keys {
		if strings.HasPrefix(key, tenantPrefix) {
			hasTenantObjects = true
			break
		}
	}
	purgeData, err := objects.Get(ctx, a.purgeKey(action.Tenant))
	if err != nil {
		return err
	}
	tombstonedTenant := false
	if action.ManagedReceiptTransfer && !hasTenantObjects && len(purgeData) > 0 {
		complete, err := storage.IsCompleteTenantPurgeTombstone(ctx, action.Tenant, purgeData)
		if err != nil || !complete {
			return invalid("migration contains an invalid or incomplete tenant purge tombstone")
		}
		tombstonedTenant = true
	}
	if !tombstonedTenant && !seen[tenantPrefix+"manifest.parquet"] {
		return invalid("migration is missing tenant incarnation metadata")
	}
	for _, receiptGeneration := range managedGenerations {
		if generation != receiptGeneration {
			return invalid("managed WAL receipt generation does not match the migrating tenant")
		}
	}
	source := storage.NewTenantStore(objects, a.Store.Prefix)
	if !tombstonedTenant {
		if err := storage.ValidateTenantMigrationSource(ctx, source, action.Tenant, seen); err != nil {
			return fmt.Errorf("%w: tenant graph validation failed: %v", errInvalidTenantTransfer, err)
		}
	}
	// Restore the source incarnation before acquiring the destination writer
	// fence: a previously retired shard still has a local purge tombstone.
	// These controls and the graph replacement share the replication journal.
	installedReceipt := false
	for _, key := range keys {
		if strings.HasPrefix(key, tenantPrefix) {
			continue
		}
		data, err := objects.Get(ctx, key)
		if err != nil {
			return err
		}
		if key == a.purgeKey(action.Tenant) && len(data) == 0 {
			if err := a.Files.Delete(ctx, key); err != nil {
				return err
			}
		} else if err := a.Files.Put(ctx, key, data); err != nil {
			return err
		} else if strings.HasPrefix(key, a.ingestPrefix()) {
			installedReceipt = true
			a.trackTenantReceipt(action.Tenant, key)
		}
		if _, managed := managedGenerations[key]; managed {
			a.trackTenantReceipt(action.Tenant, key)
			var record acceptedRequest
			if err := json.Unmarshal(data, &record); err != nil {
				return err
			}
			if record.AliasDigest != "" {
				canonical := a.ingestKey(record.Tenant, record.Request.Source, record.Request.CollectorID, record.Request.BatchID, record.Generation)
				if err := a.saveIngestAlias(ctx, record, canonical); err != nil {
					return err
				}
			}
		}
	}
	if installedReceipt {
		if err := a.invalidatePendingCheckpoint(ctx); err != nil {
			return err
		}
	}
	if tombstonedTenant {
		existing, err := a.Files.List(ctx, tenantPrefix)
		if err != nil {
			return err
		}
		for _, object := range existing {
			if err := a.Files.Delete(ctx, object.Key); err != nil {
				return err
			}
		}
		a.Store.InvalidateTenantCaches(action.Tenant)
	} else if _, err := storage.CopyTenantObjects(ctx, source, action.Tenant, a.Store, action.Tenant, storage.TenantMigrationOptions{Overwrite: true}); err != nil {
		return err
	}
	return a.deleteTransferChunks(ctx, action.Tenant)
}
