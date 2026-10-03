package ha

import (
	"context"
	"crypto/sha256"
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

func (a *Application) tenantTransfer(ctx context.Context, tenant, move string, epoch uint64) ([]byte, error) {
	owner, err := a.ownership(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if owner.State != "frozen" || owner.Epoch != epoch || owner.MoveID != move {
		return nil, fmt.Errorf("source is not frozen for this migration")
	}
	if err := storage.ValidateTenantMigrationSource(ctx, a.Store, tenant, nil); err != nil {
		return nil, err
	}
	transfer := sharding.Transfer{Tenant: tenant, MoveID: move}
	objects, err := a.Files.List(ctx, path.Join(a.Store.Prefix, "tenants", tenant)+"/")
	if err != nil {
		return nil, err
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
	accepted, err := a.Files.List(ctx, a.ingestPrefix())
	if err != nil {
		return nil, err
	}
	for _, object := range accepted {
		record, err := a.accepted(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		if record.Tenant == tenant {
			data, err := a.Files.Get(ctx, object.Key)
			if err != nil {
				return nil, err
			}
			if err := appendObject(object.Key, data); err != nil {
				return nil, err
			}
		}
	}
	generation, err := a.Store.ReplicationTenantGeneration(ctx, tenant)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(generation)
	if err := appendObject(a.generationKey(tenant), data); err != nil {
		return nil, err
	}
	tombstone, err := a.Files.Get(ctx, a.purgeKey(tenant))
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
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
	return path.Join(a.Store.Prefix, "control/tenant-generations", tenant+".json")
}
func (a *Application) purgeKey(tenant string) string {
	return path.Join(a.Store.Prefix, "control/tenant-purges", url.PathEscape(tenant)+".parquet")
}

func (a *Application) installTenantTransfer(ctx context.Context, action sharding.Action) error {
	invalid := func(message string) error { return fmt.Errorf("%w: %s", errInvalidTenantTransfer, message) }
	if action.Bytes <= 0 || action.Bytes > a.MaxSnapshotBytes || action.Parts <= 0 || int64(action.Parts) != (action.Bytes+sharding.ChunkBytes-1)/sharding.ChunkBytes {
		return invalid("invalid migration size or chunk count")
	}
	reader := func() *restoreReader {
		return &restoreReader{ctx: ctx, app: a, manifest: restoreManifest{Bytes: action.Bytes}, partKey: func(part int64) string { return a.transferKey(action.Tenant, action.MoveID, int(part)) }}
	}
	sourceReader := reader()
	digest := sha256.New()
	size, err := io.Copy(digest, sourceReader)
	err = errors.Join(err, sourceReader.Close())
	if errors.Is(err, storage.ErrNotFound) {
		return invalid("migration chunk is missing; retry transfer")
	}
	if err != nil {
		return err
	}
	if size != action.Bytes || fmt.Sprintf("%x", digest.Sum(nil)) != action.Digest {
		return invalid("migration snapshot checksum mismatch")
	}
	view, err := a.Files.CaptureObjectView(ctx, nil, a.MaxSnapshotBytes)
	if err != nil {
		return err
	}
	defer view.Close()
	objects := view.Store
	sourceReader = reader()
	defer sourceReader.Close()
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
			var generation int64
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
	if !seen[a.generationKey(action.Tenant)] || !seen[a.purgeKey(action.Tenant)] || !seen[tenantPrefix+"manifest.parquet"] {
		return invalid("migration is missing tenant incarnation metadata")
	}
	source := storage.NewTenantStore(objects, a.Store.Prefix)
	if err := storage.ValidateTenantMigrationSource(ctx, source, action.Tenant, seen); err != nil {
		return fmt.Errorf("%w: tenant graph validation failed: %v", errInvalidTenantTransfer, err)
	}
	// Restore the source incarnation before acquiring the destination writer
	// fence: a previously retired shard still has a local purge tombstone.
	// These controls and the graph replacement share the replication journal.
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
		}
	}
	if _, err := storage.CopyTenantObjects(ctx, source, action.Tenant, a.Store, action.Tenant, storage.TenantMigrationOptions{Overwrite: true}); err != nil {
		return err
	}
	return a.deleteTransferChunks(ctx, action.Tenant)
}
