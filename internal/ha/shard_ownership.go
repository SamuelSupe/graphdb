package ha

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (a *Application) ownershipKey(tenant string) string {
	return path.Join(a.Store.Prefix, "control/sharding/tenants", tenant+".json")
}

func (a *Application) ownership(ctx context.Context, tenant string) (sharding.Ownership, error) {
	var owner sharding.Ownership
	data, err := a.Files.Get(ctx, a.ownershipKey(tenant))
	if errors.Is(err, storage.ErrNotFound) {
		return owner, nil
	}
	if err == nil {
		err = json.Unmarshal(data, &owner)
	}
	return owner, err
}

func (a *Application) checkOwnership(ctx context.Context, tenant string, epoch uint64) (bool, error) {
	owner, err := a.ownership(ctx, tenant)
	return err == nil && epoch > 0 && owner.Epoch == epoch && owner.State == "active", err
}

func (a *Application) applyOwnership(ctx context.Context, action sharding.Action) ([]byte, error) {
	conflict := func(message string) ([]byte, error) {
		return resultJSON(http.StatusConflict, map[string]any{"code": "shard_epoch_changed", "error": message, "retryable": true})
	}
	if err := storage.ValidateTenantID(action.Tenant); err != nil || action.Epoch == 0 {
		return conflict("a valid tenant and positive routing epoch are required")
	}
	if action.Operation != "own" && sharding.ValidateIdentifier(action.MoveID) != nil {
		return conflict("a valid migration identity is required")
	}
	owner, err := a.ownership(ctx, action.Tenant)
	if err != nil {
		return nil, err
	}
	match := owner.Epoch == action.Epoch && owner.MoveID == action.MoveID
	switch action.Operation {
	case "own":
		if owner.Epoch != 0 && (owner.Epoch != action.Epoch || owner.State != "active") {
			return conflict("tenant already has a different shard ownership")
		}
		owner = sharding.Ownership{Epoch: action.Epoch, State: "active"}
	case "reserve":
		if match && (owner.State == "importing" || owner.State == "installed" || owner.State == "active") {
			return resultJSON(http.StatusOK, owner)
		}
		if owner.Epoch >= action.Epoch || (owner.Epoch > 0 && owner.State != "retired") {
			return conflict("destination already owns this tenant")
		}
		if owner.Epoch == 0 {
			if _, err := a.Store.GetTenantInfo(ctx, action.Tenant); err == nil {
				return conflict("destination contains unmanaged tenant data")
			} else if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
		}
		owner = sharding.Ownership{Epoch: action.Epoch, State: "importing", MoveID: action.MoveID}
	case "freeze":
		if match && owner.State == "frozen" {
			return resultJSON(http.StatusOK, owner)
		}
		if owner.Epoch != action.Epoch || owner.State != "active" {
			return conflict("source ownership changed before freeze")
		}
		if ready, err := a.tenantMigrationReady(ctx, action.Tenant); err != nil {
			return nil, err
		} else if !ready {
			return conflict("source is draining accepted ingestion or queued tasks")
		}
		owner.State, owner.MoveID = "frozen", action.MoveID
	case "activate":
		if !match || (owner.State != "installed" && owner.State != "active") {
			return conflict("destination has not installed this migration")
		}
		owner.State = "active"
	case "thaw":
		if match && owner.State == "active" {
			return resultJSON(http.StatusOK, owner)
		}
		if owner.Epoch+1 != action.Epoch || (owner.State != "active" && !(owner.State == "frozen" && owner.MoveID == action.MoveID)) {
			return conflict("source cannot cancel this migration")
		}
		owner = sharding.Ownership{Epoch: action.Epoch, State: "active", MoveID: action.MoveID}
	case "retire":
		if match && owner.State == "retired" {
			return resultJSON(http.StatusOK, owner)
		}
		if !match || owner.State != "frozen" {
			return conflict("source ownership changed before cleanup")
		}
		if err := a.removeMigratedTenant(ctx, action.Tenant); err != nil {
			return nil, err
		}
		owner.State = "retired"
	case "discard":
		if owner.Epoch > action.Epoch || (owner.Epoch > 0 && !match) {
			return resultJSON(http.StatusOK, owner)
		}
		if owner.State == "active" && match {
			return conflict("an active destination cannot be discarded")
		}
		if match && (owner.State == "installed" || owner.State == "importing") {
			if err := a.removeMigratedTenant(ctx, action.Tenant); err != nil {
				return nil, err
			}
		}
		owner = sharding.Ownership{Epoch: action.Epoch, State: "retired", MoveID: action.MoveID}
	case "stage":
		if !match || (owner.State != "importing" && owner.State != "installed") || action.Part < 0 || int64(action.Part)*sharding.ChunkBytes >= a.MaxSnapshotBytes || len(action.Data) > sharding.ChunkBytes {
			return conflict("invalid migration chunk or destination ownership")
		}
		// A lost install response can restart the copy phase. Installed data
		// is already checksummed; replayed chunks must not recreate staging files.
		if owner.State == "installed" {
			return resultJSON(http.StatusOK, owner)
		}
		if action.Bytes <= 0 || action.Bytes > a.MaxSnapshotBytes || int64(action.Part)*sharding.ChunkBytes >= action.Bytes || int64(len(action.Data)) != min(sharding.ChunkBytes, action.Bytes-int64(action.Part)*sharding.ChunkBytes) || (owner.TransferBytes != 0 && owner.TransferBytes != action.Bytes) {
			return conflict("migration chunk size or transfer size changed")
		}
		if action.Part > owner.NextPart || (owner.Digest != "" && action.Digest != "" && owner.Digest != action.Digest) {
			return conflict("migration chunk sequence or snapshot digest changed")
		}
		if action.Digest != "" {
			owner.Digest = action.Digest
		}
		key := a.transferKey(action.Tenant, action.MoveID, action.Part)
		previous, err := a.Files.Get(ctx, key)
		if err == nil {
			if action.Part < len(owner.ChunkDigests) && owner.ChunkDigests[action.Part] != "" && fmt.Sprintf("%x", sha256.Sum256(previous)) != owner.ChunkDigests[action.Part] {
				return nil, fmt.Errorf("persisted migration chunk %d integrity failed", action.Part)
			}
			if string(previous) != string(action.Data) {
				return conflict("migration chunk identity was reused for different data")
			}
		} else if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		} else if action.Part < owner.NextPart {
			return nil, fmt.Errorf("persisted migration chunk %d is missing: %w", action.Part, err)
		} else if err := a.Files.Put(ctx, key, action.Data); err != nil {
			return nil, err
		}
		owner.TransferBytes = action.Bytes
		for len(owner.ChunkDigests) <= action.Part {
			owner.ChunkDigests = append(owner.ChunkDigests, "")
		}
		owner.ChunkDigests[action.Part] = fmt.Sprintf("%x", sha256.Sum256(action.Data))
		if action.Part == owner.NextPart {
			owner.NextPart++
		}
	case "install":
		if match && (owner.State == "installed" || owner.State == "active") {
			if owner.Digest != action.Digest {
				return conflict("migration identity was reused for a different snapshot")
			}
			return resultJSON(http.StatusOK, owner)
		}
		if !match || owner.State != "importing" {
			return conflict("destination is not reserved for this migration")
		}
		if err := a.installTenantTransfer(ctx, action, owner); err != nil {
			if errors.Is(err, errInvalidTenantTransfer) {
				return conflict(err.Error())
			}
			return nil, err
		}
		owner.State, owner.Digest = "installed", action.Digest
		owner.ChunkDigests = nil
	default:
		return conflict("unsupported ownership operation")
	}
	data, err := json.Marshal(owner)
	if err != nil {
		return nil, err
	}
	if err := a.Files.Put(ctx, a.ownershipKey(action.Tenant), data); err != nil {
		return nil, err
	}
	return resultJSON(http.StatusOK, owner)
}

func (a *Application) tenantMigrationReady(ctx context.Context, tenant string) (bool, error) {
	objects, err := a.Files.List(ctx, a.ingestPrefix())
	if err != nil {
		return false, err
	}
	for _, object := range objects {
		record, err := a.accepted(ctx, object.Key)
		if err != nil {
			return false, err
		}
		if record.Tenant == tenant && record.State == "accepted" {
			return false, nil
		}
	}
	tasks, err := a.Store.ListTasks(ctx, tenant, storage.TaskListOptions{})
	if err != nil {
		return false, err
	}
	for _, task := range tasks {
		if task.Status == storage.TaskStatusQueued || task.Status == storage.TaskStatusRunning {
			return false, nil
		}
	}
	return true, nil
}

func (a *Application) removeMigratedTenant(ctx context.Context, tenant string) error {
	if _, err := a.Store.PurgeTenant(ctx, tenant, true); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	objects, err := a.Files.List(ctx, a.ingestPrefix())
	if err != nil {
		return err
	}
	for _, object := range objects {
		record, err := a.accepted(ctx, object.Key)
		if err != nil {
			return err
		}
		if record.Tenant == tenant {
			if err := a.Files.Delete(ctx, object.Key); err != nil {
				return err
			}
		}
	}
	return a.deleteTransferChunks(ctx, tenant)
}

func (a *Application) transferKey(tenant, move string, part int) string {
	return path.Join(a.Store.Prefix, "control/sharding/transfers", tenant, move, fmt.Sprintf("%08d.bin", part))
}

func (a *Application) deleteTransferChunks(ctx context.Context, tenant string) error {
	objects, err := a.Files.List(ctx, path.Join(a.Store.Prefix, "control/sharding/transfers", tenant)+"/")
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := a.Files.Delete(ctx, object.Key); err != nil {
			return err
		}
	}
	return nil
}
