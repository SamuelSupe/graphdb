package ha

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
)

func (c *Cluster) copyTenantMove(ctx context.Context, source, target sharding.Shard, placement sharding.Placement, owner sharding.Ownership) error {
	action := sharding.Action{Tenant: placement.Tenant, MoveID: placement.Move.ID, Epoch: placement.Move.Epoch - 1}
	var info sharding.TransferInfo
	err := c.shards.JSON(ctx, source, http.MethodPost, "/cluster/export/manifest", action, &info)
	var legacy []byte
	var responseErr *sharding.HTTPError
	if errors.As(err, &responseErr) && (responseErr.Status == http.StatusNotFound || responseErr.Status == http.StatusMethodNotAllowed) {
		// Old sources expose only the complete JSON export. The replicated
		// payload remains identical, so rolling upgrades keep that fallback.
		response, requestErr := c.shards.Do(ctx, source, http.MethodPost, "/cluster/export", action)
		if requestErr != nil {
			return requestErr
		}
		legacy, err = io.ReadAll(io.LimitReader(response.Body, c.App.MaxSnapshotBytes+1))
		response.Body.Close()
		if err != nil {
			return err
		}
		if response.StatusCode >= 400 {
			return &sharding.HTTPError{Status: response.StatusCode, Body: string(legacy)}
		}
		info = sharding.TransferInfo{Bytes: int64(len(legacy)), Parts: (len(legacy) + sharding.ChunkBytes - 1) / sharding.ChunkBytes, Digest: fmt.Sprintf("%x", sha256.Sum256(legacy))}
	}
	if err != nil {
		return err
	}
	digest, decodeErr := hex.DecodeString(info.Digest)
	if decodeErr != nil || len(digest) != sha256.Size || info.Bytes <= 0 || info.Bytes > c.App.MaxSnapshotBytes || int64(info.Parts) != (info.Bytes+sharding.ChunkBytes-1)/sharding.ChunkBytes {
		return fmt.Errorf("invalid migration export manifest or transfer budget")
	}
	if owner.NextPart > info.Parts || (owner.Digest != "" && owner.Digest != info.Digest) {
		return fmt.Errorf("migration source changed after transfer began")
	}
	end := min(info.Parts, owner.NextPart+8)
	for part := owner.NextPart; part < end; part++ {
		size := min(int64(sharding.ChunkBytes), info.Bytes-int64(part)*sharding.ChunkBytes)
		var data []byte
		if legacy != nil {
			data = legacy[int64(part)*sharding.ChunkBytes : int64(part)*sharding.ChunkBytes+size]
		} else {
			action.Part, action.Digest = part, info.Digest
			response, err := c.shards.Do(ctx, source, http.MethodPost, "/cluster/export/chunk", action)
			if err != nil {
				return err
			}
			data, err = io.ReadAll(io.LimitReader(response.Body, size+1))
			response.Body.Close()
			if err != nil {
				return err
			}
			if response.StatusCode >= 400 {
				return &sharding.HTTPError{Status: response.StatusCode, Body: string(data)}
			}
			if int64(len(data)) != size {
				return fmt.Errorf("migration source returned an invalid chunk size")
			}
		}
		stage := sharding.Action{Operation: "stage", Tenant: placement.Tenant, MoveID: placement.Move.ID, Epoch: placement.Move.Epoch, Part: part, Bytes: info.Bytes, Digest: info.Digest, Data: data}
		stageCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := c.shards.JSON(stageCtx, target, http.MethodPost, "/cluster/action", stage, nil)
		cancel()
		if err != nil {
			return err
		}
	}
	if end < info.Parts {
		return nil
	}
	install := sharding.Action{Operation: "install", Tenant: placement.Tenant, MoveID: placement.Move.ID, Epoch: placement.Move.Epoch, Parts: info.Parts, Bytes: info.Bytes, Digest: info.Digest}
	if err := c.shards.JSON(ctx, target, http.MethodPost, "/cluster/action", install, nil); err != nil {
		return err
	}
	return c.catalogStep(ctx, placement, "cutover")
}
