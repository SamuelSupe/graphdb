package ha

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"golang.org/x/sync/errgroup"
)

func (c *Cluster) replicateMaintenance(ctx context.Context, task storage.Task, input io.ReadSeeker, generation int64) error {
	digest := sha256.New()
	size, err := io.Copy(digest, input)
	if err != nil {
		return err
	}
	if size <= 0 || size > c.App.MaxSnapshotBytes {
		return fmt.Errorf("prepared maintenance exceeds snapshot budget")
	}
	if task.Type != storage.TaskTypeGC {
		if err := c.App.Store.CheckWriteDiskSpace(ctx, size*2); err != nil {
			return err
		}
	}
	manifest := restoreManifest{Bytes: size, SHA256: hex.EncodeToString(digest.Sum(nil)), Generation: generation, Maintenance: true}
	prefix := c.App.restorePrefix(task.TenantID, task.ID)
	c.App.mu.RLock()
	previous, err := c.App.Files.Get(ctx, prefix+"manifest.json")
	c.App.mu.RUnlock()
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	var existing restoreManifest
	if err == nil && (json.Unmarshal(previous, &existing) != nil || existing != manifest) {
		command, err := newCommand("maintenance_reset")
		if err != nil {
			return err
		}
		command.Tenant = task.TenantID
		command.IDs = []string{task.ID}
		command.ExpectedGeneration = generation
		if _, err := c.propose(ctx, command); err != nil {
			return err
		}
	}
	transfer, cancel := context.WithCancel(ctx)
	defer cancel()
	group, transfer := errgroup.WithContext(transfer)
	group.SetLimit(4)
	var inputErr error
	for offset := int64(0); offset < size; offset += restoreChunkBytes {
		if inputErr = transfer.Err(); inputErr != nil {
			break
		}
		if _, err := input.Seek(offset, io.SeekStart); err != nil {
			inputErr = err
			break
		}
		data := make([]byte, min(restoreChunkBytes, size-offset))
		if _, err := io.ReadFull(input, data); err != nil {
			inputErr = err
			break
		}
		part := offset / restoreChunkBytes
		c.App.mu.RLock()
		previous, err := c.App.Files.Get(transfer, restorePartKey(prefix, part))
		c.App.mu.RUnlock()
		if err == nil && bytes.Equal(previous, data) {
			continue
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			inputErr = err
			break
		}
		command, err := newCommand("restore_part")
		if err != nil {
			inputErr = err
			break
		}
		command.Tenant = task.TenantID
		command.IDs = []string{task.ID}
		command.ExpectedGeneration = generation
		command.Restore = data
		command.Body, err = json.Marshal(restorePart{restoreManifest: manifest, Part: part})
		if err != nil {
			inputErr = err
			break
		}
		if part == 0 {
			// Establish the manifest before later parts can arrive out of order.
			if _, err := c.propose(transfer, command); err != nil {
				inputErr = err
				break
			}
		} else {
			group.Go(func() error {
				_, err := c.propose(transfer, command)
				return err
			})
		}
	}
	if inputErr != nil {
		cancel()
	}
	if err := errors.Join(inputErr, group.Wait()); err != nil {
		return err
	}
	return c.publishRestore(ctx, task, manifest)
}
