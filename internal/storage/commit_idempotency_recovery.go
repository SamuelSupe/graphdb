package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

func (s *TenantStore) directCommitPreparedPublished(ctx context.Context, tenantID string, record DirectCommitRecord) (published bool, decisive bool, err error) {
	// A skipped request has no graph publication to prove, including version 0.
	if record.Result.Skipped {
		return true, true, nil
	}
	targetVersion := record.Result.Version
	targetCommitID := record.Result.HeadCommitID
	if targetVersion < 1 || targetCommitID == "" {
		return false, false, fmt.Errorf("%w: prepared commit idempotency record has no commit identity", ErrConflict)
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	if manifest.Version < targetVersion {
		return false, true, nil
	}
	if manifest.Version == targetVersion {
		return manifest.HeadCommitID == targetCommitID, true, nil
	}
	if manifest.SnapshotVersion >= targetVersion {
		return false, false, nil
	}

	foundVersion := false
	for _, ref := range manifest.CommitSegments {
		if targetVersion < ref.FirstVersion || targetVersion > ref.LastVersion {
			continue
		}
		items, err := s.loadCommitSegment(ctx, tenantID, ref)
		if err != nil {
			return false, false, err
		}
		for _, item := range items {
			if item.Commit.Version != targetVersion {
				continue
			}
			foundVersion = true
			if item.Commit.ID == targetCommitID {
				return true, true, nil
			}
		}
	}
	for _, key := range manifest.CommitKeys {
		commit, err := s.getCommitObject(ctx, key)
		if err != nil {
			return false, false, err
		}
		if commit.Version != targetVersion {
			continue
		}
		foundVersion = true
		if commit.ID == targetCommitID {
			return true, true, nil
		}
	}
	if foundVersion {
		return false, true, nil
	}
	return false, false, fmt.Errorf("%w: current manifest does not identify commit version %d", ErrConflict, targetVersion)
}

// Called under the tenant lock before compaction discards commit identities.
// Scan in bounded pages; terminal records need no write and unresolved outcomes
// stop publication rather than turning an uncertain request into a success.
func (s *TenantStore) settleDirectCommitsBeforeCompaction(ctx context.Context, tenantID string, snapshotVersion int64) error {
	prefix := path.Join(s.tenantObjectPrefix(tenantID), "idempotency", "commits") + "/"
	return scanObjectPrefixFresh(ctx, s.Objects, prefix, func(objects []ObjectInfo) error {
		for _, object := range objects {
			if !strings.HasSuffix(object.Key, ".parquet") {
				continue
			}
			record, meta, err := s.loadDirectCommitRecordWithMeta(ctx, object.Key)
			if err != nil {
				return err
			}
			if directCommitRecordStatus(record) != directCommitStatusPrepared || record.Result.Version > snapshotVersion {
				continue
			}
			if err := validateDirectCommitRecord(tenantID, record.Request, record); err != nil {
				return err
			}
			if object.Key != s.commitIdempotencyKey(tenantID, record.Request.IdempotencyKey) {
				return fmt.Errorf("%w: commit idempotency path mismatch", ErrConflict)
			}
			record.TenantID = tenantID
			published, decisive, err := s.directCommitPreparedPublished(ctx, tenantID, record)
			if err != nil {
				return err
			}
			if !decisive {
				return fmt.Errorf("%w: cannot compact unresolved commit idempotency key %q", ErrConflict, record.Request.IdempotencyKey)
			}
			reservation := &directCommitReservation{key: object.Key, record: record, meta: meta}
			if published {
				if err := s.completeDirectCommit(ctx, reservation, record.Result, record.FinishedAt); err != nil {
					return err
				}
			} else {
				// The version belongs to another commit. Preserve that negative
				// outcome before its evidence is compacted away, allowing retry.
				record.Status = directCommitStatusPending
				record.Result = CommitResult{Manifest: Manifest{TenantID: tenantID}}
				record.FinishedAt = time.Time{}
				if err := s.updateDirectCommitReservation(ctx, reservation, record); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
