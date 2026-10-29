package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
)

func (s *TenantStore) tenantMetadataBackupKey(tenant string) string {
	return s.tenantObjectPrefix(tenant) + "control/tenant-metadata-backup.parquet"
}

func (s *TenantStore) tenantMetadataPendingKey(tenant string) string {
	return s.tenantObjectPrefix(tenant) + "control/tenant-metadata-pending.parquet"
}

// The separate key cannot collide with an object path. It serializes metadata
// copies with each other and with local directory publication, not graph IO.
func (s *TenantStore) lockLocalTenantMetadata(ctx context.Context, tenant string) (func(), error) {
	if files, ok := unwrapTenantMigrationStore(s.Objects).(*FileStore); ok {
		return files.lockObjectContext(ctx, "\x00metadata:"+s.tenantMetadataKey(tenant))
	}
	return func() {}, ctx.Err()
}

func (s *TenantStore) recoverableMetadataFiles(ctx context.Context) *FileStore {
	files, _ := unwrapTenantMigrationStore(s.Objects).(*FileStore)
	if files == nil || files.replicatedWrites.Load() || s.ReplicationMode || IsReplicatedContext(ctx) {
		return nil
	}
	return files
}

type tenantMetadataRecord struct {
	data     []byte
	meta     ObjectMeta
	metadata TenantMetadata
	valid    bool
	err      error
}

func (s *TenantStore) readTenantMetadataRecord(ctx context.Context, key string) (tenantMetadataRecord, error) {
	s.clearWriterObjectKey(key)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err == nil || errors.Is(err, ErrNotFound) {
		// Disk damage bypasses FileStore mutation notifications. Reconcile the
		// CAS cache with this fresh read so repair cannot loop on a stale ETag.
		if files := s.recoverableMetadataFiles(ctx); files != nil {
			files.invalidateStaleObjectMeta(key, meta)
		}
	}
	record := tenantMetadataRecord{data: data, meta: meta}
	if errors.Is(err, ErrNotFound) {
		return record, nil
	}
	return record, err
}

func (record *tenantMetadataRecord) decode(ctx context.Context, tenant string) {
	if !record.meta.Exists {
		return
	}
	record.metadata, record.err = decodeParquetTenantMetadata(ctx, record.data)
	if record.err == nil && record.metadata.TenantID != tenant {
		record.err = fmt.Errorf("tenant metadata mismatch: path tenant %q contains tenant %q", tenant, record.metadata.TenantID)
	}
	if record.err == nil {
		record.metadata.Status = normalizeTenantStatus(record.metadata.Status)
		record.valid = true
	}
}

// Pending is a durable write intent. Without it, the primary is authoritative;
// the backup repairs missing/corrupt primaries, including disabled/deleted ones.
// Only when no valid copy remains do legacy tenants use the active default.
func (s *TenantStore) localTenantMetadataCandidate(ctx context.Context, tenant string) (tenantMetadataRecord, tenantMetadataRecord, string, bool, error) {
	primary, err := s.readTenantMetadataRecord(ctx, s.tenantMetadataKey(tenant))
	if err != nil {
		return primary, primary, "", false, err
	}
	primary.decode(ctx, tenant)
	pending, err := s.readTenantMetadataRecord(ctx, s.tenantMetadataPendingKey(tenant))
	if err != nil {
		return primary, primary, "", false, err
	}
	pending.decode(ctx, tenant)
	if pending.valid {
		return primary, pending, "pending", true, nil
	}
	backup, err := s.readTenantMetadataRecord(ctx, s.tenantMetadataBackupKey(tenant))
	if err != nil {
		return primary, primary, "", false, err
	}
	if primary.valid {
		// An identical copy inherits the primary's validation. A differing
		// backup must be replaced regardless of whether it decodes successfully.
		return primary, primary, "primary", pending.meta.Exists || !backup.meta.Exists || !bytes.Equal(primary.data, backup.data), nil
	}
	backup.decode(ctx, tenant)
	if backup.valid {
		return primary, backup, "backup", true, nil
	}
	manifest, meta, err := s.readManifest(ctx, tenant, true)
	if err != nil {
		return primary, primary, "", false, err
	}
	if !meta.Exists {
		return primary, primary, "", false, primary.err
	}
	now := mutationTime(ctx)
	metadata := TenantMetadata{TenantID: manifest.TenantID, Status: TenantStatusActive, CreatedAt: now, UpdatedAt: now}
	return primary, tenantMetadataRecord{metadata: metadata, valid: true}, "manifest", true, nil
}

func (s *TenantStore) getLocalTenantMetadata(ctx context.Context, tenant string) (TenantMetadata, bool, ObjectMeta, error) {
	unlock, err := s.lockLocalTenantMetadata(ctx, tenant)
	if err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	defer unlock()
	primary, candidate, source, repair, err := s.localTenantMetadataCandidate(ctx, tenant)
	if err != nil {
		return TenantMetadata{}, false, primary.meta, err
	}
	if repair {
		if source == "pending" || source == "backup" {
			_, manifestMeta, err := s.readManifest(ctx, tenant, true)
			if err != nil {
				return TenantMetadata{}, false, primary.meta, err
			}
			if !manifestMeta.Exists {
				return primary.metadata, primary.valid, primary.meta, primary.err
			}
		}
		if purged, err := s.tenantPurgeTombstoneExists(ctx, tenant); err != nil {
			return TenantMetadata{}, false, primary.meta, err
		} else if purged {
			return primary.metadata, primary.valid, primary.meta, primary.err
		}
		s.scheduleTenantMetadataRepair(tenant)
	}
	// Keep create-on-legacy semantics until the asynchronous repair publishes.
	return candidate.metadata, candidate.valid && source != "manifest", primary.meta, nil
}

// A successful metadata write has two identical copies and no pending intent.
// If any later step fails, reads and online repair use the durable intent rather
// than reviving an older active backup after a failed disable/delete operation.
func (s *TenantStore) putRecoverableTenantMetadata(ctx context.Context, tenant string, data []byte, meta ObjectMeta) (ObjectMeta, error) {
	unlock, err := s.lockLocalTenantMetadata(ctx, tenant)
	if err != nil {
		return ObjectMeta{}, err
	}
	defer unlock()
	_, current, err := s.Objects.GetWithMeta(ctx, s.tenantMetadataKey(tenant))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return ObjectMeta{}, err
	}
	condition := PutCondition{IfNoneMatch: !meta.Exists, IfMatch: meta.ETag}
	if err := checkCondition(condition, current.ETag, current.Exists); err != nil {
		return ObjectMeta{}, err
	}
	return s.publishTenantMetadataCopies(ctx, tenant, data, current)
}

func (s *TenantStore) publishTenantMetadataCopies(ctx context.Context, tenant string, data []byte, meta ObjectMeta) (ObjectMeta, error) {
	if err := s.putTenantObject(ctx, tenant, s.tenantMetadataPendingKey(tenant), data); err != nil {
		return ObjectMeta{}, err
	}
	next, err := s.putTenantBytesWithMetaResult(ctx, tenant, s.tenantMetadataKey(tenant), data, meta)
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := s.putTenantObject(ctx, tenant, s.tenantMetadataBackupKey(tenant), data); err != nil {
		return ObjectMeta{}, err
	}
	if err := s.deleteTenantObject(ctx, tenant, s.tenantMetadataPendingKey(tenant)); err != nil {
		return ObjectMeta{}, err
	}
	return next, nil
}

// One bounded worker avoids spawning goroutines per request. Full queues are
// retried on later access; no tenant-wide scan or maintenance task is needed.
func (s *TenantStore) scheduleTenantMetadataRepair(tenant string) {
	s.metadataRepairMu.Lock()
	defer s.metadataRepairMu.Unlock()
	if s.metadataRepairs == nil {
		s.metadataRepairs = make(map[string]bool)
		s.metadataRepairQueue = make(chan string, 64)
		if !s.StartBackground(context.Background(), func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				case tenant := <-s.metadataRepairQueue:
					attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
					source, err := s.repairLocalTenantMetadata(attempt, tenant)
					cancel()
					if s.MetadataRecoveryLogger != nil {
						fields := map[string]any{"tenant_id": tenant, "source": source}
						if err != nil {
							fields["error"] = err.Error()
							s.MetadataRecoveryLogger.Error("tenant_metadata_recovery_failed", fields)
						} else if source != "" {
							s.MetadataRecoveryLogger.Info("tenant_metadata_recovered", fields)
						}
					}
					s.metadataRepairMu.Lock()
					delete(s.metadataRepairs, tenant)
					if len(s.metadataRepairs) == 0 {
						s.metadataRepairs = nil
						s.metadataRepairQueue = nil
						s.metadataRepairMu.Unlock()
						return
					}
					s.metadataRepairMu.Unlock()
				}
			}
		}) {
			s.metadataRepairs = nil
			s.metadataRepairQueue = nil
			return
		}
	}
	if !s.metadataRepairs[tenant] {
		select {
		case s.metadataRepairQueue <- tenant:
			s.metadataRepairs[tenant] = true
		default:
		}
	}
}

func (s *TenantStore) repairLocalTenantMetadata(ctx context.Context, tenant string) (string, error) {
	unlock, err := s.lockTenantMaintenance(ctx, tenant)
	if err != nil {
		return "", err
	}
	defer unlock()
	unlockMetadata, err := s.lockLocalTenantMetadata(ctx, tenant)
	if err != nil {
		return "", err
	}
	defer unlockMetadata()
	if purged, err := s.tenantPurgeTombstoneExists(ctx, tenant); err != nil || purged {
		return "", err
	}
	primary, candidate, source, repair, err := s.localTenantMetadataCandidate(ctx, tenant)
	if err != nil || !repair {
		return "", err
	}
	_, manifestMeta, err := s.readManifest(ctx, tenant, true)
	if err != nil || !manifestMeta.Exists {
		return "", err
	}
	if err := s.checkDiskSpace(ctx, 1<<20, false); err != nil {
		return source, err
	}
	ctx, err = s.acquireAndBindWriterFence(ctx, tenant)
	if err != nil {
		return source, err
	}
	data := candidate.data
	if data == nil {
		data, err = marshalParquetTenantMetadata(ctx, candidate.metadata)
		if err != nil {
			return source, err
		}
	}
	s.deleteRegisteredTenantCached(tenant)
	if err := s.addTenantToRegistry(ctx, tenant); err != nil {
		return source, err
	}
	next := primary.meta
	if source == "primary" {
		// The authoritative record is unchanged and no valid pending intent
		// exists. Repair its secondary copy without another primary replacement
		// or allocating an intent; a crash can retry from the same primary.
		err = s.putTenantObject(ctx, tenant, s.tenantMetadataBackupKey(tenant), data)
		if err == nil {
			err = s.deleteTenantObject(ctx, tenant, s.tenantMetadataPendingKey(tenant))
		}
	} else {
		next, err = s.publishTenantMetadataCopies(ctx, tenant, data, primary.meta)
	}
	if err == nil {
		s.setCachedTenantMetadata(tenant, candidate.metadata, true, next)
	}
	return source, err
}
