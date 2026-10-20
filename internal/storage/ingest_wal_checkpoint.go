package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

const ingestWALCheckpointFile = ".checkpoint"

type ingestWALCheckpoint struct {
	Version int                 `json:"version"`
	Through uint64              `json:"through_lsn"`
	Pending []walIngestEnvelope `json:"pending"`
}

func loadIngestWALCheckpoint(dir string) (ingestWALCheckpoint, int64, error) {
	var checkpoint ingestWALCheckpoint
	data, err := os.ReadFile(filepath.Join(dir, ingestWALCheckpointFile))
	if os.IsNotExist(err) {
		return checkpoint, 0, nil
	}
	if err != nil {
		return checkpoint, 0, err
	}
	if len(data) < 4 || crc32.Checksum(data[4:], crc32.MakeTable(crc32.Castagnoli)) != binary.BigEndian.Uint32(data[:4]) ||
		json.Unmarshal(data[4:], &checkpoint) != nil || checkpoint.Version != 1 {
		return checkpoint, 0, fmt.Errorf("%w: invalid ingest checkpoint", ErrIngestWALCorrupt)
	}
	ids := make(map[string]bool, len(checkpoint.Pending))
	for _, envelope := range checkpoint.Pending {
		if envelope.RecordID == "" || envelope.TenantID == "" || envelope.AcceptedLSN == 0 || envelope.AcceptedLSN > checkpoint.Through || ids[envelope.RecordID] {
			return checkpoint, 0, fmt.Errorf("%w: inconsistent ingest checkpoint", ErrIngestWALCorrupt)
		}
		ids[envelope.RecordID] = true
	}
	return checkpoint, int64(len(data)), nil
}

// Fold durable WAL history on the writer goroutine. The replacement checkpoint
// owns every outstanding request before any old segment can be removed, even
// when a tenant cannot publish. No accepted input becomes a terminal failure.
func (s *ingestWALWriterState) checkpoint() error {
	if s.writtenLSN == 0 {
		return nil
	}
	if err := syncStorageFile(s.file); err != nil {
		return errors.Join(ErrIngestWALFailed, err)
	}
	if err := s.persistDurableTail(s.writtenLSN); err != nil {
		return errors.Join(ErrIngestWALFailed, err)
	}
	s.durableLSN = s.writtenLSN
	var recovery ingestRecovery
	if _, _, _, err := scanIngestWAL(s.wal.config.Dir, recovery.apply); err != nil {
		return errors.Join(ErrIngestWALFailed, err)
	}
	checkpoint := ingestWALCheckpoint{Version: 1, Through: s.writtenLSN}
	for _, pending := range recovery.pending {
		checkpoint.Pending = append(checkpoint.Pending, pending.envelope)
	}
	sort.Slice(checkpoint.Pending, func(i, j int) bool { return checkpoint.Pending[i].AcceptedLSN < checkpoint.Pending[j].AcceptedLSN })
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	data := make([]byte, 4, len(payload)+4)
	binary.BigEndian.PutUint32(data, crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	data = append(data, payload...)
	_, previousBytes, err := loadIngestWALCheckpoint(s.wal.config.Dir)
	if err != nil {
		return errors.Join(ErrIngestWALFailed, err)
	}
	if int64(len(data)) >= s.wal.config.MaxBytes-s.wal.config.controlReserveBytes() {
		return ErrIngestWALFull
	}
	if err := writeFileAtomic(filepath.Join(s.wal.config.Dir, ingestWALCheckpointFile), data); err != nil {
		return err
	}
	s.totalBytes += int64(len(data)) - previousBytes
	s.checkpointRequired = true
	return s.prune(checkpoint.Through + 1)
}
