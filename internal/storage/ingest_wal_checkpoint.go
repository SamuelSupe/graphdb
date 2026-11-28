package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
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
	if len(data) < 4 || crc32.Checksum(data[4:], ingestWALCRCTable) != binary.BigEndian.Uint32(data[:4]) ||
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
	if s.wal.config.Durability == IngestWALDurabilitySync {
		if err := s.persistDurableTail(s.writtenLSN); err != nil {
			return errors.Join(ErrIngestWALFailed, err)
		}
	}
	s.durableLSN = s.writtenLSN
	var recovery ingestRecovery
	var recoveryErr error
	if s.live == nil {
		_, _, _, recoveryErr = scanIngestWALWithDurability(s.wal.config.Dir, s.wal.config.Durability, recovery.apply)
	} else {
		recovery, recoveryErr = s.recoverLive()
	}
	if recoveryErr != nil {
		return errors.Join(ErrIngestWALFailed, recoveryErr)
	}
	if s.headProgressID != "" && recovery.pending[s.headProgressID] == nil {
		s.headProgressID = ""
	}
	checkpoint := ingestWALCheckpoint{Version: 1, Through: s.writtenLSN, Pending: make([]walIngestEnvelope, 0, len(recovery.pending))}
	for _, pending := range recovery.pending {
		envelope := pending.envelope
		if envelope.Prepared != nil {
			// The same outcome is already recorded in the durable plan.
			envelope.Result = nil
		}
		checkpoint.Pending = append(checkpoint.Pending, envelope)
	}
	sort.Slice(checkpoint.Pending, func(i, j int) bool { return checkpoint.Pending[i].AcceptedLSN < checkpoint.Pending[j].AcceptedLSN })
	previousBytes := int64(0)
	if info, err := os.Stat(filepath.Join(s.wal.config.Dir, ingestWALCheckpointFile)); err == nil {
		previousBytes = info.Size()
	} else if !os.IsNotExist(err) {
		return err
	}
	size, err := writeIngestWALCheckpoint(s.wal.config.Dir, checkpoint, s.wal.config.MaxBytes-s.wal.config.terminalReserveBytes())
	if err != nil {
		return err
	}
	s.totalBytes += size - previousBytes
	for id := range s.live {
		s.live[id] = nil
	}
	s.checkpointRequired = true
	s.wal.hasCheckpoint.Store(true)
	if s.wal.config.Durability == IngestWALDurabilitySync {
		if err := s.persistDurableTail(s.writtenLSN); err != nil {
			return errors.Join(ErrIngestWALFailed, err)
		}
	}
	if err := s.prune(checkpoint.Through + 1); err != nil || len(checkpoint.Pending) != 0 {
		return err
	}
	// The empty checkpoint covered the deleted segments. Make the new tail
	// independent of it before removing it, including across a crash.
	s.checkpointRequired = false
	if s.wal.config.Durability == IngestWALDurabilitySync {
		if err := s.persistDurableTail(s.writtenLSN); err != nil {
			return errors.Join(ErrIngestWALFailed, err)
		}
	}
	if err := os.Remove(filepath.Join(s.wal.config.Dir, ingestWALCheckpointFile)); err != nil {
		return err
	}
	s.totalBytes -= size
	s.wal.hasCheckpoint.Store(false)
	s.observeState(0)
	if err := syncDir(s.wal.config.Dir); err != nil {
		return errors.Join(ErrIngestWALFailed, err)
	}
	return nil
}

// Preserve the v1 CRC+JSON checkpoint format without a second pending-sized
// encoded buffer. Each encoder allocation is bounded by one accepted request.
func writeIngestWALCheckpoint(dir string, checkpoint ingestWALCheckpoint, limit int64) (size int64, err error) {
	file, err := os.CreateTemp(dir, ".tmp-.checkpoint-")
	if err != nil {
		return 0, err
	}
	name := file.Name()
	defer func() { file.Close(); os.Remove(name) }()
	if err = file.Chmod(0600); err != nil {
		return 0, err
	}
	if _, err = file.Write(make([]byte, 4)); err != nil {
		return 0, err
	}
	crc := crc32.New(ingestWALCRCTable)
	writer := &ingestCheckpointWriter{writer: io.MultiWriter(file, crc), remaining: limit - 5}
	if _, err = fmt.Fprintf(writer, `{"version":1,"through_lsn":%d,"pending":[`, checkpoint.Through); err != nil {
		return 0, err
	}
	encoder := json.NewEncoder(writer)
	for i := range checkpoint.Pending {
		if i > 0 {
			if _, err = io.WriteString(writer, ","); err != nil {
				return 0, err
			}
		}
		if err = encoder.Encode(checkpoint.Pending[i]); err != nil {
			return 0, err
		}
	}
	if _, err = io.WriteString(writer, "]}"); err != nil {
		return 0, err
	}
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc.Sum32())
	if _, err = file.WriteAt(checksum[:], 0); err != nil {
		return 0, err
	}
	if err = syncStorageFile(file); err != nil {
		return 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	size = info.Size()
	if err = file.Close(); err != nil {
		return 0, err
	}
	if err = os.Rename(name, filepath.Join(dir, ingestWALCheckpointFile)); err != nil {
		return 0, err
	}
	if err = syncDir(dir); err != nil {
		return 0, errors.Join(ErrIngestWALFailed, err)
	}
	return size, nil
}

type ingestCheckpointWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *ingestCheckpointWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, ErrIngestWALFull
	}
	n, err := w.writer.Write(data)
	w.remaining -= int64(n)
	return n, err
}
