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
	previousBytes := int64(0)
	if info, err := os.Stat(filepath.Join(s.wal.config.Dir, ingestWALCheckpointFile)); err == nil {
		previousBytes = info.Size()
	} else if !os.IsNotExist(err) {
		return err
	}
	size, err := writeIngestWALCheckpoint(s.wal.config.Dir, checkpoint, s.wal.config.MaxBytes-s.wal.config.controlReserveBytes())
	if err != nil {
		return err
	}
	s.totalBytes += size - previousBytes
	s.checkpointRequired = true
	return s.prune(checkpoint.Through + 1)
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
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
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
		return 0, err
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
