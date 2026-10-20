package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const ingestWALDurableFile = ".durable"

type ingestWALDurableTail struct {
	startLSN   uint64
	offset     int64
	lastLSN    uint64
	checkpoint bool
}

func loadIngestWALDurableTail(dir string) (*ingestWALDurableTail, error) {
	data, err := os.ReadFile(filepath.Join(dir, ingestWALDurableFile))
	if os.IsNotExist(err) {
		return nil, nil // Legacy WALs have no proof that a complete bad frame was unacknowledged.
	}
	if err != nil {
		return nil, err
	}
	if len(data) != 36 || string(data[:4]) != "GIWD" || data[4] != 1 ||
		crc32.Checksum(data[:32], crc32.MakeTable(crc32.Castagnoli)) != binary.BigEndian.Uint32(data[32:]) {
		return nil, fmt.Errorf("%w: invalid durable WAL boundary", ErrIngestWALCorrupt)
	}
	tail := &ingestWALDurableTail{startLSN: binary.BigEndian.Uint64(data[8:16]), offset: int64(binary.BigEndian.Uint64(data[16:24])), lastLSN: binary.BigEndian.Uint64(data[24:32]), checkpoint: data[5] == 1}
	if data[5] > 1 {
		return nil, fmt.Errorf("%w: invalid durable WAL flags", ErrIngestWALCorrupt)
	}
	if tail.startLSN == 0 || tail.offset < 0 || tail.lastLSN < tail.startLSN-1 || (tail.offset == 0) != (tail.lastLSN == tail.startLSN-1) {
		return nil, fmt.Errorf("%w: inconsistent durable WAL boundary", ErrIngestWALCorrupt)
	}
	return tail, nil
}

func (s *ingestWALWriterState) persistDurableTail(lsn uint64) error {
	segment := s.segments[s.current]
	data := make([]byte, 36)
	copy(data, "GIWD")
	data[4] = 1
	if s.checkpointRequired {
		data[5] = 1
	}
	binary.BigEndian.PutUint64(data[8:16], segment.startLSN)
	binary.BigEndian.PutUint64(data[16:24], uint64(segment.size))
	binary.BigEndian.PutUint64(data[24:32], lsn)
	binary.BigEndian.PutUint32(data[32:], crc32.Checksum(data[:32], crc32.MakeTable(crc32.Castagnoli)))
	if err := writeFileAtomic(filepath.Join(s.wal.config.Dir, ingestWALDurableFile), data); err != nil {
		return fmt.Errorf("persist durable WAL boundary: %w", err)
	}
	return nil
}

func (s *ingestWALWriterState) initializeDurableTail() error {
	segment := s.segments[s.current]
	if segment.size > 0 {
		file, err := os.OpenFile(segment.path, os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		err = errors.Join(file.Sync(), file.Close())
		if err != nil {
			return err
		}
	}
	return s.persistDurableTail(s.writtenLSN)
}
