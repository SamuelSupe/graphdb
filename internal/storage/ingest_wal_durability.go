package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const ingestWALDurableFile = ".durable"
const ingestWALTailSlotBytes = 44

type ingestWALDurableTail struct {
	startLSN   uint64
	offset     int64
	lastLSN    uint64
	checkpoint bool
	sequence   uint64
	uncertain  bool
}

func (t *ingestWALDurableTail) valid() bool {
	return t.startLSN > 0 && t.offset >= 0 && t.lastLSN >= t.startLSN-1 && (t.offset == 0) == (t.lastLSN == t.startLSN-1)
}
func loadIngestWALDurableTail(dir string) (*ingestWALDurableTail, error) {
	data, err := os.ReadFile(filepath.Join(dir, ingestWALDurableFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	bad := func() (*ingestWALDurableTail, error) {
		return nil, fmt.Errorf("%w: invalid durable WAL boundary", ErrIngestWALCorrupt)
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	if len(data) == 36 {
		if string(data[:4]) != "GIWD" || data[4] != 1 || data[5] > 1 || crc32.Checksum(data[:32], table) != binary.BigEndian.Uint32(data[32:]) {
			return bad()
		}
		tail := &ingestWALDurableTail{startLSN: binary.BigEndian.Uint64(data[8:16]), offset: int64(binary.BigEndian.Uint64(data[16:24])), lastLSN: binary.BigEndian.Uint64(data[24:32]), checkpoint: data[5] == 1}
		if !tail.valid() {
			return bad()
		}
		return tail, nil
	}
	if len(data) != 2*ingestWALTailSlotBytes {
		return bad()
	}
	var valid []*ingestWALDurableTail
	for slot := range 2 {
		b := data[slot*ingestWALTailSlotBytes : (slot+1)*ingestWALTailSlotBytes]
		if string(b[:4]) != "GIWD" || b[4] != 2 || b[5] > 1 || b[6] != 0 || b[7] != 0 || crc32.Checksum(b[:40], table) != binary.BigEndian.Uint32(b[40:]) {
			continue
		}
		t := &ingestWALDurableTail{sequence: binary.BigEndian.Uint64(b[8:16]), startLSN: binary.BigEndian.Uint64(b[16:24]), offset: int64(binary.BigEndian.Uint64(b[24:32])), lastLSN: binary.BigEndian.Uint64(b[32:40]), checkpoint: b[5] == 1}
		if t.valid() && t.sequence%2 == uint64(slot) {
			valid = append(valid, t)
		}
	}
	if len(valid) == 0 {
		return bad()
	}
	latest := valid[0]
	if len(valid) == 2 {
		old := valid[1]
		if old.sequence > latest.sequence {
			old, latest = latest, old
		}
		if latest.sequence-old.sequence != 1 || latest.lastLSN < old.lastLSN || latest.startLSN < old.startLSN {
			return bad()
		}
	} else {
		// The damaged slot may contain a newer ACK. Replay valid frames, but never
		// use this older proof to truncate a corrupt or incomplete suffix.
		latest.uncertain = true
	}
	return latest, nil
}
func encodeIngestWALTail(t ingestWALDurableTail) []byte {
	b := make([]byte, ingestWALTailSlotBytes)
	copy(b, "GIWD")
	b[4] = 2
	if t.checkpoint {
		b[5] = 1
	}
	binary.BigEndian.PutUint64(b[8:16], t.sequence)
	binary.BigEndian.PutUint64(b[16:24], t.startLSN)
	binary.BigEndian.PutUint64(b[24:32], uint64(t.offset))
	binary.BigEndian.PutUint64(b[32:40], t.lastLSN)
	binary.BigEndian.PutUint32(b[40:], crc32.Checksum(b[:40], crc32.MakeTable(crc32.Castagnoli)))
	return b
}
func (s *ingestWALWriterState) persistDurableTail(lsn uint64) error {
	segment := s.segments[s.current]
	tail := ingestWALDurableTail{startLSN: segment.startLSN, offset: segment.size, lastLSN: lsn, checkpoint: s.checkpointRequired}
	name := filepath.Join(s.wal.config.Dir, ingestWALDurableFile)
	if s.tailFile == nil {
		info, err := os.Lstat(name)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("invalid durable WAL boundary file")
		}
		if os.IsNotExist(err) || info.Size() == 36 {
			data := encodeIngestWALTail(tail)
			tail.sequence = 1
			data = append(data, encodeIngestWALTail(tail)...)
			if err := writeFileAtomic(name, data); err != nil {
				return err
			}
			s.tailSequence = 1
		} else {
			previous, err := loadIngestWALDurableTail(s.wal.config.Dir)
			if err != nil {
				return err
			}
			s.tailSequence = previous.sequence
		}
		s.tailFile, err = os.OpenFile(name, os.O_RDWR, 0600)
		if err != nil {
			return err
		}
	}
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	opened, err := s.tailFile.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || !os.SameFile(info, opened) {
		return fmt.Errorf("durable WAL boundary file was replaced")
	}
	if s.tailSequence == math.MaxUint64 {
		return fmt.Errorf("durable WAL boundary sequence exhausted")
	}
	tail.sequence = s.tailSequence + 1
	data := encodeIngestWALTail(tail)
	n, err := s.tailFile.WriteAt(data, int64(tail.sequence%2)*ingestWALTailSlotBytes)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short durable WAL boundary write")
	}
	if err := syncStorageFile(s.tailFile); err != nil {
		return fmt.Errorf("persist durable WAL boundary: %w", err)
	}
	s.tailSequence = tail.sequence
	return nil
}
func (s *ingestWALWriterState) initializeDurableTail() error {
	segment := s.segments[s.current]
	if segment.size > 0 {
		file, err := os.OpenFile(segment.path, os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		if err = errors.Join(file.Sync(), file.Close()); err != nil {
			return err
		}
	}
	return s.persistDurableTail(s.writtenLSN)
}

// Only the locked WAL owner can reclaim these abandoned atomic-write files.
func cleanupIngestWALTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	changed := false
	for _, entry := range entries {
		if entry.Type().IsRegular() && (strings.HasPrefix(entry.Name(), ".tmp-.durable-") || strings.HasPrefix(entry.Name(), ".tmp-.checkpoint-")) {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
				return err
			}
			changed = true
		}
	}
	if changed {
		return syncDir(dir)
	}
	return nil
}
