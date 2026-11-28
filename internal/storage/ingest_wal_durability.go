package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/wal"
)

const ingestWALDurableFile = wal.DurableFileName
const ingestWALTailSlotBytes = wal.TailSlotBytes

var ingestWALCRCTable = wal.CRCTable

type ingestWALDurableTail struct {
	startLSN   uint64
	offset     int64
	lastLSN    uint64
	checkpoint bool
	sequence   uint64
	uncertain  bool
}

func (t *ingestWALDurableTail) valid() bool {
	wt := wal.DurableTail{
		StartLSN:   t.startLSN,
		Offset:     t.offset,
		LastLSN:    t.lastLSN,
		Checkpoint: t.checkpoint,
		Sequence:   t.sequence,
		Uncertain:  t.uncertain,
	}
	return wt.Valid()
}

func loadIngestWALDurableTail(dir string) (*ingestWALDurableTail, error) {
	wt, err := wal.LoadDurableTail(dir)
	if err != nil || wt == nil {
		return nil, err
	}
	return &ingestWALDurableTail{
		startLSN:   wt.StartLSN,
		offset:     wt.Offset,
		lastLSN:    wt.LastLSN,
		checkpoint: wt.Checkpoint,
		sequence:   wt.Sequence,
		uncertain:  wt.Uncertain,
	}, nil
}

func encodeIngestWALTail(t ingestWALDurableTail) []byte {
	return wal.EncodeDurableTail(wal.DurableTail{
		StartLSN:   t.startLSN,
		Offset:     t.offset,
		LastLSN:    t.lastLSN,
		Checkpoint: t.checkpoint,
		Sequence:   t.sequence,
		Uncertain:  t.uncertain,
	})
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
	if s.wal.config.Durability != IngestWALDurabilitySync {
		path := filepath.Join(s.wal.config.Dir, ingestWALDurableFile)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return syncDirectory(s.wal.config.Dir)
	}
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
