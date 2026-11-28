// Package wal encapsulates durable WAL boundary encoding, dual-slot CRC32C
// validation, and recovery sentinel errors.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	DurableFileName    = ".durable"
	CheckpointFileName = ".checkpoint"
	TailSlotBytes      = 44
)

var (
	CRCTable     = crc32.MakeTable(crc32.Castagnoli)
	ErrCorrupt   = errors.New("ingest WAL corrupt")
	ErrFailed    = errors.New("ingest WAL failed")
	ErrQueueFull = errors.New("ingest WAL queue full")
)

type DurableTail struct {
	StartLSN   uint64
	Offset     int64
	LastLSN    uint64
	Checkpoint bool
	Sequence   uint64
	Uncertain  bool
}

func (t *DurableTail) Valid() bool {
	return t.StartLSN > 0 && t.Offset >= 0 && t.LastLSN >= t.StartLSN-1 && (t.Offset == 0) == (t.LastLSN == t.StartLSN-1)
}

func LoadDurableTail(dir string) (*DurableTail, error) {
	data, err := os.ReadFile(filepath.Join(dir, DurableFileName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	bad := func() (*DurableTail, error) {
		return nil, fmt.Errorf("%w: invalid durable WAL boundary", ErrCorrupt)
	}
	table := CRCTable
	if len(data) == 36 {
		if string(data[:4]) != "GIWD" || data[4] != 1 || data[5] > 1 || crc32.Checksum(data[:32], table) != binary.BigEndian.Uint32(data[32:]) {
			return bad()
		}
		tail := &DurableTail{
			StartLSN:   binary.BigEndian.Uint64(data[8:16]),
			Offset:     int64(binary.BigEndian.Uint64(data[16:24])),
			LastLSN:    binary.BigEndian.Uint64(data[24:32]),
			Checkpoint: data[5] == 1,
		}
		if !tail.Valid() {
			return bad()
		}
		return tail, nil
	}
	if len(data) != 2*TailSlotBytes {
		return bad()
	}
	var valid []*DurableTail
	for slot := range 2 {
		b := data[slot*TailSlotBytes : (slot+1)*TailSlotBytes]
		if string(b[:4]) != "GIWD" || b[4] != 2 || b[5] > 1 || b[6] != 0 || b[7] != 0 || crc32.Checksum(b[:40], table) != binary.BigEndian.Uint32(b[40:]) {
			continue
		}
		t := &DurableTail{
			Sequence:   binary.BigEndian.Uint64(b[8:16]),
			StartLSN:   binary.BigEndian.Uint64(b[16:24]),
			Offset:     int64(binary.BigEndian.Uint64(b[24:32])),
			LastLSN:    binary.BigEndian.Uint64(b[32:40]),
			Checkpoint: b[5] == 1,
		}
		if t.Valid() && t.Sequence%2 == uint64(slot) {
			valid = append(valid, t)
		}
	}
	if len(valid) == 0 {
		return bad()
	}
	latest := valid[0]
	if len(valid) == 2 {
		old := valid[1]
		if old.Sequence > latest.Sequence {
			old, latest = latest, old
		}
		if latest.Sequence-old.Sequence != 1 || latest.LastLSN < old.LastLSN || latest.StartLSN < old.StartLSN {
			return bad()
		}
	} else {
		latest.Uncertain = true
	}
	return latest, nil
}

func EncodeDurableTail(t DurableTail) []byte {
	b := make([]byte, TailSlotBytes)
	copy(b, "GIWD")
	b[4] = 2
	if t.Checkpoint {
		b[5] = 1
	}
	binary.BigEndian.PutUint64(b[8:16], t.Sequence)
	binary.BigEndian.PutUint64(b[16:24], t.StartLSN)
	binary.BigEndian.PutUint64(b[24:32], uint64(t.Offset))
	binary.BigEndian.PutUint64(b[32:40], t.LastLSN)
	binary.BigEndian.PutUint32(b[40:], crc32.Checksum(b[:40], CRCTable))
	return b
}
