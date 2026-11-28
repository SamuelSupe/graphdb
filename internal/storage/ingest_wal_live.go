package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
)

type ingestWALFrameRef struct {
	segment string
	offset  int64
	lsn     uint64
	kind    IngestWALRecordType
}

// The writer owns the references. They change after the frame write and
// required sync, before an acknowledgement can update service state.
type ingestWALLiveRecords map[string][]ingestWALFrameRef

func (live ingestWALLiveRecords) update(id string, ref ingestWALFrameRef) {
	if live == nil {
		return
	}
	switch ref.kind {
	case IngestWALAccepted:
		if ref.segment == "" {
			live[id] = nil
		} else {
			live[id] = []ingestWALFrameRef{ref}
		}
	case IngestWALPrepared, IngestWALPublished:
		if current, ok := live[id]; ok {
			live[id] = append(current, ref)
		}
	case IngestWALFinalized, IngestWALFailed:
		delete(live, id)
	}
}

// A checkpoint supplies the base envelopes it already owns. For the WAL
// suffix, replay only frames still referenced by outstanding requests.
func (s *ingestWALWriterState) recoverLive() (ingestRecovery, error) {
	r := ingestRecovery{pending: make(map[string]*ingestPending, len(s.live)), selective: true}
	if s.checkpointRequired {
		base, _, err := loadIngestWALCheckpoint(s.wal.config.Dir)
		if err != nil {
			return r, err
		}
		if base.Version == 0 || base.Through > s.durableLSN {
			return r, fmt.Errorf("%w: missing or inconsistent ingest checkpoint", ErrIngestWALCorrupt)
		}
		for _, envelope := range base.Pending {
			refs, active := s.live[envelope.RecordID]
			if active && (len(refs) == 0 || refs[0].kind != IngestWALAccepted) {
				r.pending[envelope.RecordID] = &ingestPending{envelope: envelope}
			}
		}
	}
	frames := make(map[uint64]ingestWALFrameRef)
	for _, refs := range s.live {
		for _, ref := range refs {
			frames[ref.lsn] = ref
		}
	}
	var file *os.File
	var segment string
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	for _, lsn := range slices.Sorted(maps.Keys(frames)) {
		ref := frames[lsn]
		if segment != ref.segment {
			if file != nil {
				file.Close()
			}
			var err error
			file, err = os.Open(filepath.Join(s.wal.config.Dir, ref.segment))
			if err != nil {
				return r, err
			}
			segment = ref.segment
		}
		record, err := readIngestWALFrameAt(file, ref)
		if err != nil {
			return r, err
		}
		if err := r.apply(record); err != nil {
			return r, err
		}
	}
	if len(r.pending) != len(s.live) {
		return r, fmt.Errorf("%w: incomplete live ingest WAL index", ErrIngestWALCorrupt)
	}
	for id := range r.pending {
		if _, ok := s.live[id]; !ok {
			return r, fmt.Errorf("%w: unexpected live ingest WAL record", ErrIngestWALCorrupt)
		}
	}
	return r, nil
}

func readIngestWALFrameAt(file io.ReaderAt, ref ingestWALFrameRef) (IngestWALRecord, error) {
	var header [ingestWALHeaderBytes]byte
	if _, err := file.ReadAt(header[:], ref.offset); err != nil {
		return IngestWALRecord{}, err
	}
	size := binary.BigEndian.Uint32(header[16:20])
	if string(header[:4]) != ingestWALMagic || binary.BigEndian.Uint16(header[4:6]) != ingestWALFormatVersion ||
		IngestWALRecordType(header[6]) != ref.kind || binary.BigEndian.Uint64(header[8:16]) != ref.lsn || size > ingestWALMaxPayload {
		return IngestWALRecord{}, fmt.Errorf("%w: invalid live frame at LSN %d", ErrIngestWALCorrupt, ref.lsn)
	}
	frame := make([]byte, int(size)+ingestWALChecksumBytes)
	if _, err := file.ReadAt(frame, ref.offset+ingestWALHeaderBytes); err != nil {
		return IngestWALRecord{}, err
	}
	if crc32.Update(crc32.Checksum(header[:], ingestWALCRCTable), ingestWALCRCTable, frame[:size]) != binary.BigEndian.Uint32(frame[size:]) {
		return IngestWALRecord{}, fmt.Errorf("%w: live frame checksum at LSN %d", ErrIngestWALCorrupt, ref.lsn)
	}
	return IngestWALRecord{Type: ref.kind, LSN: ref.lsn, Segment: ref.segment, Offset: ref.offset, Payload: frame[:size]}, nil
}
