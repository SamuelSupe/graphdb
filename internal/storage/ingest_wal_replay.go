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
	"strconv"
	"strings"
)

func scanIngestWAL(dir string, replay func(IngestWALRecord) error) ([]ingestWALSegment, uint64, int64, error) {
	durable, err := loadIngestWALDurableTail(dir)
	if err != nil {
		return nil, 0, 0, err
	}
	checkpoint, checkpointBytes, err := loadIngestWALCheckpoint(dir)
	if err != nil {
		return nil, 0, 0, err
	}
	if checkpoint.Version != 0 && (durable == nil || checkpoint.Through > durable.lastLSN) {
		return nil, 0, 0, fmt.Errorf("%w: checkpoint exceeds durable WAL boundary", ErrIngestWALCorrupt)
	}
	if durable != nil && durable.checkpoint && checkpoint.Version == 0 {
		return nil, 0, 0, fmt.Errorf("%w: required ingest checkpoint is missing", ErrIngestWALCorrupt)
	}
	for _, envelope := range checkpoint.Pending {
		payload, err := json.Marshal(envelope)
		if err == nil && replay != nil {
			err = replay(IngestWALRecord{Type: IngestWALAccepted, LSN: envelope.AcceptedLSN, Payload: payload})
		}
		if err != nil {
			return nil, 0, 0, err
		}
	}
	visitor := func(record IngestWALRecord) error {
		if record.LSN <= checkpoint.Through || replay == nil {
			return nil
		}
		return replay(record)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, 0, err
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ingestWALFileExtension) {
			if _, ok := parseIngestWALSegmentName(entry.Name()); ok {
				paths = append(paths, filepath.Join(dir, entry.Name()))
			}
		}
	}
	sort.Strings(paths)
	if durable != nil {
		found := false
		for _, name := range paths {
			start, _ := parseIngestWALSegmentName(filepath.Base(name))
			found = found || start == durable.startLSN
		}
		if !found {
			return nil, 0, 0, fmt.Errorf("%w: durable WAL segment is missing", ErrIngestWALCorrupt)
		}
	}
	segments := make([]ingestWALSegment, 0, len(paths))
	var lastLSN uint64
	totalBytes := checkpointBytes
	for index, path := range paths {
		startLSN, _ := parseIngestWALSegmentName(filepath.Base(path))
		if index == 0 {
			if checkpoint.Version != 0 && startLSN > checkpoint.Through+1 {
				return nil, 0, 0, fmt.Errorf("%w: WAL suffix after checkpoint is missing", ErrIngestWALCorrupt)
			}
			lastLSN = startLSN - 1
		} else if startLSN != lastLSN+1 {
			return nil, 0, 0, fmt.Errorf(
				"%w: segment %q starts at LSN %d after LSN %d",
				ErrIngestWALCorrupt, path, startLSN, lastLSN,
			)
		}
		isLast := index == len(paths)-1
		segmentLastLSN, size, err := scanIngestWALSegment(path, isLast, lastLSN, visitor, durable)
		if err != nil {
			return nil, 0, 0, err
		}
		segment := ingestWALSegment{path: path, startLSN: startLSN, size: size}
		if segmentLastLSN > lastLSN {
			segment.maxLSN = segmentLastLSN
			lastLSN = segmentLastLSN
		}
		segments = append(segments, segment)
		totalBytes += size
	}
	return segments, lastLSN + 1, totalBytes, nil
}

func scanIngestWALSegment(path string, isLast bool, previousLSN uint64, replay func(IngestWALRecord) error, durable *ingestWALDurableTail) (uint64, int64, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := info.Size()
	start, _ := parseIngestWALSegmentName(filepath.Base(path))
	boundaryVerified := durable != nil && (start > durable.startLSN || (start == durable.startLSN && durable.offset == 0 && previousLSN == durable.lastLSN))
	if durable != nil && start == durable.startLSN && size < durable.offset {
		return 0, 0, fmt.Errorf("%w: WAL is shorter than its durable boundary", ErrIngestWALCorrupt)
	}
	var offset int64
	var lastLSN = previousLSN
	badTail := func(at int64, reason string, incomplete bool) (uint64, int64, error) {
		provenUnacknowledged := durable != nil && !durable.uncertain && boundaryVerified && (start > durable.startLSN || (start == durable.startLSN && at >= durable.offset))
		if isLast && (provenUnacknowledged || (durable == nil && incomplete)) {
			length, err := truncateIngestWALTail(file, path, at)
			return lastLSN, length, err
		}
		return 0, 0, fmt.Errorf("%w: %s at %s:%d", ErrIngestWALCorrupt, reason, path, at)
	}
	checksumTable := crc32.MakeTable(crc32.Castagnoli)
	for offset < size {
		recordOffset := offset
		header := make([]byte, ingestWALHeaderBytes)
		read, readErr := io.ReadFull(file, header)
		if readErr != nil {
			if !errors.Is(readErr, io.ErrUnexpectedEOF) {
				return 0, 0, fmt.Errorf("read WAL header: %w", readErr)
			}
			return badTail(recordOffset, "incomplete header", true)
		}
		offset += int64(read)
		if string(header[:4]) != ingestWALMagic || binary.BigEndian.Uint16(header[4:6]) != ingestWALFormatVersion {
			return badTail(recordOffset, "invalid header", false)
		}
		kind := IngestWALRecordType(header[6])
		lsn := binary.BigEndian.Uint64(header[8:16])
		payloadBytes := binary.BigEndian.Uint32(header[16:20])
		if !validIngestWALRecordType(kind) || payloadBytes > ingestWALMaxPayload || lsn != lastLSN+1 {
			return badTail(recordOffset, "invalid record metadata", false)
		}
		body := make([]byte, int(payloadBytes)+ingestWALChecksumBytes)
		read, readErr = io.ReadFull(file, body)
		if readErr != nil {
			if !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
				return 0, 0, fmt.Errorf("read WAL payload: %w", readErr)
			}
			return badTail(recordOffset, "incomplete payload", true)
		}
		offset += int64(read)
		payload := body[:payloadBytes]
		wantCRC := binary.BigEndian.Uint32(body[payloadBytes:])
		checksum := crc32.Update(crc32.Checksum(header, checksumTable), checksumTable, payload)
		if checksum != wantCRC {
			return badTail(recordOffset, "checksum mismatch", false)
		}
		if durable != nil && start == durable.startLSN && !boundaryVerified && offset >= durable.offset {
			if offset != durable.offset || lsn != durable.lastLSN {
				return 0, 0, fmt.Errorf("%w: WAL durable boundary is not a record boundary", ErrIngestWALCorrupt)
			}
			boundaryVerified = true
		}
		if err := replay(IngestWALRecord{
			Type:    kind,
			LSN:     lsn,
			Segment: filepath.Base(path),
			Offset:  recordOffset,
			Payload: payload,
		}); err != nil {
			return 0, 0, err
		}
		lastLSN = lsn
	}
	if durable != nil && start == durable.startLSN && !boundaryVerified {
		return 0, 0, fmt.Errorf("%w: WAL durable boundary is not present", ErrIngestWALCorrupt)
	}
	return lastLSN, size, nil
}

func truncateIngestWALTail(file *os.File, path string, size int64) (int64, error) {
	if err := file.Truncate(size); err != nil {
		return 0, fmt.Errorf("truncate recoverable ingest WAL tail %q: %w", path, err)
	}
	if err := syncStorageFile(file); err != nil {
		return 0, err
	}
	return size, nil
}

func encodeIngestWALFrame(kind IngestWALRecordType, lsn uint64, payload []byte) []byte {
	frame := make([]byte, ingestWALHeaderBytes+len(payload)+ingestWALChecksumBytes)
	copy(frame[:4], ingestWALMagic)
	binary.BigEndian.PutUint16(frame[4:6], ingestWALFormatVersion)
	frame[6] = byte(kind)
	binary.BigEndian.PutUint64(frame[8:16], lsn)
	binary.BigEndian.PutUint32(frame[16:20], uint32(len(payload)))
	copy(frame[ingestWALHeaderBytes:], payload)
	checksumOffset := ingestWALHeaderBytes + len(payload)
	binary.BigEndian.PutUint32(
		frame[checksumOffset:],
		crc32.Checksum(frame[:checksumOffset], crc32.MakeTable(crc32.Castagnoli)),
	)
	return frame
}

func validIngestWALRecordType(kind IngestWALRecordType) bool {
	return kind >= IngestWALAccepted && kind <= IngestWALFailed
}

func ingestWALRecordTypeName(kind IngestWALRecordType) string {
	switch kind {
	case IngestWALAccepted:
		return "accepted"
	case IngestWALPrepared:
		return "prepared"
	case IngestWALPublished:
		return "published"
	case IngestWALFinalized:
		return "finalized"
	case IngestWALFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func ingestWALSegmentName(startLSN uint64) string {
	return fmt.Sprintf("%020d%s", startLSN, ingestWALFileExtension)
}

func parseIngestWALSegmentName(name string) (uint64, bool) {
	base := strings.TrimSuffix(name, ingestWALFileExtension)
	if base == name || len(base) != 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(base, 10, 64)
	return value, err == nil && value > 0
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return syncStorageFile(dir)
}
