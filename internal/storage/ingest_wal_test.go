package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestIngestWALConcurrentAppendsRecoverInLSNOrder(t *testing.T) {
	config := testIngestWALConfig(t)
	wal, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("initial records = %d, want 0", len(records))
	}

	const count = 32
	results := make([]IngestWALAppendResult, count)
	var wait sync.WaitGroup
	wait.Add(count)
	for index := range count {
		go func() {
			defer wait.Done()
			result, appendErr := wal.Append(context.Background(), IngestWALAccepted, []byte{byte(index)})
			if appendErr != nil {
				t.Errorf("append %d: %v", index, appendErr)
				return
			}
			results[index] = result
		}()
	}
	wait.Wait()
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	lsns := make([]uint64, 0, count)
	for _, result := range results {
		if !result.Durable {
			t.Fatalf("append result = %#v, want durable sync acknowledgement", result)
		}
		lsns = append(lsns, result.LSN)
	}
	sort.Slice(lsns, func(i, j int) bool { return lsns[i] < lsns[j] })
	for index, lsn := range lsns {
		if lsn != uint64(index+1) {
			t.Fatalf("sorted LSN[%d] = %d, want %d", index, lsn, index+1)
		}
	}

	reopened, recovered, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(recovered) != count {
		t.Fatalf("recovered records = %d, want %d", len(recovered), count)
	}
	for index, record := range recovered {
		if record.LSN != uint64(index+1) || record.Type != IngestWALAccepted {
			t.Fatalf("record[%d] = %#v", index, record)
		}
	}
}

func TestIngestWALTruncatesOnlyIncompleteFinalTail(t *testing.T) {
	config := testIngestWALConfig(t)
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, []byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(config.Dir, ingestWALSegmentName(1))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	goodSize := info.Size()
	partial := encodeIngestWALFrame(IngestWALAccepted, 2, []byte("partial"))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(partial[:len(partial)-3]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(records) != 1 || string(records[0].Payload) != "complete" {
		t.Fatalf("recovered records = %#v", records)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != goodSize {
		t.Fatalf("truncated size = %d, want %d", info.Size(), goodSize)
	}
}

func TestIngestWALRejectsChecksumCorruptionAndSecondProcess(t *testing.T) {
	config := testIngestWALConfig(t)
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenIngestWAL(config); !errors.Is(err, ErrIngestWALLocked) {
		t.Fatalf("second open err = %v, want ErrIngestWALLocked", err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(config.Dir, ingestWALSegmentName(1))
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'X'}, ingestWALHeaderBytes+1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenIngestWAL(config); !errors.Is(err, ErrIngestWALCorrupt) {
		t.Fatalf("corrupt open err = %v, want ErrIngestWALCorrupt", err)
	}
}

func TestIngestWALRejectsLSNGaps(t *testing.T) {
	config := testIngestWALConfig(t)
	path := filepath.Join(config.Dir, ingestWALSegmentName(1))
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	first := encodeIngestWALFrame(IngestWALAccepted, 1, []byte("first"))
	third := encodeIngestWALFrame(IngestWALAccepted, 3, []byte("third"))
	if err := os.WriteFile(path, append(first, third...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenIngestWAL(config); !errors.Is(err, ErrIngestWALCorrupt) {
		t.Fatalf("gap open err = %v, want ErrIngestWALCorrupt", err)
	}
}

func TestIngestWALWriterFencesAfterWriteFailure(t *testing.T) {
	config := testIngestWALConfig(t)
	wal := &IngestWAL{config: config}
	file, err := os.CreateTemp(config.Dir, "closed-writer-*")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	request := ingestWALAppendRequest{
		ctx:     context.Background(),
		kind:    IngestWALAccepted,
		payload: []byte("must-fence"),
		done:    make(chan ingestWALAppendResponse, 1),
	}
	state := ingestWALWriterState{
		wal:        wal,
		file:       file,
		segments:   []ingestWALSegment{{path: path, startLSN: 1}},
		nextLSN:    1,
		writtenLSN: 0,
		durableLSN: 0,
	}
	state.writeRequests([]ingestWALAppendRequest{request})
	response := <-request.done
	if !errors.Is(response.err, ErrIngestWALFenced) {
		t.Fatalf("write failure = %v, want ErrIngestWALFenced", response.err)
	}
	if !state.fenced {
		t.Fatal("writer did not enter fenced state")
	}
	if state.nextLSN != 1 {
		t.Fatalf("next LSN after failed write = %d, want 1", state.nextLSN)
	}
	if !errors.Is(wal.fatalError(), ErrIngestWALFenced) {
		t.Fatalf("WAL fatal error = %v, want ErrIngestWALFenced", wal.fatalError())
	}
	second := ingestWALAppendRequest{
		ctx:     context.Background(),
		kind:    IngestWALAccepted,
		payload: []byte("must-not-append"),
		done:    make(chan ingestWALAppendResponse, 1),
	}
	state.writeRequests([]ingestWALAppendRequest{second})
	if err := (<-second.done).err; !errors.Is(err, ErrIngestWALFenced) {
		t.Fatalf("append after fence = %v, want ErrIngestWALFenced", err)
	}
	if state.nextLSN != 1 {
		t.Fatalf("next LSN after append post-fence = %d, want 1", state.nextLSN)
	}
	if err := state.prune(ingestWALPruneRequest{beforeLSN: 2}); !errors.Is(err, ErrIngestWALFenced) {
		t.Fatalf("prune after fence = %v, want ErrIngestWALFenced", err)
	}
}

func TestIngestWALInitialOpenFailureFencesAndCloseReports(t *testing.T) {
	config := testIngestWALConfig(t)
	if err := os.Mkdir(filepath.Join(config.Dir, ingestWALSegmentName(1)), 0o700); err != nil {
		t.Fatal(err)
	}
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, []byte("after-open-failure")); !errors.Is(err, ErrIngestWALFenced) {
		t.Fatalf("append after initial open failure = %v, want ErrIngestWALFenced", err)
	}
	if err := wal.Close(); !errors.Is(err, ErrIngestWALFenced) {
		t.Fatalf("close after initial open failure = %v, want ErrIngestWALFenced", err)
	}
	if err := wal.Close(); !errors.Is(err, ErrIngestWALFenced) {
		t.Fatalf("second close after initial open failure = %v, want ErrIngestWALFenced", err)
	}
}

func TestIngestWALConcurrentCloseIsIdempotent(t *testing.T) {
	wal, _, err := OpenIngestWAL(testIngestWALConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			errs <- wal.Close()
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent close = %v, want nil", err)
		}
	}
}

func TestIngestWALPruneReclaimsCompletedSegments(t *testing.T) {
	config := testIngestWALConfig(t)
	config.SegmentBytes = 96
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 8 {
		if _, err := wal.Append(context.Background(), IngestWALFinalized, []byte{byte(index), 1, 2, 3, 4, 5, 6, 7}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Prune(context.Background(), 9); err != nil {
		t.Fatal(err)
	}
	entries, err := filepath.Glob(filepath.Join(config.Dir, "*"+ingestWALFileExtension))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("WAL segments after prune = %v, want one active segment", entries)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIngestWALReopensAtCapacityForRecovery(t *testing.T) {
	payload := []byte("fills-the-segment")
	config := testIngestWALConfig(t)
	config.MaxBytes = int64(ingestWALHeaderBytes + len(payload) + ingestWALChecksumBytes)
	config.SegmentBytes = config.MaxBytes
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, payload); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(records) != 1 || string(records[0].Payload) != string(payload) {
		t.Fatalf("recovered records = %#v", records)
	}
	if _, err := reopened.Append(context.Background(), IngestWALAccepted, payload); !errors.Is(err, ErrIngestWALFull) {
		t.Fatalf("append at capacity err = %v, want ErrIngestWALFull", err)
	}
}

func TestIngestWALCheckpointRecoversOnlyActiveTail(t *testing.T) {
	config := testIngestWALConfig(t)
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	results := make([]IngestWALAppendResult, 4)
	for index := range results {
		result, appendErr := wal.Append(
			context.Background(),
			IngestWALAccepted,
			[]byte{byte(index + 1)},
		)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		results[index] = result
	}
	if err := wal.pruneFrom(
		context.Background(),
		results[2].LSN,
		results[2].Segment,
		results[2].Offset,
	); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := loadIngestWALCheckpoint(config.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.NextLSN != results[2].LSN || checkpoint.Segment != results[2].Segment || checkpoint.Offset != results[2].Offset {
		t.Fatalf("checkpoint = %#v, want LSN %d at %s:%d", checkpoint, results[2].LSN, results[2].Segment, results[2].Offset)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(records) != 2 || records[0].LSN != results[2].LSN || records[1].LSN != results[3].LSN {
		t.Fatalf("checkpoint recovery records = %#v, want LSNs %d,%d", records, results[2].LSN, results[3].LSN)
	}
}

func TestIngestWALCorruptCheckpointFallsBackToFullRecovery(t *testing.T) {
	config := testIngestWALConfig(t)
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		if _, err := wal.Append(context.Background(), IngestWALAccepted, []byte{byte(index + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.Dir, ingestWALCheckpointFile), []byte("{invalid"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(records) != 3 {
		t.Fatalf("fallback recovery records = %#v, want all records", records)
	}
	for index, record := range records {
		if record.LSN != uint64(index+1) {
			t.Fatalf("fallback record[%d] = %#v", index, record)
		}
	}
}

func TestIngestWALCorruptCheckpointFallsBackAfterPrunedSegments(t *testing.T) {
	config := testIngestWALConfig(t)
	config.SegmentBytes = 96
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 32)
	for index := range 4 {
		payload[0] = byte(index + 1)
		if _, err := wal.Append(context.Background(), IngestWALAccepted, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Prune(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := filepath.Glob(filepath.Join(config.Dir, "*"+ingestWALFileExtension))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || filepath.Base(entries[0]) != ingestWALSegmentName(3) {
		t.Fatalf("WAL segments after prune = %v, want segments starting at LSN 3", entries)
	}
	if err := os.WriteFile(filepath.Join(config.Dir, ingestWALCheckpointFile), []byte("{invalid"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(records) != 2 || records[0].LSN != 3 || records[1].LSN != 4 {
		t.Fatalf("fallback recovery records = %#v, want LSNs 3,4", records)
	}
	result, err := reopened.Append(context.Background(), IngestWALAccepted, payload)
	if err != nil {
		t.Fatal(err)
	}
	if result.LSN != 5 {
		t.Fatalf("LSN after fallback recovery = %d, want 5", result.LSN)
	}
}

func testIngestWALConfig(t *testing.T) IngestWALConfig {
	t.Helper()
	config := DefaultIngestWALConfig(t.TempDir())
	config.BufferBytes = 1024
	config.FsyncInterval = 2 * time.Millisecond
	config.MaxBytes = 1024 * 1024
	config.SegmentBytes = 64 * 1024
	config.AppendQueue = 128
	return config
}
