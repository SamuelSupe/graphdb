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

func TestIngestWALReservesControlStateCapacity(t *testing.T) {
	config := testIngestWALConfig(t)
	config.MaxBytes = 4096
	config.SegmentBytes = config.MaxBytes
	config.ControlReserveBytes = 512
	acceptedPayload := make([]byte, 3560)
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, acceptedPayload); err != nil {
		t.Fatalf("accepted payload: %v", err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, nil); !errors.Is(err, ErrIngestWALFull) {
		t.Fatalf("second accepted payload err = %v, want reserved capacity rejection", err)
	}
	if _, err := wal.Append(context.Background(), IngestWALPrepared, make([]byte, 400)); err != nil {
		t.Fatalf("prepared state did not fit in reserved capacity: %v", err)
	}
	if _, err := wal.Append(context.Background(), IngestWALFinalized, make([]byte, 60)); err != nil {
		t.Fatalf("finalized state did not fit in reserved capacity: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(records) != 3 {
		t.Fatalf("recovered records = %d, want accepted/prepared/finalized", len(records))
	}
	if records[0].Type != IngestWALAccepted || records[1].Type != IngestWALPrepared || records[2].Type != IngestWALFinalized {
		t.Fatalf("recovered record types = %#v", records)
	}
}

func TestIngestWALReportsBackgroundWriterStartupFailure(t *testing.T) {
	config := testIngestWALConfig(t)
	wal := &IngestWAL{
		config:   config,
		appendCh: make(chan ingestWALAppendRequest, 1),
		pruneCh:  make(chan ingestWALPruneRequest),
		closeCh:  make(chan struct{}),
		done:     make(chan struct{}),
		ready:    make(chan error, 1),
	}
	missing := filepath.Join(config.Dir, "missing", ingestWALSegmentName(1))
	go wal.run([]ingestWALSegment{{path: missing, startLSN: 1}}, 1, 0)

	if err := <-wal.ready; err == nil {
		t.Fatal("background WAL writer startup unexpectedly succeeded")
	}
	const closers = 8
	errs := make(chan error, closers)
	for range closers {
		go func() { errs <- wal.Close() }()
	}
	message := ""
	for range closers {
		closeErr := <-errs
		if closeErr == nil {
			t.Fatal("WAL close hid the background writer startup failure")
		}
		if message == "" {
			message = closeErr.Error()
		} else if closeErr.Error() != message {
			t.Fatalf("concurrent close err = %v, want %q", closeErr, message)
		}
	}
}

func TestIngestWALRuntimeWriteFailureFailsClosedAndRecovers(t *testing.T) {
	tests := []struct {
		name      string
		failWrite bool
		failShort bool
		failSync  bool
	}{
		{name: "write_error", failWrite: true},
		{name: "short_write", failShort: true},
		{name: "sync_error", failSync: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testIngestWALConfig(t)
			wal, _, err := OpenIngestWAL(config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wal.Append(context.Background(), IngestWALAccepted, []byte("durable")); err != nil {
				_ = wal.Close()
				t.Fatalf("append durable record: %v", err)
			}
			if err := wal.Close(); err != nil {
				t.Fatalf("close initial WAL: %v", err)
			}

			fault := &ingestWALFaultFile{
				failWrite: test.failWrite,
				failShort: test.failShort,
				failSync:  test.failSync,
			}
			faultConfig := config
			faultConfig.openWriterFile = ingestWALFaultOpener(fault)
			failedWAL, recovered, err := OpenIngestWAL(faultConfig)
			if err != nil {
				t.Fatal(err)
			}
			if len(recovered) != 1 || recovered[0].LSN != 1 || string(recovered[0].Payload) != "durable" {
				t.Fatalf("records before injected failure = %#v, want one durable LSN 1 record", recovered)
			}

			_, err = failedWAL.Append(context.Background(), IngestWALAccepted, []byte("must-fail"))
			if !errors.Is(err, ErrIngestWALFailed) {
				t.Fatalf("injected failure err = %v, want ErrIngestWALFailed", err)
			}
			for index := 0; index < 2; index++ {
				_, err = failedWAL.Append(context.Background(), IngestWALAccepted, []byte("must-stay-failed"))
				if !errors.Is(err, ErrIngestWALFailed) {
					t.Fatalf("post-failure append %d err = %v, want ErrIngestWALFailed", index, err)
				}
			}
			pruneCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			err = failedWAL.Prune(pruneCtx, 2)
			cancel()
			if !errors.Is(err, ErrIngestWALFailed) {
				t.Fatalf("post-failure prune err = %v, want ErrIngestWALFailed", err)
			}
			if err := failedWAL.Close(); !errors.Is(err, ErrIngestWALFailed) {
				t.Fatalf("failed WAL close err = %v, want ErrIngestWALFailed", err)
			}

			recoveryConfig := config
			reopened, records, err := OpenIngestWAL(recoveryConfig)
			if err != nil {
				t.Fatalf("reopen after injected failure: %v", err)
			}
			if len(records) < 1 || records[0].LSN != 1 || string(records[0].Payload) != "durable" {
				_ = reopened.Close()
				t.Fatalf("records after injected failure = %#v, want durable LSN 1 record", records)
			}
			for index, record := range records {
				if record.LSN != uint64(index+1) {
					_ = reopened.Close()
					t.Fatalf("recovered record %d LSN = %d, want %d", index, record.LSN, index+1)
				}
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("close recovered WAL: %v", err)
			}
		})
	}
}

func TestIngestWALPruneReopensFromRemainingSegmentLSN(t *testing.T) {
	config := testIngestWALConfig(t)
	config.BufferBytes = 1
	config.SegmentBytes = 96
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = wal.Close()
		}
	})

	payload := []byte("01234567890123456789")
	for index := 0; index < 5; index++ {
		result, err := wal.Append(context.Background(), IngestWALAccepted, payload)
		if err != nil {
			t.Fatalf("append %d: %v", index, err)
		}
		if result.LSN != uint64(index+1) {
			t.Fatalf("append %d LSN = %d, want %d", index, result.LSN, index+1)
		}
	}

	pruneCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = wal.Prune(pruneCtx, 3)
	cancel()
	if err != nil {
		t.Fatalf("prune before LSN 3: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(config.Dir, "*"+ingestWALFileExtension))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("segments after prune = %v, want segments starting at LSN 3 and 5", paths)
	}
	starts := make([]uint64, len(paths))
	for index, path := range paths {
		start, ok := parseIngestWALSegmentName(filepath.Base(path))
		if !ok {
			t.Fatalf("segment path %q has invalid name", path)
		}
		starts[index] = start
	}
	if starts[0] != 3 || starts[1] != 5 {
		t.Fatalf("remaining segment starts = %v, want [3 5]", starts)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reopened, records, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatalf("reopen after prune: %v", err)
	}
	defer reopened.Close()
	if len(records) != 3 {
		t.Fatalf("recovered records after prune = %d, want 3", len(records))
	}
	for index, record := range records {
		wantLSN := uint64(index + 3)
		if record.LSN != wantLSN || string(record.Payload) != string(payload) {
			t.Fatalf("recovered record %d = %#v, want LSN %d with original payload", index, record, wantLSN)
		}
	}
	result, err := reopened.Append(context.Background(), IngestWALAccepted, []byte("next"))
	if err != nil {
		t.Fatalf("append after prune recovery: %v", err)
	}
	if result.LSN != 6 {
		t.Fatalf("append after prune recovery LSN = %d, want 6", result.LSN)
	}
}

func TestIngestWALRuntimeRotateFailureFailsClosedAndRecovers(t *testing.T) {
	config := testIngestWALConfig(t)
	config.BufferBytes = 1
	config.SegmentBytes = 64
	fault := &ingestWALFaultFile{failOpenAfter: 1}
	config.openWriterFile = ingestWALFaultOpener(fault)
	wal, _, err := OpenIngestWAL(config)
	if err != nil {
		t.Fatal(err)
	}

	prefix := []byte("0123456789")
	if _, err := wal.Append(context.Background(), IngestWALAccepted, prefix); err != nil {
		_ = wal.Close()
		t.Fatalf("append valid prefix: %v", err)
	}
	_, err = wal.Append(context.Background(), IngestWALAccepted, []byte("triggers-rotate"))
	if !errors.Is(err, ErrIngestWALFailed) {
		t.Fatalf("rotate failure err = %v, want ErrIngestWALFailed", err)
	}
	for index := 0; index < 2; index++ {
		_, err = wal.Append(context.Background(), IngestWALAccepted, []byte("must-stay-failed"))
		if !errors.Is(err, ErrIngestWALFailed) {
			t.Fatalf("post-rotate-failure append %d err = %v, want ErrIngestWALFailed", index, err)
		}
	}
	if err := wal.Close(); !errors.Is(err, ErrIngestWALFailed) {
		t.Fatalf("failed rotate WAL close err = %v, want ErrIngestWALFailed", err)
	}

	reopened, records, err := OpenIngestWAL(testIngestWALConfigWithDir(config.Dir))
	if err != nil {
		t.Fatalf("reopen after rotate failure: %v", err)
	}
	defer reopened.Close()
	if len(records) != 1 || records[0].LSN != 1 || string(records[0].Payload) != string(prefix) {
		t.Fatalf("records after rotate failure = %#v, want one valid prefix record", records)
	}
	result, err := reopened.Append(context.Background(), IngestWALAccepted, []byte("after-recovery"))
	if err != nil {
		t.Fatalf("append after rotate recovery: %v", err)
	}
	if result.LSN != 2 {
		t.Fatalf("append after rotate recovery LSN = %d, want 2", result.LSN)
	}
}

type ingestWALFaultFile struct {
	file           *os.File
	failWrite      bool
	failShort      bool
	failSync       bool
	failAfterWrite int
	failOpenAfter  int
	openCall       int
	writeCall      int
	syncCall       int
}

func (f *ingestWALFaultFile) Write(payload []byte) (int, error) {
	f.writeCall++
	if f.failAfterWrite > 0 && f.writeCall > f.failAfterWrite {
		return 0, errors.New("injected WAL write failure")
	}
	if f.writeCall == 1 {
		if f.failWrite {
			return 0, errors.New("injected WAL write failure")
		}
		if f.failShort {
			return len(payload) - 1, nil
		}
	}
	return f.file.Write(payload)
}

func (f *ingestWALFaultFile) Sync() error {
	f.syncCall++
	if f.failSync && f.syncCall == 1 {
		return errors.New("injected WAL sync failure")
	}
	return f.file.Sync()
}

func (f *ingestWALFaultFile) Close() error {
	return f.file.Close()
}

func ingestWALFaultOpener(fault *ingestWALFaultFile) func(string) (ingestWALWriteFile, error) {
	return func(path string) (ingestWALWriteFile, error) {
		fault.openCall++
		if fault.failOpenAfter > 0 && fault.openCall > fault.failOpenAfter {
			return nil, errors.New("injected WAL segment open failure")
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}
		fault.file = file
		return fault, nil
	}
}

func testIngestWALConfigWithDir(dir string) IngestWALConfig {
	config := DefaultIngestWALConfig(dir)
	config.BufferBytes = 1024
	config.FsyncInterval = 2 * time.Millisecond
	config.MaxBytes = 1024 * 1024
	config.SegmentBytes = 64
	config.AppendQueue = 128
	return config
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

type ingestWALTestObserver struct {
	mu    sync.Mutex
	syncs []ingestWALSyncObservation
}

type ingestWALSyncObservation struct {
	status  string
	records int
	bytes   int
}

func (o *ingestWALTestObserver) RecordIngestWALAppend(string, string, int, time.Duration) {}

func (o *ingestWALTestObserver) RecordIngestWALSync(status string, records int, bytes int, _ time.Duration) {
	o.mu.Lock()
	o.syncs = append(o.syncs, ingestWALSyncObservation{status: status, records: records, bytes: bytes})
	o.mu.Unlock()
}

func (o *ingestWALTestObserver) RecordIngestWALState(int, int64, uint64, uint64) {}

func (o *ingestWALTestObserver) RecordIngestQueue(int, int64, time.Duration) {}

func (o *ingestWALTestObserver) RecordIngestQueueCache(string) {}

func (o *ingestWALTestObserver) RecordIngestFlush(string, time.Duration, int, int, int, int, int, bool) {
}

func (o *ingestWALTestObserver) RecordIngestRecovery(string, int, int, int, time.Duration) {}

func (o *ingestWALTestObserver) syncSnapshot() []ingestWALSyncObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ingestWALSyncObservation(nil), o.syncs...)
}
