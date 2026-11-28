package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

const snapshotStreamHeader = "X-GraphDB-Snapshot-Format"
const snapshotMetadataLimit = 1 << 20
const snapshotPayloadDigestProtocol = 13

type SnapshotSource interface {
	WriteTo(context.Context, io.WriteSeeker) error
	Close() error
}

// A payload digest source writes a 32-byte SHA-256 prefix followed by the
// bytes it has already hashed. Only sources with this layout can skip rehashing.
type snapshotPayloadDigestSource interface {
	WriteToWithPayloadDigest(context.Context, io.WriteSeeker) ([sha256.Size]byte, error)
}

type StreamingStateMachine interface {
	CaptureSnapshot(context.Context) (SnapshotSource, error)
	RestoreSnapshot(context.Context, uint64, io.ReadSeeker) error
}

type snapshotFile struct {
	SHA256        string `json:"sha256"`
	Bytes         int64  `json:"bytes"`
	PayloadDigest bool   `json:"payload_digest,omitempty"`
}

func (s snapshotFile) valid() bool {
	hash, err := hex.DecodeString(s.SHA256)
	return err == nil && len(hash) == sha256.Size && strings.ToLower(s.SHA256) == s.SHA256 && s.Bytes >= sha256.Size
}

func snapshotPath(dir string, index uint64, file snapshotFile) string {
	return filepath.Join(dir, "snapshots", fmt.Sprintf("%020d-%s.snap", index, file.SHA256))
}

func copySnapshotFile(output io.Writer, source io.Reader, payloadDigest bool) (snapshotFile, error) {
	result := snapshotFile{PayloadDigest: payloadDigest}
	var prefix [sha256.Size]byte
	if payloadDigest {
		if _, err := io.ReadFull(source, prefix[:]); err != nil {
			return result, err
		}
		n, err := io.Copy(output, bytes.NewReader(prefix[:]))
		result.Bytes = n
		if err != nil {
			return result, err
		}
	}
	digest := sha256.New()
	size, err := io.Copy(io.MultiWriter(output, digest), source)
	result.Bytes += size
	if err != nil {
		return result, err
	}
	checksum := digest.Sum(nil)
	if payloadDigest && !bytes.Equal(prefix[:], checksum) {
		return result, fmt.Errorf("snapshot payload digest mismatch")
	}
	result.SHA256 = hex.EncodeToString(checksum)
	return result, nil
}

func persistSnapshotFile(dir string, index uint64, source io.Reader, expected *snapshotFile, maxBytes int64) (result snapshotFile, err error) {
	targetDir := filepath.Join(dir, "snapshots")
	if err := prepareSnapshotDirectory(dir); err != nil {
		return result, err
	}
	file, err := os.CreateTemp(targetDir, ".snapshot-")
	if err != nil {
		return result, err
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	result, err = copySnapshotFile(file, io.LimitReader(source, maxBytes+1), expected != nil && expected.PayloadDigest)
	if result.Bytes > maxBytes {
		return result, ErrSnapshotTooLarge
	}
	if err != nil {
		return result, err
	}
	if !result.valid() || (expected != nil && result != *expected) {
		return result, fmt.Errorf("snapshot file integrity failed")
	}
	return result, publishSnapshotFile(dir, index, file, result)
}

func publishSnapshotFile(dir string, index uint64, file *os.File, descriptor snapshotFile) error {
	_, err := publishSnapshotFileExclusive(dir, index, file, descriptor)
	return err
}

func publishSnapshotFileExclusive(dir string, index uint64, file *os.File, descriptor snapshotFile) (bool, error) {
	if err := file.Sync(); err != nil {
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	target := snapshotPath(dir, index, descriptor)
	created := false
	if err := os.Link(file.Name(), target); err != nil {
		if !os.IsExist(err) {
			if _, statErr := os.Stat(target); statErr == nil {
				return false, nil
			}
			if renameErr := os.Rename(file.Name(), target); renameErr != nil {
				return false, renameErr
			}
			created = true
		}
	} else {
		created = true
	}
	directory, err := os.Open(filepath.Join(dir, "snapshots"))
	if err != nil {
		return created, err
	}
	return created, errors.Join(directory.Sync(), directory.Close())
}

func stageSnapshotFile(dir string, source io.Reader, expected *snapshotFile, maxBytes int64) (file *os.File, result snapshotFile, err error) {
	targetDir := filepath.Join(dir, "snapshots")
	if err := prepareSnapshotDirectory(dir); err != nil {
		return nil, result, err
	}
	file, err = os.CreateTemp(targetDir, ".snapshot-")
	if err != nil {
		return nil, result, err
	}
	result, err = copySnapshotFile(file, io.LimitReader(source, maxBytes+1), expected != nil && expected.PayloadDigest)
	if result.Bytes > maxBytes {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, result, ErrSnapshotTooLarge
	}
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, result, err
	}
	if !result.valid() || (expected != nil && result != *expected) {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, result, fmt.Errorf("snapshot file integrity failed")
	}
	return file, result, nil
}

func (n *Node) stepReceivedSnapshot(ctx context.Context, message raftpb.Message, staged *os.File, descriptor snapshotFile) error {
	index := message.Snapshot.Metadata.Index
	n.snapshotReceiveMu.Lock()
	defer n.snapshotReceiveMu.Unlock()
	if index <= n.committed.Load() || index <= n.snapshotIndex.Load() {
		return nil
	}
	created, err := publishSnapshotFileExclusive(n.cfg.Dir, index, staged, descriptor)
	if err != nil {
		return err
	}
	if err := n.raft.Step(ctx, message); err != nil {
		if created && index > n.snapshotIndex.Load() {
			_ = os.Remove(snapshotPath(n.cfg.Dir, index, descriptor))
		}
		return err
	}
	return nil
}

func restoreSnapshot(ctx context.Context, machine StateMachine, dir string, snapshot raftpb.Snapshot) error {
	envelope, err := decodeSnapshot(snapshot.Data)
	if err != nil {
		return err
	}
	if envelope.File == nil {
		return machine.Restore(ctx, snapshot.Metadata.Index, envelope.State)
	}
	streaming, ok := machine.(StreamingStateMachine)
	if !ok {
		return fmt.Errorf("state machine cannot restore streaming snapshots")
	}
	file, err := os.Open(snapshotPath(dir, snapshot.Metadata.Index, *envelope.File))
	if err != nil {
		return err
	}
	defer file.Close()
	if err := verifySnapshotFile(file, *envelope.File); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return streaming.RestoreSnapshot(ctx, snapshot.Metadata.Index, file)
}

func (n *Node) startStreamSnapshot(conf raftpb.ConfState) (result chan snapshotBuild, err error) {
	finish := n.metrics.Start("snapshot_capture")
	defer func() { finish(err) }()
	machine, ok := n.machine.(StreamingStateMachine)
	if !ok {
		return nil, fmt.Errorf("state machine does not support streaming snapshots")
	}
	index := n.applied.Load()
	envelope, err := n.snapshotMetadata(index)
	if err != nil {
		return nil, err
	}
	source, err := machine.CaptureSnapshot(n.ctx)
	if err != nil {
		return nil, err
	}
	building := make(chan snapshotBuild, 1)
	n.workers.Add(1)
	go n.buildStreamSnapshot(source, index, conf, envelope, building)
	return building, nil
}

func sealSnapshotFile(dir string, index uint64, file *os.File, maxBytes int64, payloadDigest *[sha256.Size]byte) (snapshotFile, error) {
	info, err := file.Stat()
	if err != nil {
		return snapshotFile{}, err
	}
	if info.Size() > maxBytes {
		return snapshotFile{}, ErrSnapshotTooLarge
	}
	var stored snapshotFile
	if payloadDigest != nil {
		var prefix [sha256.Size]byte
		if _, err := file.ReadAt(prefix[:], 0); err != nil {
			return stored, err
		}
		if prefix != *payloadDigest {
			return stored, fmt.Errorf("snapshot prefix does not match archive digest")
		}
		stored = snapshotFile{SHA256: hex.EncodeToString(prefix[:]), Bytes: info.Size(), PayloadDigest: true}
	} else {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return stored, err
		}
		stored, err = copySnapshotFile(io.Discard, file, false)
		if err != nil {
			return stored, err
		}
	}
	return stored, publishSnapshotFile(dir, index, file, stored)
}

func validateSnapshotFile(dir string, index uint64, descriptor snapshotFile) error {
	file, err := os.Open(snapshotPath(dir, index, descriptor))
	if err != nil {
		return err
	}
	defer file.Close()
	return verifySnapshotFile(file, descriptor)
}

func verifySnapshotFile(file io.Reader, descriptor snapshotFile) error {
	actual, err := copySnapshotFile(io.Discard, file, descriptor.PayloadDigest)
	if err != nil {
		return err
	}
	if actual != descriptor {
		return fmt.Errorf("persisted snapshot file integrity failed")
	}
	return nil
}

func (n *Node) buildStreamSnapshot(source SnapshotSource, index uint64, conf raftpb.ConfState, envelope snapshotEnvelope, done chan<- snapshotBuild) {
	defer n.workers.Done()
	closeSource := sync.OnceFunc(func() { _ = source.Close() })
	defer closeSource()
	dir := filepath.Join(n.cfg.Dir, "snapshots")
	err := prepareSnapshotDirectory(n.cfg.Dir)
	finish := n.metrics.Start("snapshot_build")
	defer func() { finish(err) }()
	var data []byte
	if err == nil {
		file, createErr := os.CreateTemp(dir, ".build-")
		err = createErr
		if err == nil {
			var payloadDigest *[sha256.Size]byte
			if n.cfg.SnapshotPreflight != nil {
				bytes := n.cfg.MaxSnapshotBytes + (32 << 20)
				if estimated, ok := source.(interface{ SnapshotBytes() int64 }); ok {
					bytes = min(bytes, estimated.SnapshotBytes())
				}
				err = n.cfg.SnapshotPreflight(n.ctx, bytes)
			}
			if err == nil {
				if digested, ok := source.(snapshotPayloadDigestSource); ok && envelope.Protocol >= snapshotPayloadDigestProtocol {
					var checksum [sha256.Size]byte
					checksum, err = digested.WriteToWithPayloadDigest(n.ctx, file)
					payloadDigest = &checksum
				} else {
					err = source.WriteTo(n.ctx, file)
				}
			}
			if err == nil && !n.cfg.StreamSnapshots {
				info, statErr := file.Stat()
				err = statErr
				if err == nil {
					if info.Size() > n.cfg.MaxSnapshotBytes {
						err = ErrSnapshotTooLarge
					}
				}
			}
			if err == nil {
				var stored snapshotFile
				stored, err = sealSnapshotFile(n.cfg.Dir, index, file, n.cfg.MaxSnapshotBytes+(32<<20), payloadDigest)
				if err == nil {
					// Only the descriptor enters Raft persistence. Default-mode
					// transport expands the file for format-1 peers off that loop.
					envelope.Version = 2
					envelope.State = nil
					envelope.File = &stored
					data, err = json.Marshal(envelope)
				}
			}
			file.Close()
			os.Remove(file.Name())
		}
	}
	// Completing a probe can permit another capture. Release the pinned source
	// before exposing this result, including when construction failed.
	closeSource()
	select {
	case done <- snapshotBuild{request: snapshotRequest{index: index, conf: conf, data: data, done: make(chan error, 1)}, err: err}:
	case <-n.ctx.Done():
	}
}

type snapshotBuild struct {
	request snapshotRequest
	err     error
}

func (n *Node) receiveSnapshot(w http.ResponseWriter, r *http.Request) {
	if !n.protocolCompatible(r.Header.Get(protocolHeader)) || r.Header.Get(snapshotStreamHeader) != "2" {
		http.Error(w, "incompatible snapshot format", http.StatusUpgradeRequired)
		return
	}
	if n.receiveSnapshotSlots != nil {
		select {
		case n.receiveSnapshotSlots <- struct{}{}:
			defer func() { <-n.receiveSnapshotSlots }()
		default:
			http.Error(w, "snapshot receive concurrency limit reached", http.StatusServiceUnavailable)
			return
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, n.cfg.MaxSnapshotBytes+(32<<20)+snapshotMetadataLimit+4)
	var prefix [4]byte
	if _, err := io.ReadFull(r.Body, prefix[:]); err != nil {
		http.Error(w, "truncated metadata", http.StatusBadRequest)
		return
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size == 0 || size > snapshotMetadataLimit {
		http.Error(w, "oversized metadata", http.StatusRequestEntityTooLarge)
		return
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(r.Body, data); err != nil {
		http.Error(w, "truncated metadata", http.StatusBadRequest)
		return
	}
	var message raftpb.Message
	if err := message.Unmarshal(data); err != nil || message.Type != raftpb.MsgSnap || message.Snapshot == nil {
		http.Error(w, "invalid snapshot message", http.StatusBadRequest)
		return
	}
	n.peerMu.RLock()
	_, known := n.peers[message.From]
	n.peerMu.RUnlock()
	if !known || message.From == n.cfg.ID || message.To != n.cfg.ID || message.Snapshot.Metadata.Index == 0 {
		http.Error(w, "invalid snapshot peer", http.StatusBadRequest)
		return
	}
	envelope, err := decodeSnapshot(message.Snapshot.Data)
	if err != nil || envelope.File == nil || envelope.File.Bytes > n.cfg.MaxSnapshotBytes+(32<<20) {
		http.Error(w, "invalid snapshot envelope", http.StatusBadRequest)
		return
	}
	if err := n.available(false); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if message.Snapshot.Metadata.Index <= n.committed.Load() || message.Snapshot.Metadata.Index <= n.snapshotIndex.Load() {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, envelope.File.Bytes+1))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if n.cfg.SnapshotPreflight != nil {
		if err := n.cfg.SnapshotPreflight(r.Context(), envelope.File.Bytes); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	staged, persisted, err := stageSnapshotFile(n.cfg.Dir, r.Body, envelope.File, n.cfg.MaxSnapshotBytes+(32<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer func() {
		_ = staged.Close()
		_ = os.Remove(staged.Name())
	}()
	if err := n.stepReceivedSnapshot(r.Context(), message, staged, persisted); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func pruneSnapshotFiles(dir string, snapshot raftpb.Snapshot, allOrphans bool) ([]os.FileInfo, error) {
	envelope, err := decodeSnapshot(snapshot.Data)
	if err != nil {
		return nil, nil
	}
	keep := ""
	if envelope.File != nil {
		keep = filepath.Base(snapshotPath(dir, snapshot.Metadata.Index, *envelope.File))
	}
	return pruneSnapshotFilesBefore(dir, snapshot.Metadata.Index, keep, allOrphans)
}

func pruneSnapshotFilesBefore(dir string, index uint64, keep string, allOrphans bool) ([]os.FileInfo, error) {
	files, err := os.ReadDir(filepath.Join(dir, "snapshots"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var failed []os.FileInfo
	var cleanupErr error
	for _, file := range files {
		if file.Name() == keep {
			continue
		}
		if allOrphans || (!strings.HasPrefix(file.Name(), ".") && file.Name() < fmt.Sprintf("%020d-", index)) {
			if err := os.Remove(filepath.Join(dir, "snapshots", file.Name())); err != nil && !os.IsNotExist(err) {
				info, infoErr := file.Info()
				if infoErr == nil {
					failed = append(failed, info)
				}
				cleanupErr = errors.Join(cleanupErr, err, infoErr)
			}
		}
	}
	return failed, cleanupErr
}

func prepareSnapshotDirectory(dir string) error {
	if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0700); err != nil {
		return err
	}
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(parent.Sync(), parent.Close())
}

func drainPackets(queue chan packet) {
	for {
		select {
		case packet := <-queue:
			if packet.file != nil {
				packet.file.Close()
			}
		default:
			return
		}
	}
}
