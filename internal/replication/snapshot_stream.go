package replication

import (
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

	"go.etcd.io/raft/v3/raftpb"
)

const snapshotStreamHeader = "X-GraphDB-Snapshot-Format"
const snapshotMetadataLimit = 1 << 20

type SnapshotSource interface {
	WriteTo(context.Context, io.WriteSeeker) error
	Close() error
}

type StreamingStateMachine interface {
	CaptureSnapshot(context.Context) (SnapshotSource, error)
	RestoreSnapshot(context.Context, uint64, io.ReadSeeker) error
}

type snapshotFile struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

func (s snapshotFile) valid() bool {
	hash, err := hex.DecodeString(s.SHA256)
	return err == nil && len(hash) == sha256.Size && strings.ToLower(s.SHA256) == s.SHA256 && s.Bytes >= sha256.Size
}

func snapshotPath(dir string, index uint64, file snapshotFile) string {
	return filepath.Join(dir, "snapshots", fmt.Sprintf("%020d-%s.snap", index, file.SHA256))
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
	digest := sha256.New()
	result.Bytes, err = io.Copy(io.MultiWriter(file, digest), io.LimitReader(source, maxBytes+1))
	if err != nil {
		return result, err
	}
	if result.Bytes > maxBytes {
		return result, ErrSnapshotTooLarge
	}
	result.SHA256 = hex.EncodeToString(digest.Sum(nil))
	if !result.valid() || (expected != nil && result != *expected) {
		return result, fmt.Errorf("snapshot file integrity failed")
	}
	if err := file.Sync(); err != nil {
		return result, err
	}
	if err := file.Close(); err != nil {
		return result, err
	}
	if err := os.Rename(file.Name(), snapshotPath(dir, index, result)); err != nil {
		return result, err
	}
	directory, err := os.Open(targetDir)
	if err != nil {
		return result, err
	}
	return result, errors.Join(directory.Sync(), directory.Close())
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
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return err
	}
	if size != envelope.File.Bytes || hex.EncodeToString(digest.Sum(nil)) != envelope.File.SHA256 {
		return fmt.Errorf("persisted snapshot file integrity failed")
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

func sealSnapshotFile(dir string, index uint64, file *os.File, maxBytes int64) (snapshotFile, error) {
	info, err := file.Stat()
	if err != nil {
		return snapshotFile{}, err
	}
	if info.Size() > maxBytes {
		return snapshotFile{}, ErrSnapshotTooLarge
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return snapshotFile{}, err
	}
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return snapshotFile{}, err
	}
	stored := snapshotFile{SHA256: hex.EncodeToString(digest.Sum(nil)), Bytes: size}
	if err := file.Sync(); err != nil {
		return stored, err
	}
	if err := file.Close(); err != nil {
		return stored, err
	}
	if err := os.Rename(file.Name(), snapshotPath(dir, index, stored)); err != nil {
		return stored, err
	}
	directory, err := os.Open(filepath.Join(dir, "snapshots"))
	if err != nil {
		return stored, err
	}
	return stored, errors.Join(directory.Sync(), directory.Close())
}

func validateSnapshotFile(dir string, index uint64, descriptor snapshotFile) error {
	file, err := os.Open(snapshotPath(dir, index, descriptor))
	if err != nil {
		return err
	}
	defer file.Close()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return err
	}
	if size != descriptor.Bytes || hex.EncodeToString(digest.Sum(nil)) != descriptor.SHA256 {
		return fmt.Errorf("persisted snapshot file integrity failed")
	}
	return nil
}

func (n *Node) buildStreamSnapshot(source SnapshotSource, index uint64, conf raftpb.ConfState, envelope snapshotEnvelope, done chan<- snapshotBuild) {
	defer n.workers.Done()
	defer source.Close()
	dir := filepath.Join(n.cfg.Dir, "snapshots")
	err := prepareSnapshotDirectory(n.cfg.Dir)
	finish := n.metrics.Start("snapshot_build")
	defer func() { finish(err) }()
	var data []byte
	if err == nil {
		file, createErr := os.CreateTemp(dir, ".build-")
		err = createErr
		if err == nil {
			if n.cfg.SnapshotPreflight != nil {
				bytes := n.cfg.MaxSnapshotBytes + (32 << 20)
				if estimated, ok := source.(interface{ SnapshotBytes() int64 }); ok {
					bytes = min(bytes, estimated.SnapshotBytes())
				}
				err = n.cfg.SnapshotPreflight(n.ctx, bytes)
			}
			if err == nil {
				err = source.WriteTo(n.ctx, file)
			}
			if err == nil {
				_, err = file.Seek(0, io.SeekStart)
			}
			if err == nil {
				var stored snapshotFile
				stored, err = sealSnapshotFile(n.cfg.Dir, index, file, n.cfg.MaxSnapshotBytes+(32<<20))
				if err == nil {
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
	if n.cfg.SnapshotPreflight != nil {
		if err := n.cfg.SnapshotPreflight(r.Context(), envelope.File.Bytes); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	if _, err := persistSnapshotFile(n.cfg.Dir, message.Snapshot.Metadata.Index, r.Body, envelope.File, n.cfg.MaxSnapshotBytes+(32<<20)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := n.raft.Step(r.Context(), message); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func pruneSnapshotFiles(dir string, snapshot raftpb.Snapshot, allOrphans bool) error {
	envelope, err := decodeSnapshot(snapshot.Data)
	if err != nil {
		return nil
	}
	files, err := os.ReadDir(filepath.Join(dir, "snapshots"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	keep := ""
	if envelope.File != nil {
		keep = filepath.Base(snapshotPath(dir, snapshot.Metadata.Index, *envelope.File))
	}
	for _, file := range files {
		if file.Name() == keep {
			continue
		}
		if allOrphans || (!strings.HasPrefix(file.Name(), ".") && file.Name() < fmt.Sprintf("%020d-", snapshot.Metadata.Index)) {
			if err := os.Remove(filepath.Join(dir, "snapshots", file.Name())); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
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
