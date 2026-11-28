package replication

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestInlineFileSnapshotBodyMatchesLegacyWire(t *testing.T) {
	for _, size := range []int{24 << 10, (24 << 10) + 1, (24 << 10) + 2} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x00, 0x55, 0xff}, size/3+1)[:size]
			file, err := os.CreateTemp(t.TempDir(), "archive-")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write(payload); err != nil {
				t.Fatal(err)
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(payload)
			envelope := snapshotEnvelope{Version: 2, Protocol: 12, Retired: []uint64{3}, Peers: map[uint64]string{1: "http://one", 2: "http://two"},
				File: &snapshotFile{SHA256: hex.EncodeToString(digest[:]), Bytes: int64(size)}}
			data, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			message := raftpb.Message{From: 1, To: 2, Type: raftpb.MsgSnap, Term: 9, Snapshot: &raftpb.Snapshot{
				Data: data, Metadata: raftpb.SnapshotMetadata{Index: 7, Term: 3, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
			}}
			body, advertised, err := inlineSnapshotBody(message, file, int64(size))
			if err != nil {
				t.Fatal(err)
			}
			wire, err := io.ReadAll(body)
			if err != nil || int64(len(wire)) != advertised {
				t.Fatalf("wire length %d, advertised %d, error %v", len(wire), advertised, err)
			}
			var decoded raftpb.Message
			if err := decoded.Unmarshal(wire); err != nil || decoded.Snapshot == nil {
				t.Fatalf("legacy protobuf decode failed: %v", err)
			}
			got, err := decodeSnapshot(decoded.Snapshot.Data)
			if err != nil || got.Version != 1 || got.Protocol != envelope.Protocol || got.Peers[2] != envelope.Peers[2] ||
				len(got.Retired) != 1 || got.Retired[0] != 3 || !bytes.Equal(got.State, payload) ||
				decoded.From != message.From || decoded.To != message.To || decoded.Term != message.Term ||
				decoded.Snapshot.Metadata.Index != message.Snapshot.Metadata.Index || len(decoded.Snapshot.Metadata.ConfState.Voters) != 2 {
				t.Fatalf("legacy decoded metadata or state changed: error %v, state size %d", err, len(got.State))
			}
		})
	}
}

func TestLargeInlineSnapshotReceive(t *testing.T) {
	payload := bytes.Repeat([]byte("snapshot-state-"), 180000)
	peers := map[uint64]string{1: "http://one", 2: "http://two"}
	// An older encoder can omit the protocol and retirements altogether.
	body, err := json.Marshal(struct {
		State   []byte            `json:"state"`
		Version int               `json:"version"`
		Peers   map[uint64]string `json:"peers"`
	}{payload, 1, peers})
	if err != nil {
		t.Fatal(err)
	}
	invalidState := bytes.Clone(body)
	invalidState[len(`{"state":"`)+65536] = '!'
	stateStart := len(`{"state":"`)
	escapedState := append(bytes.Clone(body[:stateStart]), fmt.Sprintf(`\u%04x`, body[stateStart])...)
	escapedState = append(escapedState, body[stateStart+1:]...)
	literalNewline := bytes.Clone(body)
	literalNewline[stateStart+65536] = '\n'
	for _, tc := range []struct {
		name      string
		envelope  []byte
		status    int
		chunked   bool
		truncated bool
		from      uint64
		maxBytes  int64
	}{
		{name: "fixed_length", envelope: body, status: http.StatusNoContent},
		{name: "chunked", envelope: body, status: http.StatusNoContent, chunked: true},
		{name: "escaped_base64", envelope: escapedState, status: http.StatusNoContent},
		{name: "invalid_base64", envelope: invalidState, status: http.StatusBadRequest},
		{name: "invalid_json_newline", envelope: literalNewline, status: http.StatusBadRequest},
		{name: "duplicate_state", envelope: append(bytes.Clone(body[:len(body)-1]), `,"state":"AA=="}`...), status: http.StatusBadRequest},
		{name: "truncated_frame", envelope: body, status: http.StatusBadRequest, truncated: true},
		{name: "unknown_peer", envelope: body, status: http.StatusBadRequest, from: 3},
		{name: "over_raw_budget", envelope: body, status: http.StatusRequestEntityTooLarge, maxBytes: int64(len(payload) - 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			maxBytes := tc.maxBytes
			if maxBytes == 0 {
				maxBytes = int64(len(payload))
			}
			receiver := &transportStepRaft{}
			node := &Node{
				cfg: Config{ID: 2, ClusterID: "test", Token: "test-token", Dir: dir, MaxSnapshotBytes: maxBytes},
				ctx: t.Context(), raft: receiver, peers: peers, machine: &retrySnapshotMachine{},
				receiveLargeMessage: make(chan struct{}, 1),
			}
			from := max(tc.from, 1)
			message := raftpb.Message{From: from, To: 2, Type: raftpb.MsgSnap, Term: 9, Snapshot: &raftpb.Snapshot{
				Data: tc.envelope, Metadata: raftpb.SnapshotMetadata{Index: 7, Term: 3, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}},
			}}
			packet, err := message.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if tc.truncated {
				packet = packet[:len(packet)-1]
			}
			request := httptest.NewRequest(http.MethodPost, "/raft/message", bytes.NewReader(packet))
			request.Header.Set("Authorization", "Bearer "+node.cfg.Token)
			request.Header.Set("X-Raft-Cluster", node.cfg.ClusterID)
			request.Header.Set(protocolHeader, fmt.Sprint(ProtocolVersion))
			if tc.chunked {
				request.ContentLength = -1
			}
			response := httptest.NewRecorder()
			node.Handler().ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("HTTP %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
			entries, err := os.ReadDir(filepath.Join(dir, "snapshots"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if tc.status != http.StatusNoContent {
				if receiver.last.Snapshot != nil || len(entries) != 0 {
					t.Fatalf("rejected snapshot was published: %+v files=%v", receiver.last, entries)
				}
				return
			}
			got := receiver.last
			if got.Snapshot == nil || got.Term != 9 || got.Snapshot.Metadata.Index != 7 || got.Snapshot.Metadata.Term != 3 ||
				len(got.Snapshot.Metadata.ConfState.Voters) != 2 {
				t.Fatalf("Raft metadata was lost: %+v", got)
			}
			decoded, err := decodeSnapshot(got.Snapshot.Data)
			hash := sha256.Sum256(payload)
			if err != nil || decoded.File == nil || decoded.File.Bytes != int64(len(payload)) || decoded.File.SHA256 != hex.EncodeToString(hash[:]) || len(entries) != 1 {
				t.Fatalf("snapshot descriptor %+v, files=%v, err=%v", decoded.File, entries, err)
			}
			if err := validateSnapshotFile(dir, got.Snapshot.Metadata.Index, *decoded.File); err != nil {
				t.Fatal(err)
			}
		})
	}
}
