package replication

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"go.etcd.io/raft/v3/raftpb"
)

func inlineSnapshotBody(message raftpb.Message, file *os.File, maxBytes int64) (io.Reader, int64, error) {
	if message.Snapshot == nil {
		return nil, 0, fmt.Errorf("missing inline snapshot")
	}
	snapshot := *message.Snapshot
	envelope, err := decodeSnapshot(snapshot.Data)
	if err != nil || envelope.File == nil {
		return nil, 0, fmt.Errorf("invalid inline snapshot source")
	}
	size := envelope.File.Bytes
	if size > maxBytes {
		return nil, 0, ErrSnapshotTooLarge
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	if info.Size() != size {
		return nil, 0, fmt.Errorf("inline snapshot size changed")
	}
	envelope.Version, envelope.State, envelope.File = 1, nil, nil
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, 0, err
	}
	before, after, ok := bytes.Cut(data, []byte(`"state":null`))
	if !ok {
		return nil, 0, fmt.Errorf("inline snapshot has no state field")
	}
	const statePrefix = `"state":"`
	suffix := append([]byte{'"'}, after...)
	encodedBytes := (size + 2) / 3 * 4
	envelopeBytes := int64(len(before)+len(statePrefix)+len(suffix)) + encodedBytes
	if envelopeBytes > inlineSnapshotLimit(maxBytes) {
		return nil, 0, ErrSnapshotTooLarge
	}

	// Use the generated code for metadata, then add the length-delimited state
	// field. Protobuf readers accept the snapshot and data fields in this order.
	snapshot.Data = nil
	nested, err := snapshot.Marshal()
	if err != nil {
		return nil, 0, err
	}
	nested = binary.AppendUvarint(append(nested, inlineStateTag), uint64(envelopeBytes))
	message.Snapshot = nil
	head, err := message.Marshal()
	if err != nil {
		return nil, 0, err
	}
	head = binary.AppendUvarint(append(head, inlineSnapshotTag), uint64(int64(len(nested))+envelopeBytes))
	head = append(head, nested...)
	head = append(head, before...)
	head = append(head, statePrefix...)
	body := io.MultiReader(bytes.NewReader(head), &inlineBase64Archive{source: file, left: size}, bytes.NewReader(suffix))
	return body, int64(len(head)+len(suffix)) + encodedBytes, nil
}

type inlineBase64Archive struct {
	source  io.Reader
	left    int64
	raw     [24 << 10]byte
	encoded [32 << 10]byte
	pending []byte
}

func (a *inlineBase64Archive) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(a.pending) == 0 {
		if a.left == 0 {
			return 0, io.EOF
		}
		raw := a.raw[:min(int64(len(a.raw)), a.left)]
		if _, err := io.ReadFull(a.source, raw); err != nil {
			return 0, errors.Join(io.ErrUnexpectedEOF, err)
		}
		a.left -= int64(len(raw))
		a.pending = a.encoded[:base64.StdEncoding.EncodedLen(len(raw))]
		base64.StdEncoding.Encode(a.pending, raw)
	}
	n := copy(dst, a.pending)
	a.pending = a.pending[n:]
	return n, nil
}
