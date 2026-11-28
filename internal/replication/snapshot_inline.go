package replication

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

// These are the existing raftpb.Message.snapshot and raftpb.Snapshot.data tags.
// Leave all remaining protobuf validation to the generated unmarshallers.
const inlineSnapshotTag = 9<<3 | 2
const inlineStateTag = 1<<3 | 2

func splitInlineField(input io.Reader, field uint64, readField func(io.Reader, int64) error) ([]byte, error) {
	reader := bufio.NewReaderSize(input, 32<<10)
	var metadata []byte
	found := false
	for {
		tag, err := binary.ReadUvarint(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if tag>>3 == 0 || tag>>3 >= 1<<29 {
			return nil, fmt.Errorf("invalid snapshot protobuf tag")
		}
		if tag == field {
			if found {
				return nil, fmt.Errorf("repeated snapshot protobuf field")
			}
			found = true
			size, err := readInlineFieldSize(reader)
			if err != nil {
				return nil, err
			}
			limited := &io.LimitedReader{R: reader, N: size}
			if err := readField(limited, size); err != nil {
				return nil, err
			}
			if limited.N != 0 {
				return nil, io.ErrUnexpectedEOF
			}
			continue
		}
		metadata = binary.AppendUvarint(metadata, tag)
		if len(metadata) > snapshotMetadataLimit {
			return nil, fmt.Errorf("oversized snapshot protobuf metadata")
		}
		var size int64
		switch tag & 7 {
		case 0:
			value, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, err
			}
			metadata = binary.AppendUvarint(metadata, value)
		case 1:
			size = 8
		case 2:
			size, err = readInlineFieldSize(reader)
			if err != nil {
				return nil, err
			}
			metadata = binary.AppendUvarint(metadata, uint64(size))
		case 5:
			size = 4
		default:
			return nil, fmt.Errorf("unsupported snapshot protobuf field")
		}
		if size > int64(snapshotMetadataLimit-len(metadata)) {
			return nil, fmt.Errorf("oversized snapshot protobuf metadata")
		}
		start := len(metadata)
		metadata = append(metadata, make([]byte, int(size))...)
		if _, err := io.ReadFull(reader, metadata[start:]); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, fmt.Errorf("missing snapshot protobuf field")
	}
	return metadata, nil
}

func readInlineFieldSize(reader io.ByteReader) (int64, error) {
	size, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	if size > math.MaxInt64 {
		return 0, fmt.Errorf("invalid snapshot protobuf length")
	}
	return int64(size), nil
}

type inlineStateMarker bool

func (m *inlineStateMarker) UnmarshalJSON(data []byte) error {
	if bool(*m) || !bytes.Equal(data, []byte(`"AA=="`)) {
		return fmt.Errorf("invalid or repeated snapshot state")
	}
	*m = true
	return nil
}

func readInlineSnapshotEnvelope(input io.Reader, copyState func(io.Reader) error) (snapshotEnvelope, error) {
	var header bytes.Buffer
	decoder := json.NewDecoder(io.TeeReader(io.LimitReader(input, snapshotMetadataLimit), &header))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return snapshotEnvelope{}, fmt.Errorf("invalid inline snapshot envelope")
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return snapshotEnvelope{}, err
		}
		if key, ok := token.(string); ok && strings.EqualFold(key, "state") {
			prefix := bytes.Clone(header.Bytes()[:int(decoder.InputOffset())])
			reader := bufio.NewReaderSize(io.MultiReader(decoder.Buffered(), input), 32<<10)
			if err := expectInlineJSON(reader, ':'); err != nil {
				return snapshotEnvelope{}, err
			}
			if err := expectInlineJSON(reader, '"'); err != nil {
				return snapshotEnvelope{}, err
			}
			state := &inlineBase64String{reader: reader}
			if err := copyState(base64.NewDecoder(base64.StdEncoding, state)); err != nil {
				return snapshotEnvelope{}, err
			}
			if !state.done {
				return snapshotEnvelope{}, io.ErrUnexpectedEOF
			}
			// Validate the original envelope with a small placeholder in place
			// of the streamed state, while detecting a second state field.
			prefix = append(prefix, `:"AA=="`...)
			suffix, err := io.ReadAll(io.LimitReader(reader, int64(snapshotMetadataLimit-len(prefix))+1))
			if err != nil {
				return snapshotEnvelope{}, err
			}
			if len(prefix)+len(suffix) > snapshotMetadataLimit {
				return snapshotEnvelope{}, fmt.Errorf("oversized inline snapshot metadata")
			}
			var decoded struct {
				snapshotEnvelope
				State inlineStateMarker `json:"state"`
			}
			if err := json.Unmarshal(append(prefix, suffix...), &decoded); err != nil || !bool(decoded.State) || decoded.Version != 1 {
				return snapshotEnvelope{}, fmt.Errorf("invalid inline snapshot envelope")
			}
			decoded.snapshotEnvelope.State = []byte{0}
			return decoded.snapshotEnvelope, decoded.snapshotEnvelope.validate()
		}
		var metadata json.RawMessage
		if err := decoder.Decode(&metadata); err != nil {
			return snapshotEnvelope{}, err
		}
	}
	return snapshotEnvelope{}, fmt.Errorf("missing inline snapshot state")
}

func expectInlineJSON(reader *bufio.Reader, expected byte) error {
	for {
		value, err := reader.ReadByte()
		if err != nil {
			return err
		}
		switch value {
		case ' ', '\r', '\n', '\t':
			continue
		default:
			if value != expected {
				return fmt.Errorf("invalid inline snapshot state")
			}
			return nil
		}
	}
}

// Expose the contents of a JSON string as a reader, leaving the rest of the
// document unread. Escapes may represent ASCII base64 bytes or CR/LF.
type inlineBase64String struct {
	reader *bufio.Reader
	done   bool
}

func (s *inlineBase64String) Read(dst []byte) (int, error) {
	if s.done {
		return 0, io.EOF
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if _, err := s.reader.Peek(1); err != nil {
		return 0, errors.Join(io.ErrUnexpectedEOF, err)
	}
	part, _ := s.reader.Peek(min(len(dst), s.reader.Buffered()))
	n := bytes.IndexAny(part, "\"\\\r\n")
	if n != 0 {
		if n < 0 {
			n = len(part)
		}
		copy(dst, part[:n])
		_, err := s.reader.Discard(n)
		return n, err
	}
	value, _ := s.reader.ReadByte()
	switch value {
	case '"':
		s.done = true
		return 0, io.EOF
	case '\\':
		var quoted [8]byte
		quoted[0], quoted[1] = '"', '\\'
		escape, err := s.reader.ReadByte()
		if err != nil {
			return 0, io.ErrUnexpectedEOF
		}
		quoted[2], quoted[3] = escape, '"'
		size := 4
		if escape == 'u' {
			if _, err := io.ReadFull(s.reader, quoted[3:7]); err != nil {
				return 0, io.ErrUnexpectedEOF
			}
			quoted[7], size = '"', 8
		}
		var decoded string
		if err := json.Unmarshal(quoted[:size], &decoded); err != nil || len(decoded) != 1 {
			return 0, fmt.Errorf("invalid inline snapshot state escape")
		}
		dst[0] = decoded[0]
		return 1, nil
	default:
		return 0, fmt.Errorf("invalid literal newline in inline snapshot state")
	}
}

func (n *Node) receiveInlineSnapshot(w http.ResponseWriter, r *http.Request, body io.Reader) {
	var staged *os.File
	var descriptor snapshotFile
	var envelope snapshotEnvelope
	var snapshot raftpb.Snapshot
	var storageErr error
	defer func() {
		if staged != nil {
			staged.Close()
			os.Remove(staged.Name())
		}
	}()
	metadata, err := splitInlineField(body, inlineSnapshotTag, func(input io.Reader, _ int64) error {
		fields, err := splitInlineField(input, inlineStateTag, func(data io.Reader, size int64) error {
			if size > inlineSnapshotLimit(n.cfg.MaxSnapshotBytes) {
				return ErrSnapshotTooLarge
			}
			var err error
			envelope, err = readInlineSnapshotEnvelope(data, func(source io.Reader) error {
				if n.cfg.SnapshotPreflight != nil {
					storageErr = n.cfg.SnapshotPreflight(r.Context(), min(n.cfg.MaxSnapshotBytes, size/4*3))
					if storageErr != nil {
						return storageErr
					}
				}
				if storageErr = prepareSnapshotDirectory(n.cfg.Dir); storageErr != nil {
					return storageErr
				}
				staged, storageErr = os.CreateTemp(filepath.Join(n.cfg.Dir, "snapshots"), ".snapshot-")
				if storageErr != nil {
					return storageErr
				}
				var err error
				descriptor, err = copySnapshotFile(staged, io.LimitReader(source, n.cfg.MaxSnapshotBytes+1), false)
				if descriptor.Bytes > n.cfg.MaxSnapshotBytes {
					return ErrSnapshotTooLarge
				}
				return err
			})
			return err
		})
		if err != nil {
			return err
		}
		return snapshot.Unmarshal(fields)
	})
	if err != nil || !descriptor.valid() {
		code := http.StatusBadRequest
		var pathErr *os.PathError
		var limitErr *http.MaxBytesError
		switch {
		case errors.Is(err, ErrSnapshotTooLarge) || errors.As(err, &limitErr):
			code = http.StatusRequestEntityTooLarge
		case storageErr != nil || errors.As(err, &pathErr):
			code = http.StatusServiceUnavailable
		}
		http.Error(w, "invalid or unavailable inline snapshot", code)
		return
	}
	var message raftpb.Message
	if err := message.Unmarshal(metadata); err != nil || message.Type != raftpb.MsgSnap || snapshot.Metadata.Index == 0 {
		http.Error(w, "invalid inline snapshot message", http.StatusBadRequest)
		return
	}
	n.peerMu.RLock()
	_, known := n.peers[message.From]
	n.peerMu.RUnlock()
	if !known || message.To != n.cfg.ID || message.From == n.cfg.ID {
		http.Error(w, "invalid snapshot peer", http.StatusBadRequest)
		return
	}
	if err := n.available(false); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if snapshot.Metadata.Index <= n.committed.Load() || snapshot.Metadata.Index <= n.snapshotIndex.Load() {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	envelope.Version, envelope.State, envelope.File = 2, nil, &descriptor
	snapshot.Data, err = json.Marshal(envelope)
	if err != nil {
		http.Error(w, "encode snapshot metadata failed", http.StatusInternalServerError)
		return
	}
	message.Snapshot = &snapshot
	if err := n.stepReceivedSnapshot(r.Context(), message, staged, descriptor); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
