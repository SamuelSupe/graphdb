package graphdb

import (
	"encoding/json"
	"io"
)

type Stream struct {
	body        io.ReadCloser
	decoder     *json.Decoder
	err         error
	requireDone bool
	done        bool
}

func newStream(body io.ReadCloser) *Stream {
	return &Stream{body: body, decoder: json.NewDecoder(body)}
}

func (s *Stream) Next(v any) bool {
	if s == nil || s.err != nil {
		return false
	}
	var raw json.RawMessage
	target := v
	if s.requireDone {
		target = &raw
	}
	if err := s.decoder.Decode(target); err != nil {
		if err != io.EOF {
			s.err = err
		} else if s.requireDone && !s.done {
			s.err = io.ErrUnexpectedEOF
		}
		return false
	}
	if s.requireDone {
		var meta struct {
			Done bool `json:"done"`
		}
		if err := json.Unmarshal(raw, &meta); err != nil {
			s.err = err
			return false
		}
		s.done = s.done || meta.Done
		if err := json.Unmarshal(raw, v); err != nil {
			s.err = err
			return false
		}
	}
	return true
}

func (s *Stream) Err() error {
	if s == nil {
		return nil
	}
	return s.err
}

func (s *Stream) Close() error {
	if s == nil || s.body == nil {
		return nil
	}
	return s.body.Close()
}
