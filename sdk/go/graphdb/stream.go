package graphdb

import (
	"encoding/json"
	"io"
)

type Stream struct {
	body    io.ReadCloser
	decoder *json.Decoder
	err     error
}

func newStream(body io.ReadCloser) *Stream {
	return &Stream{body: body, decoder: json.NewDecoder(body)}
}

func (s *Stream) Next(v any) bool {
	if s == nil || s.err != nil {
		return false
	}
	if err := s.decoder.Decode(v); err != nil {
		if err != io.EOF {
			s.err = err
		}
		return false
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
