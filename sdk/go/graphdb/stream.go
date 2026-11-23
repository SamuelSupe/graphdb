package graphdb

import (
	"encoding/json"
	"io"
	"net/http"
)

type Stream struct {
	body    io.ReadCloser
	decoder *json.Decoder
	err     error
	done    bool
}

func newStream(body io.ReadCloser) *Stream {
	return &Stream{body: body, decoder: json.NewDecoder(body)}
}

func (s *Stream) Next(v any) bool {
	if s == nil || s.err != nil {
		return false
	}
	var raw json.RawMessage
	if err := s.decoder.Decode(&raw); err != nil {
		if err != io.EOF {
			s.err = err
		} else if !s.done {
			s.err = io.ErrUnexpectedEOF
		}
		return false
	}
	var meta struct {
		Done  bool   `json:"done"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		s.err = err
		return false
	}
	if err := json.Unmarshal(raw, v); err != nil {
		s.err = err
		return false
	}
	s.done = s.done || meta.Done
	if meta.Done && meta.Error != "" {
		s.err = apiErrorFromBody(http.StatusOK, raw)
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
