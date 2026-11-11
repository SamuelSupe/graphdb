package graphdb

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
)

type Stream struct {
	body           io.ReadCloser
	decoder        *json.Decoder
	err            error
	requireDone    bool
	done           bool
	requireSuccess bool
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
			Done    bool            `json:"done"`
			Success bool            `json:"success"`
			Error   json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(raw, &meta); err != nil {
			s.err = err
			return false
		}
		s.done = s.done || meta.Done
		if s.requireSuccess && meta.Done && !meta.Success {
			e := decodeErrorEnvelope(meta.Error)
			s.err = &APIError{StatusCode: 200, Code: e.Code, Message: e.Message, Retryable: e.Retryable, Detail: e.Detail, Body: raw}
			return false
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		if s.requireSuccess {
			d.UseNumber()
			// Cypher frames have different shapes. Reusing a map or struct
			// must not carry row fields into the terminal frame.
			if dst := reflect.ValueOf(v); dst.Kind() == reflect.Pointer && !dst.IsNil() {
				dst.Elem().SetZero()
			}
		}
		if err := d.Decode(v); err != nil {
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
