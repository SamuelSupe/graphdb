package main

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

type eventWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func newEventWriter(w io.Writer) *eventWriter {
	return &eventWriter{enc: json.NewEncoder(w)}
}

func (w *eventWriter) emit(kind string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"] = kind
	fields["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.enc.Encode(fields)
}
