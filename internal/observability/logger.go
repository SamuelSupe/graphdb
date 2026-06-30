package observability

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

type Logger struct {
	mu  sync.Mutex
	out io.Writer
}

func NewLogger(out io.Writer) *Logger {
	return &Logger{out: out}
}

func (l *Logger) Info(event string, fields map[string]any) {
	l.write("info", event, fields)
}

func (l *Logger) Error(event string, fields map[string]any) {
	l.write("error", event, fields)
}

func (l *Logger) write(level string, event string, fields map[string]any) {
	if l == nil || l.out == nil {
		return
	}
	record := map[string]any{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": level,
		"event": event,
	}
	for key, value := range fields {
		record[key] = value
	}
	data, err := json.Marshal(record)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(append(data, '\n'))
}
