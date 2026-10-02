package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// OperationMetrics tracks process-local work with fixed operation/event names.
// Callers must not use tenant IDs, URLs, request IDs or error messages as names.
type OperationMetrics struct {
	mu       sync.Mutex
	duration map[string]*histogram
	inflight map[string]float64
	events   map[string]float64
}

var operationBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}

func NewOperationMetrics() *OperationMetrics {
	return &OperationMetrics{duration: map[string]*histogram{}, inflight: map[string]float64{}, events: map[string]float64{}}
}

func (m *OperationMetrics) Start(operation string) func(error) {
	if m == nil {
		return func(error) {}
	}
	started := time.Now()
	m.mu.Lock()
	m.inflight[operation]++
	m.mu.Unlock()
	return func(err error) {
		status := "ok"
		if errors.Is(err, context.DeadlineExceeded) {
			status = "timeout"
		} else if errors.Is(err, context.Canceled) {
			status = "canceled"
		} else if err != nil {
			status = "error"
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.inflight[operation]--
		observeHistogram(m.duration, labelKey(operation, status), operationBuckets, time.Since(started).Seconds())
	}
}

func (m *OperationMetrics) Event(event string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events[event]++
}

func (m *OperationMetrics) WritePrometheus(w io.Writer, prefix string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	var b bytes.Buffer
	writeHistogram(&b, prefix+"_operation_seconds", "Local operation latency and completed attempts, including failures.", []string{"operation", "status"}, m.duration)
	writeGaugeValues(&b, prefix+"_operations_inflight", "Operations currently executing in this process.", []string{"operation"}, m.inflight)
	writeCounter(&b, prefix+"_events_total", "Local cache, retry and transport events.", []string{"event"}, m.events)
	m.mu.Unlock()
	w.Write(b.Bytes())
}

func WriteScalar(w io.Writer, name, help, metricType string, value float64) {
	var b bytes.Buffer
	writeScalar(&b, name, help, metricType, value)
	w.Write(b.Bytes())
}

func WriteInfo(w io.Writer, name, help string, labels, values []string) {
	var b bytes.Buffer
	writeMetricHeader(&b, name, help, "gauge")
	b.WriteString(name + formatLabels(labels, values) + " 1\n")
	w.Write(b.Bytes())
}
