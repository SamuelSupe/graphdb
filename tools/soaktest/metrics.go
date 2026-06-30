package main

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

type registry struct {
	mu   sync.Mutex
	data map[string]*metric
}

type metric struct {
	count    int
	errors   int
	statuses map[int]int
	latency  []time.Duration
}

type metricSnapshot struct {
	name     string
	count    int
	errors   int
	statuses map[int]int
	latency  []time.Duration
}

func newRegistry() *registry {
	return &registry{data: map[string]*metric{}}
}

func (r *registry) add(name string, latency time.Duration, status int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.data[name]
	if m == nil {
		m = &metric{statuses: map[int]int{}}
		r.data[name] = m
	}
	m.count++
	if err != nil {
		m.errors++
	}
	if status != 0 {
		m.statuses[status]++
	}
	m.latency = append(m.latency, latency)
}

func (r *registry) hasErrors() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.data {
		if m.errors > 0 {
			return true
		}
	}
	return false
}

func (r *registry) snapshots() []metricSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.data))
	for name := range r.data {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]metricSnapshot, 0, len(names))
	for _, name := range names {
		m := r.data[name]
		statuses := make(map[int]int, len(m.statuses))
		for status, count := range m.statuses {
			statuses[status] = count
		}
		latency := append([]time.Duration(nil), m.latency...)
		sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
		result = append(result, metricSnapshot{name: name, count: m.count, errors: m.errors, statuses: statuses, latency: latency})
	}
	return result
}

func (r *registry) emit(events *eventWriter) {
	for _, snapshot := range r.snapshots() {
		if snapshot.count == 0 {
			continue
		}
		events.emit("operation_metric", map[string]any{
			"name":     snapshot.name,
			"count":    snapshot.count,
			"errors":   snapshot.errors,
			"p50_ms":   durationMillis(pct(snapshot.latency, 50)),
			"p95_ms":   durationMillis(pct(snapshot.latency, 95)),
			"p99_ms":   durationMillis(pct(snapshot.latency, 99)),
			"max_ms":   durationMillis(pct(snapshot.latency, 100)),
			"statuses": snapshot.statuses,
		})
	}
}

func (r *registry) print(w io.Writer) {
	for _, m := range r.snapshots() {
		fmt.Fprintf(w, "%-22s count=%-6d errors=%-5d p50=%-8s p95=%-8s p99=%-8s max=%-8s statuses=%v\n",
			m.name, m.count, m.errors, pct(m.latency, 50), pct(m.latency, 95), pct(m.latency, 99), pct(m.latency, 100), m.statuses)
	}
}

func pct(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	if percentile >= 100 {
		return values[len(values)-1].Round(time.Millisecond)
	}
	index := (len(values)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1].Round(time.Millisecond)
}

func durationMillis(value time.Duration) float64 {
	return float64(value.Microseconds()) / 1000
}
