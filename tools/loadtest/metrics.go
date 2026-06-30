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

func (r *registry) print(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.data))
	for name := range r.data {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		m := r.data[name]
		sort.Slice(m.latency, func(i, j int) bool { return m.latency[i] < m.latency[j] })
		fmt.Fprintf(w, "%-16s count=%-5d errors=%-4d p50=%-8s p95=%-8s p99=%-8s max=%-8s statuses=%v\n",
			name, m.count, m.errors, pct(m.latency, 50), pct(m.latency, 95), pct(m.latency, 99), pct(m.latency, 100), m.statuses)
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
