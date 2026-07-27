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

type metricReport struct {
	Name     string      `json:"name"`
	Count    int         `json:"count"`
	Errors   int         `json:"errors"`
	Statuses map[int]int `json:"statuses,omitempty"`
	P50MS    int64       `json:"p50_ms"`
	P95MS    int64       `json:"p95_ms"`
	P99MS    int64       `json:"p99_ms"`
	MaxMS    int64       `json:"max_ms"`
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
	for _, report := range r.snapshot() {
		fmt.Fprintf(w, "%-16s count=%-5d errors=%-4d p50=%-8s p95=%-8s p99=%-8s max=%-8s statuses=%v\n",
			report.Name, report.Count, report.Errors,
			time.Duration(report.P50MS)*time.Millisecond,
			time.Duration(report.P95MS)*time.Millisecond,
			time.Duration(report.P99MS)*time.Millisecond,
			time.Duration(report.MaxMS)*time.Millisecond,
			report.Statuses)
	}
}

func (r *registry) snapshot() []metricReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.data))
	for name := range r.data {
		names = append(names, name)
	}
	sort.Strings(names)
	reports := make([]metricReport, 0, len(names))
	for _, name := range names {
		m := r.data[name]
		latency := append([]time.Duration(nil), m.latency...)
		sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
		statuses := make(map[int]int, len(m.statuses))
		for status, count := range m.statuses {
			statuses[status] = count
		}
		reports = append(reports, metricReport{
			Name:     name,
			Count:    m.count,
			Errors:   m.errors,
			Statuses: statuses,
			P50MS:    pct(latency, 50).Milliseconds(),
			P95MS:    pct(latency, 95).Milliseconds(),
			P99MS:    pct(latency, 99).Milliseconds(),
			MaxMS:    pct(latency, 100).Milliseconds(),
		})
	}
	return reports
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
