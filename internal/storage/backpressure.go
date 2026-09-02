package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var ErrBackpressure = errors.New("write backpressure")

type BackpressureConfig struct {
	ObjectLatencyThreshold time.Duration
	ObjectErrorWindow      time.Duration
	ObjectErrorThreshold   int
	CASConflictWindow      time.Duration
	CASConflictThreshold   int
	MaxCommitTail          int
	MaxObjectsPerTenant    int
	MaxBytesPerTenant      int64
	MaxEntitiesPerTenant   int
	MaxEdgesPerTenant      int
	RetryAfter             time.Duration
}

type BackpressureObserver interface {
	RecordManifestCASConflict(tenantID string)
	RecordCommitTail(tenantID string, length int)
}

type BackpressureReason struct {
	Code      string  `json:"code"`
	Current   float64 `json:"current,omitempty"`
	Threshold float64 `json:"threshold,omitempty"`
	Message   string  `json:"message"`
}

type BackpressureError struct {
	Reasons    []BackpressureReason
	RetryAfter time.Duration
}

func (e *BackpressureError) Error() string {
	if e == nil || len(e.Reasons) == 0 {
		return ErrBackpressure.Error()
	}
	return fmt.Sprintf("%s: %s", ErrBackpressure, e.Reasons[0].Code)
}

func (e *BackpressureError) Unwrap() error {
	return ErrBackpressure
}

func (e *BackpressureError) RetryAfterMS() int64 {
	if e == nil || e.RetryAfter <= 0 {
		return 0
	}
	return e.RetryAfter.Milliseconds()
}

type WritePressure struct {
	mu           sync.Mutex
	config       BackpressureConfig
	latencies    []time.Duration
	latencyEWMA  time.Duration
	objectErrors []time.Time
	conflicts    map[string][]time.Time
	maxSamples   int
	lastTailSize map[string]int
	lastUsage    map[string]tenantPressureUsage
}

type tenantPressureUsage struct {
	objectCount int
	totalBytes  int64
	observedAt  time.Time
}

func NewWritePressure(config BackpressureConfig) *WritePressure {
	config = normalizeBackpressureConfig(config)
	return &WritePressure{
		config:       config,
		conflicts:    map[string][]time.Time{},
		maxSamples:   128,
		lastTailSize: map[string]int{},
		lastUsage:    map[string]tenantPressureUsage{},
	}
}

func normalizeBackpressureConfig(config BackpressureConfig) BackpressureConfig {
	if config.RetryAfter <= 0 {
		config.RetryAfter = 2 * time.Second
	}
	if config.CASConflictWindow <= 0 {
		config.CASConflictWindow = 30 * time.Second
	}
	if config.ObjectErrorThreshold > 0 && config.ObjectErrorWindow <= 0 {
		config.ObjectErrorWindow = 30 * time.Second
	}
	if config.CASConflictThreshold < 0 {
		config.CASConflictThreshold = 0
	}
	if config.ObjectErrorThreshold < 0 {
		config.ObjectErrorThreshold = 0
	}
	return config
}

func (p *WritePressure) Config() BackpressureConfig {
	if p == nil {
		return BackpressureConfig{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.config
}

func (p *WritePressure) RecordObjectLatency(duration time.Duration) {
	if p == nil || duration <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latencies = append(p.latencies, duration)
	if p.latencyEWMA <= 0 {
		p.latencyEWMA = duration
	} else {
		p.latencyEWMA = (p.latencyEWMA*7 + duration) / 8
	}
	if len(p.latencies) > p.maxSamples {
		copy(p.latencies, p.latencies[len(p.latencies)-p.maxSamples:])
		p.latencies = p.latencies[:p.maxSamples]
	}
}

func (p *WritePressure) RecordObjectOperation(duration time.Duration, err error) {
	if p == nil {
		return
	}
	p.RecordObjectLatency(duration)
	if !isObjectStorePressureError(err) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objectErrors = append(p.objectErrors, time.Now().UTC())
	if len(p.objectErrors) > p.maxSamples {
		copy(p.objectErrors, p.objectErrors[len(p.objectErrors)-p.maxSamples:])
		p.objectErrors = p.objectErrors[:p.maxSamples]
	}
}

func (p *WritePressure) RecordManifestCASConflict(tenantID string) {
	if p == nil || tenantID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now().UTC()
	p.conflicts[tenantID] = append(p.trimConflictsLocked(tenantID, now), now)
}

func (p *WritePressure) RecordCommitTail(tenantID string, length int) {
	if p == nil || tenantID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastTailSize[tenantID] = length
}

func (p *WritePressure) RecordTenantUsage(tenantID string, objectCount int, totalBytes int64) {
	if p == nil || tenantID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastUsage[tenantID] = tenantPressureUsage{objectCount: objectCount, totalBytes: totalBytes, observedAt: time.Now().UTC()}
}

func (p *WritePressure) Reasons(tenantID string) []BackpressureReason {
	if p == nil {
		return nil
	}
	return p.ReasonsWithConfig(tenantID, p.Config())
}

func (p *WritePressure) ReasonsWithConfig(tenantID string, config BackpressureConfig) []BackpressureReason {
	if p == nil {
		return nil
	}
	config = normalizeBackpressureConfig(config)
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now().UTC()
	reasons := make([]BackpressureReason, 0, 2)
	if config.ObjectLatencyThreshold > 0 {
		p95 := percentileDuration(p.latencies, 0.95)
		current := p95
		if p.latencyEWMA > current {
			current = p.latencyEWMA
		}
		if current > config.ObjectLatencyThreshold {
			reasons = append(reasons, BackpressureReason{
				Code:      "object_store_latency_high",
				Current:   float64(current.Milliseconds()),
				Threshold: float64(config.ObjectLatencyThreshold.Milliseconds()),
				Message:   "object store latency p95 or ewma is above threshold",
			})
		}
	}
	if config.ObjectErrorThreshold > 0 {
		count := len(p.trimObjectErrorsLocked(now, config))
		if count >= config.ObjectErrorThreshold {
			reasons = append(reasons, BackpressureReason{
				Code:      "object_store_errors_high",
				Current:   float64(count),
				Threshold: float64(config.ObjectErrorThreshold),
				Message:   "object store errors are above threshold",
			})
		}
	}
	if config.CASConflictThreshold > 0 {
		count := len(p.trimConflictsLockedWithConfigLocked(tenantID, now, config))
		if count >= config.CASConflictThreshold {
			reasons = append(reasons, BackpressureReason{
				Code:      "manifest_cas_conflicts_high",
				Current:   float64(count),
				Threshold: float64(config.CASConflictThreshold),
				Message:   "manifest compare-and-swap conflicts are above threshold",
			})
		}
	}
	if usage, ok := p.lastUsage[tenantID]; ok {
		if config.MaxObjectsPerTenant > 0 && usage.objectCount >= config.MaxObjectsPerTenant {
			reasons = append(reasons, BackpressureReason{
				Code:      "tenant_object_count_high",
				Current:   float64(usage.objectCount),
				Threshold: float64(config.MaxObjectsPerTenant),
				Message:   "tenant object count is above write backpressure threshold",
			})
		}
		if config.MaxBytesPerTenant > 0 && usage.totalBytes >= config.MaxBytesPerTenant {
			reasons = append(reasons, BackpressureReason{
				Code:      "tenant_bytes_high",
				Current:   float64(usage.totalBytes),
				Threshold: float64(config.MaxBytesPerTenant),
				Message:   "tenant stored bytes are above write backpressure threshold",
			})
		}
	}
	return reasons
}

func newBackpressureError(reasons []BackpressureReason, retryAfter time.Duration) error {
	if len(reasons) == 0 {
		return nil
	}
	if retryAfter <= 0 {
		retryAfter = 2 * time.Second
	}
	return &BackpressureError{Reasons: reasons, RetryAfter: retryAfter}
}

func (p *WritePressure) trimConflictsLocked(tenantID string, now time.Time) []time.Time {
	return p.trimConflictsLockedWithConfigLocked(tenantID, now, p.config)
}

func (p *WritePressure) trimConflictsLockedWithConfigLocked(tenantID string, now time.Time, config BackpressureConfig) []time.Time {
	items := p.conflicts[tenantID]
	if len(items) == 0 {
		return nil
	}
	config = normalizeBackpressureConfig(config)
	cutoff := now.Add(-config.CASConflictWindow)
	start := 0
	for start < len(items) && items[start].Before(cutoff) {
		start++
	}
	if start > 0 {
		items = append([]time.Time(nil), items[start:]...)
		p.conflicts[tenantID] = items
	}
	return items
}

func (p *WritePressure) trimObjectErrorsLocked(now time.Time, config BackpressureConfig) []time.Time {
	if len(p.objectErrors) == 0 {
		return nil
	}
	if config.ObjectErrorWindow <= 0 {
		return p.objectErrors
	}
	cutoff := now.Add(-config.ObjectErrorWindow)
	start := 0
	for start < len(p.objectErrors) && p.objectErrors[start].Before(cutoff) {
		start++
	}
	if start > 0 {
		p.objectErrors = append([]time.Time(nil), p.objectErrors[start:]...)
	}
	return p.objectErrors
}

func isObjectStorePressureError(err error) bool {
	return errors.Is(err, ErrObjectStoreUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

func percentileDuration(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	copied := append([]time.Duration(nil), values...)
	sort.Slice(copied, func(i, j int) bool { return copied[i] < copied[j] })
	index := int(float64(len(copied)-1) * quantile)
	if index < 0 {
		index = 0
	}
	if index >= len(copied) {
		index = len(copied) - 1
	}
	return copied[index]
}
