// Package admission samples storage pressure and computes write-admission reasons.
package admission

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

const (
	pressureMaxSamples        = 128
	pressureMaxTrackedTenants = 4096
)

type conflictHistory struct {
	times   []time.Time
	expires time.Time
}

type WritePressure struct {
	mu             sync.Mutex
	clock          func() time.Time
	config         BackpressureConfig
	latencyBuf     [pressureMaxSamples]time.Duration
	latencyTimeBuf [pressureMaxSamples]time.Time
	latencyHead    int
	latencyCount   int
	latencyEWMA    time.Duration
	errorBuf       [pressureMaxSamples]time.Time
	errorHead      int
	errorCount     int
	conflicts      map[string]conflictHistory
	lastTailSize   map[string]int
	lastUsage      map[string]tenantPressureUsage
}

type tenantPressureUsage struct {
	objectCount int
	totalBytes  int64
	observedAt  time.Time
}

func NewWritePressure(config BackpressureConfig) *WritePressure {
	config = NormalizeConfig(config)
	return &WritePressure{
		config:       config,
		conflicts:    map[string]conflictHistory{},
		lastTailSize: map[string]int{},
		lastUsage:    map[string]tenantPressureUsage{},
	}
}

func NormalizeConfig(config BackpressureConfig) BackpressureConfig {
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
	p.recordLatencyLocked(p.now(), duration)
}

func (p *WritePressure) recordLatencyLocked(now time.Time, duration time.Duration) {
	p.trimLatenciesLocked(now, p.config)
	if p.latencyCount < pressureMaxSamples {
		idx := (p.latencyHead + p.latencyCount) % pressureMaxSamples
		p.latencyBuf[idx] = duration
		p.latencyTimeBuf[idx] = now
		p.latencyCount++
	} else {
		p.latencyBuf[p.latencyHead] = duration
		p.latencyTimeBuf[p.latencyHead] = now
		p.latencyHead = (p.latencyHead + 1) % pressureMaxSamples
	}
	if p.latencyEWMA <= 0 {
		p.latencyEWMA = duration
	} else {
		p.latencyEWMA = (p.latencyEWMA*7 + duration) / 8
	}
}

func (p *WritePressure) RecordObjectOperation(duration time.Duration, err error) {
	if p == nil {
		return
	}
	isErr := IsObjectStorePressureError(err)
	if duration <= 0 && !isErr {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if duration > 0 {
		p.recordLatencyLocked(now, duration)
	}
	if !isErr {
		return
	}
	if p.errorCount < pressureMaxSamples {
		idx := (p.errorHead + p.errorCount) % pressureMaxSamples
		p.errorBuf[idx] = now
		p.errorCount++
	} else {
		p.errorBuf[p.errorHead] = now
		p.errorHead = (p.errorHead + 1) % pressureMaxSamples
	}
}

func (p *WritePressure) RecordManifestCASConflict(tenantID string) {
	if p != nil {
		p.RecordManifestCASConflictWithConfig(tenantID, p.Config())
	}
}

func (p *WritePressure) RecordManifestCASConflictWithConfig(tenantID string, config BackpressureConfig) {
	if p == nil || tenantID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if _, exists := p.conflicts[tenantID]; !exists && len(p.conflicts) >= pressureMaxTrackedTenants {
		for id, history := range p.conflicts {
			if now.After(history.expires) {
				delete(p.conflicts, id)
			}
		}
		if len(p.conflicts) >= pressureMaxTrackedTenants {
			var oldest string
			for id, history := range p.conflicts {
				if oldest == "" || history.expires.Before(p.conflicts[oldest].expires) {
					oldest = id
				}
			}
			delete(p.conflicts, oldest)
		}
	}
	cfg := NormalizeConfig(config)
	window := cfg.CASConflictWindow
	times := p.conflictsInWindow(tenantID, now, window, true)
	maxConflictSamples := max(pressureMaxSamples, cfg.CASConflictThreshold)
	if maxConflictSamples <= 0 {
		maxConflictSamples = pressureMaxSamples
	}
	times = append(times, now)
	if len(times) > maxConflictSamples {
		trimmed := make([]time.Time, maxConflictSamples)
		copy(trimmed, times[len(times)-maxConflictSamples:])
		times = trimmed
	}
	p.conflicts[tenantID] = conflictHistory{times: times, expires: now.Add(window)}
}

func (p *WritePressure) RecordCommitTail(tenantID string, length int) {
	if p == nil || tenantID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.lastTailSize[tenantID]; !exists && len(p.lastTailSize) >= pressureMaxTrackedTenants {
		clear(p.lastTailSize)
	}
	p.lastTailSize[tenantID] = length
}

func (p *WritePressure) RecordTenantUsage(tenantID string, objectCount int, totalBytes int64) {
	if p == nil || tenantID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if _, exists := p.lastUsage[tenantID]; !exists && len(p.lastUsage) >= pressureMaxTrackedTenants {
		for id, usage := range p.lastUsage {
			if now.Sub(usage.observedAt) >= 10*time.Minute {
				delete(p.lastUsage, id)
			}
		}
		if len(p.lastUsage) >= pressureMaxTrackedTenants {
			clear(p.lastUsage)
		}
	}
	p.lastUsage[tenantID] = tenantPressureUsage{objectCount: objectCount, totalBytes: totalBytes, observedAt: now}
}

func (p *WritePressure) Reasons(tenantID string) []BackpressureReason {
	if p == nil {
		return nil
	}
	return p.reasonsWithConfig(tenantID, BackpressureConfig{}, true)
}

func (p *WritePressure) ReasonsWithConfig(tenantID string, config BackpressureConfig) []BackpressureReason {
	if p == nil {
		return nil
	}
	return p.reasonsWithConfig(tenantID, config, false)
}

func (p *WritePressure) reasonsWithConfig(tenantID string, config BackpressureConfig, useOwnConfig bool) []BackpressureReason {
	var latencyScratch [pressureMaxSamples]time.Duration
	latencyCount := 0
	var latencyEWMA time.Duration
	var errorCount int
	var conflictCount int
	var usage tenantPressureUsage
	var hasUsage bool

	p.mu.Lock()
	if useOwnConfig {
		config = p.config
	}
	config = NormalizeConfig(config)
	now := p.now()
	trimConfig := p.config
	if !useOwnConfig && config.ObjectErrorWindow > trimConfig.ObjectErrorWindow {
		trimConfig.ObjectErrorWindow = config.ObjectErrorWindow
	}
	p.trimLatenciesLocked(now, trimConfig)
	cutoff := now.Add(-config.ObjectErrorWindow)
	if config.ObjectLatencyThreshold > 0 {
		latencyCutoff := cutoff
		if config.ObjectErrorWindow <= 0 {
			latencyCutoff = now.Add(-30 * time.Second)
		}
		for i := 0; i < p.latencyCount; i++ {
			index := (p.latencyHead + i) % pressureMaxSamples
			if p.latencyTimeBuf[index].After(latencyCutoff) {
				latencyScratch[latencyCount] = p.latencyBuf[index]
				latencyCount++
			}
		}
		if latencyCount == p.latencyCount {
			latencyEWMA = p.latencyEWMA
		} else {
			for _, duration := range latencyScratch[:latencyCount] {
				if latencyEWMA == 0 {
					latencyEWMA = duration
				} else {
					latencyEWMA = (latencyEWMA*7 + duration) / 8
				}
			}
		}
	}
	if config.ObjectErrorThreshold > 0 {
		p.trimObjectErrorsLocked(now, trimConfig)
		errorCount = p.errorCount
		for i := 0; i < p.errorCount && p.errorBuf[(p.errorHead+i)%pressureMaxSamples].Before(cutoff); i++ {
			errorCount--
		}
	}
	if config.CASConflictThreshold > 0 {
		conflictCount = len(p.conflictsInWindow(tenantID, now, config.CASConflictWindow, false))
	}
	usage, hasUsage = p.lastUsage[tenantID]
	p.mu.Unlock()

	reasons := make([]BackpressureReason, 0, 2)
	if config.ObjectLatencyThreshold > 0 {
		p95 := percentileDurationInPlace(latencyScratch[:latencyCount], 0.95)
		current := p95
		if latencyEWMA > current {
			current = latencyEWMA
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
	if config.ObjectErrorThreshold > 0 && errorCount >= config.ObjectErrorThreshold {
		reasons = append(reasons, BackpressureReason{
			Code:      "object_store_errors_high",
			Current:   float64(errorCount),
			Threshold: float64(config.ObjectErrorThreshold),
			Message:   "object store errors are above threshold",
		})
	}
	if config.CASConflictThreshold > 0 && conflictCount >= config.CASConflictThreshold {
		reasons = append(reasons, BackpressureReason{
			Code:      "manifest_cas_conflicts_high",
			Current:   float64(conflictCount),
			Threshold: float64(config.CASConflictThreshold),
			Message:   "manifest compare-and-swap conflicts are above threshold",
		})
	}
	if hasUsage {
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

func NewError(reasons []BackpressureReason, retryAfter time.Duration) error {
	if len(reasons) == 0 {
		return nil
	}
	if retryAfter <= 0 {
		retryAfter = 2 * time.Second
	}
	return &BackpressureError{Reasons: reasons, RetryAfter: retryAfter}
}

func (p *WritePressure) conflictsInWindow(tenantID string, now time.Time, window time.Duration, compact bool) []time.Time {
	history, ok := p.conflicts[tenantID]
	if !ok {
		return nil
	}
	if now.After(history.expires) {
		delete(p.conflicts, tenantID)
		return nil
	}
	items := history.times
	cutoff := now.Add(-window)
	start := sort.Search(len(items), func(i int) bool {
		return !items[i].Before(cutoff)
	})
	if start == len(items) {
		if compact {
			delete(p.conflicts, tenantID)
		}
		return nil
	}
	if start > 0 {
		if compact {
			trimmed := make([]time.Time, len(items)-start)
			copy(trimmed, items[start:])
			history.times = trimmed
			p.conflicts[tenantID] = history
			return trimmed
		}
		return items[start:]
	}
	return items
}

func (p *WritePressure) trimObjectErrorsLocked(now time.Time, config BackpressureConfig) int {
	if p.errorCount == 0 {
		return 0
	}
	if config.ObjectErrorWindow <= 0 {
		return p.errorCount
	}
	cutoff := now.Add(-config.ObjectErrorWindow)
	for p.errorCount > 0 && p.errorBuf[p.errorHead].Before(cutoff) {
		p.errorHead = (p.errorHead + 1) % pressureMaxSamples
		p.errorCount--
	}
	return p.errorCount
}

func IsObjectStorePressureError(err error) bool {
	return errors.Is(err, ErrObjectStoreUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

func percentileDurationInPlace(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := int(float64(len(values)-1) * quantile)
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func (p *WritePressure) now() time.Time {
	if p.clock != nil {
		return p.clock()
	}
	return time.Now()
}

func (p *WritePressure) trimLatenciesLocked(now time.Time, config BackpressureConfig) {
	if p.latencyCount == 0 {
		return
	}
	window := config.ObjectErrorWindow
	if window <= 0 {
		window = 30 * time.Second
	}
	cutoff := now.Add(-window)
	trimmed := false
	for p.latencyCount > 0 && !p.latencyTimeBuf[p.latencyHead].After(cutoff) {
		p.latencyHead = (p.latencyHead + 1) % pressureMaxSamples
		p.latencyCount--
		trimmed = true
	}
	if !trimmed {
		return
	}
	p.latencyEWMA = 0
	for i := 0; i < p.latencyCount; i++ {
		duration := p.latencyBuf[(p.latencyHead+i)%pressureMaxSamples]
		if p.latencyEWMA == 0 {
			p.latencyEWMA = duration
		} else {
			p.latencyEWMA = (p.latencyEWMA*7 + duration) / 8
		}
	}
}

func (p *WritePressure) RecentTenantBytes(tenant string) (int64, bool) {
	if p == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	usage, ok := p.lastUsage[tenant]
	return usage.totalBytes, ok && p.now().Sub(usage.observedAt) < time.Second
}
