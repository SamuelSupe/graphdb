package main

import (
	"strings"
	"time"
)

type report struct {
	cfg config

	firstAt time.Time
	lastAt  time.Time
	events  int

	usageSamples []usageSample
	indexSamples []indexSample
	health       healthStats
	reader       readerStats
	maintenance  map[string]int
	maintEvents  []maintenanceEvent
	operations   map[string]operationMetric
	errorEvents  []errorEvent
	soakDoneAt   time.Time
}

type usageSample struct {
	at               time.Time
	manifestVersion  int64
	snapshotVersion  int64
	commitTailLength int
	objectCount      int
	totalBytes       int64
	categories       map[string]int64
}

type indexSample struct {
	at           time.Time
	version      int64
	indexEntries int
	edgeRows     int
	entityRows   int
	edgeShards   int
	entityPages  int
}

type healthStats struct {
	total      int
	unhealthy  int
	stale      int
	lastStatus string
}

type readerStats struct {
	total         int
	unready       int
	maxVersionLag int64
	maxLagMS      int64
}

type maintenanceEvent struct {
	kind string
	at   time.Time
}

type errorEvent struct {
	kind    string
	at      time.Time
	message string
	warmup  bool
}

type errorClassification struct {
	active         map[string]int
	warmup         map[string]int
	plannedRestart map[string]int
	shutdown       map[string]int
}

type operationMetric struct {
	name   string
	count  int
	errors int
	p50MS  float64
	p95MS  float64
	p99MS  float64
	maxMS  float64
}

func newReport(cfg config) *report {
	return &report{
		cfg:         cfg,
		maintenance: map[string]int{},
		operations:  map[string]operationMetric{},
	}
}

func (r *report) add(e event) {
	r.events++
	if r.firstAt.IsZero() || e.At.Before(r.firstAt) {
		r.firstAt = e.At
	}
	if e.At.After(r.lastAt) {
		r.lastAt = e.At
	}
	switch e.Kind {
	case "usage_sample":
		r.usageSamples = append(r.usageSamples, usageFromEvent(e))
	case "index_catalog_sample":
		r.indexSamples = append(r.indexSamples, indexFromEvent(e))
	case "index_health_sample":
		if r.afterWarmup(e) {
			status, _ := e.Raw["status"].(string)
			r.health.total++
			r.health.lastStatus = status
			switch status {
			case "ok", "ready", "healthy":
			case "stale":
				r.health.stale++
			default:
				r.health.unhealthy++
			}
		}
	case "reader_fleet":
		if r.afterWarmup(e) {
			r.reader.total++
			if !boolValue(e.Raw["ready"]) {
				r.reader.unready++
			}
		}
	case "reader_freshness":
		if r.afterWarmup(e) {
			r.reader.maxVersionLag = maxInt64(r.reader.maxVersionLag, int64Value(e.Raw["version_lag"]))
			r.reader.maxLagMS = maxInt64(r.reader.maxLagMS, int64Value(e.Raw["lag_ms"]))
		}
	case "operation_metric":
		if r.afterWarmup(e) {
			operation := operationFromEvent(e)
			if operation.name != "" {
				r.operations[operation.name] = operation
			}
		}
	case "compact_ok", "gc_ok", "index_rebuild_ok", "reader_restart_ok":
		r.maintenance[e.Kind]++
		r.maintEvents = append(r.maintEvents, maintenanceEvent{kind: e.Kind, at: e.At})
	case "soak_done":
		r.soakDoneAt = e.At
	default:
		if strings.HasSuffix(e.Kind, "_error") || e.Kind == "query_error" || e.Kind == "write_error" {
			message, _ := e.Raw["error"].(string)
			r.errorEvents = append(r.errorEvents, errorEvent{
				kind:    e.Kind,
				at:      e.At,
				message: message,
				warmup:  !r.afterWarmup(e),
			})
		}
	}
}

func (r *report) classifiedErrors() errorClassification {
	result := errorClassification{
		active:         map[string]int{},
		warmup:         map[string]int{},
		plannedRestart: map[string]int{},
		shutdown:       map[string]int{},
	}
	for _, item := range r.errorEvents {
		switch {
		case item.warmup:
			result.warmup[item.kind]++
		case r.inPlannedRestartGrace(item):
			result.plannedRestart[item.kind]++
		case r.inShutdownGrace(item):
			result.shutdown[item.kind]++
		default:
			result.active[item.kind]++
		}
	}
	return result
}

func effectiveOperationErrors(op operationMetric, plannedRestart map[string]int, shutdown map[string]int) (int, int, int) {
	planned := 0
	shutdownErrors := 0
	switch op.name {
	case "reader-fleet":
		planned = plannedRestart["reader_fleet_error"]
		shutdownErrors = shutdown["reader_fleet_error"]
	case "reader-freshness":
		planned = plannedRestart["reader_freshness_error"]
		shutdownErrors = shutdown["reader_freshness_error"]
	case "index-catalog":
		shutdownErrors = shutdown["index_catalog_sample_error"]
	case "index-health":
		shutdownErrors = shutdown["index_health_sample_error"]
	}
	return maxInt(0, op.errors-planned-shutdownErrors), planned, shutdownErrors
}

func (r *report) inPlannedRestartGrace(item errorEvent) bool {
	if r.cfg.readerRestartGrace <= 0 {
		return false
	}
	switch item.kind {
	case "reader_fleet_error", "reader_freshness_error":
	default:
		return false
	}
	for _, event := range r.maintEvents {
		if event.kind != "reader_restart_ok" {
			continue
		}
		delta := item.at.Sub(event.at)
		if delta < 0 {
			delta = -delta
		}
		if delta <= r.cfg.readerRestartGrace {
			return true
		}
	}
	return false
}

func (r *report) inShutdownGrace(item errorEvent) bool {
	if r.cfg.shutdownGrace <= 0 || r.soakDoneAt.IsZero() {
		return false
	}
	switch item.kind {
	case "index_catalog_sample_error", "index_health_sample_error", "reader_freshness_error", "reader_fleet_error":
	default:
		return false
	}
	if !strings.Contains(strings.ToLower(item.message), "context deadline exceeded") {
		return false
	}
	delta := item.at.Sub(r.soakDoneAt)
	if delta < 0 {
		delta = -delta
	}
	return delta <= r.cfg.shutdownGrace
}

func operationFromEvent(e event) operationMetric {
	name, _ := e.Raw["name"].(string)
	return operationMetric{
		name:   name,
		count:  intValue(e.Raw["count"]),
		errors: intValue(e.Raw["errors"]),
		p50MS:  floatValue(e.Raw["p50_ms"]),
		p95MS:  floatValue(e.Raw["p95_ms"]),
		p99MS:  floatValue(e.Raw["p99_ms"]),
		maxMS:  floatValue(e.Raw["max_ms"]),
	}
}

func (r *report) afterWarmup(e event) bool {
	if r.firstAt.IsZero() || e.At.IsZero() {
		return true
	}
	return !e.At.Before(r.firstAt.Add(r.cfg.warmup))
}

func usageFromEvent(e event) usageSample {
	return usageSample{
		at:               e.At,
		manifestVersion:  int64Value(e.Raw["manifest_version"]),
		snapshotVersion:  int64Value(e.Raw["snapshot_version"]),
		commitTailLength: intValue(e.Raw["commit_tail_length"]),
		objectCount:      intValue(e.Raw["object_count"]),
		totalBytes:       int64Value(e.Raw["total_bytes"]),
		categories:       categoryMap(e.Raw["categories"]),
	}
}

func indexFromEvent(e event) indexSample {
	return indexSample{
		at:           e.At,
		version:      int64Value(e.Raw["version"]),
		indexEntries: intValue(e.Raw["index_entries"]),
		edgeRows:     intValue(e.Raw["edge_rows"]),
		entityRows:   intValue(e.Raw["entity_rows"]),
		edgeShards:   intValue(e.Raw["edge_shards"]),
		entityPages:  intValue(e.Raw["entity_pages"]),
	}
}

func categoryMap(value any) map[string]int64 {
	result := map[string]int64{}
	raw, _ := value.(map[string]any)
	for key, value := range raw {
		result[key] = int64Value(value)
	}
	return result
}

func (r *report) duration() time.Duration {
	if r.firstAt.IsZero() || r.lastAt.IsZero() || r.lastAt.Before(r.firstAt) {
		return 0
	}
	return r.lastAt.Sub(r.firstAt)
}

func (r *report) usageRange() (usageSample, usageSample, bool) {
	if len(r.usageSamples) == 0 {
		return usageSample{}, usageSample{}, false
	}
	return r.usageSamples[0], r.usageSamples[len(r.usageSamples)-1], true
}

func (r *report) indexRange() (indexSample, indexSample, bool) {
	if len(r.indexSamples) == 0 {
		return indexSample{}, indexSample{}, false
	}
	return r.indexSamples[0], r.indexSamples[len(r.indexSamples)-1], true
}

func (r *report) lastUsage() (usageSample, bool) {
	if len(r.usageSamples) == 0 {
		return usageSample{}, false
	}
	return r.usageSamples[len(r.usageSamples)-1], true
}

func (r *report) readerUnreadyRatio() float64 {
	if r.reader.total == 0 {
		return 0
	}
	return float64(r.reader.unready) / float64(r.reader.total)
}

func perHour(delta float64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return delta / duration.Hours()
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
