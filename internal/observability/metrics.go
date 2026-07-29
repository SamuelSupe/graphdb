package observability

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var httpBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
var queryBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
var writeBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

type Metrics struct {
	mu sync.Mutex

	httpRequests   map[string]float64
	httpDuration   map[string]*histogram
	objectDuration map[string]*histogram

	queries              map[string]float64
	queryDuration        map[string]*histogram
	slowQueries          map[string]float64
	readAdmission        map[string]*histogram
	readerNotFresh       map[string]float64
	readerVisibleVersion map[string]float64
	readerCatchup        map[string]float64
	readerCatchupLatency map[string]*histogram
	readerCache          map[string]float64

	writeBackpressure      map[string]float64
	writeAdmission         map[string]*histogram
	manifestConflicts      map[string]float64
	commitTailLength       map[string]float64
	coordinatorCAS         map[string]float64
	coordinatorCleanup     map[string]float64
	coordinatorCleanupRuns map[string]float64
	coordinatorHeads       map[string]float64
	coordinatorStatus      map[string]float64

	suppressed        map[string]float64
	ingestSuppressed  map[string]float64
	ingestSkipped     map[string]float64
	indexHealthChecks map[string]float64
	indexHealthStatus map[string]string
	indexHealthIssues map[string]float64
}

type histogram struct {
	Buckets []float64
	Counts  []uint64
	Sum     float64
	Count   uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		httpRequests:           map[string]float64{},
		httpDuration:           map[string]*histogram{},
		objectDuration:         map[string]*histogram{},
		queries:                map[string]float64{},
		queryDuration:          map[string]*histogram{},
		slowQueries:            map[string]float64{},
		readAdmission:          map[string]*histogram{},
		readerNotFresh:         map[string]float64{},
		readerVisibleVersion:   map[string]float64{},
		readerCatchup:          map[string]float64{},
		readerCatchupLatency:   map[string]*histogram{},
		readerCache:            map[string]float64{},
		writeBackpressure:      map[string]float64{},
		writeAdmission:         map[string]*histogram{},
		manifestConflicts:      map[string]float64{},
		commitTailLength:       map[string]float64{},
		coordinatorCAS:         map[string]float64{},
		coordinatorCleanup:     map[string]float64{},
		coordinatorCleanupRuns: map[string]float64{},
		coordinatorHeads:       map[string]float64{},
		coordinatorStatus:      map[string]float64{},
		suppressed:             map[string]float64{},
		ingestSuppressed:       map[string]float64{},
		ingestSkipped:          map[string]float64{},
		indexHealthChecks:      map[string]float64{},
		indexHealthStatus:      map[string]string{},
		indexHealthIssues:      map[string]float64{},
	}
}

func (m *Metrics) RecordHTTPRequest(method string, route string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := labelKey(method, route, strconv.Itoa(status))
	m.httpRequests[key]++
	m.observe(m.httpDuration, key, httpBuckets, duration.Seconds())
}

func (m *Metrics) RecordObjectStoreOperation(operation string, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe(m.objectDuration, labelKey(operation, status), writeBuckets, duration.Seconds())
}

func (m *Metrics) RecordWriteBackpressure(tenantID string, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeBackpressure[labelKey(tenantID, reason)]++
}

func (m *Metrics) RecordWriteAdmissionQueue(tenantID string, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe(m.writeAdmission, labelKey(tenantID, status), writeBuckets, duration.Seconds())
}

func (m *Metrics) RecordManifestCASConflict(tenantID string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.manifestConflicts[tenantID]++
}

func (m *Metrics) RecordCommitTail(tenantID string, length int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitTailLength[tenantID] = float64(length)
}

func (m *Metrics) RecordCoordinatorCAS(tenantID string, status string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.coordinatorCAS[labelKey(tenantID, status)]++
}

func (m *Metrics) RecordCoordinatorHeadRevision(tenantID string, revision int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if value := float64(revision); value > m.coordinatorHeads[tenantID] {
		m.coordinatorHeads[tenantID] = value
	}
}

func (m *Metrics) RecordCoordinatorCleanup(status string, idempotencyDeleted, outboxDeleted int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.coordinatorCleanupRuns[status]++
	m.coordinatorCleanup["commit_idempotency"] += float64(idempotencyDeleted)
	m.coordinatorCleanup["legacy_manifest_outbox"] += float64(outboxDeleted)
}

func (m *Metrics) RecordCoordinatorStatus(backend string, available bool, mirrorLag, outbox, derived int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	availableValue := float64(0)
	if available {
		availableValue = 1
	}
	m.coordinatorStatus[labelKey(backend, "available")] = availableValue
	m.coordinatorStatus[labelKey(backend, "mirror_lag")] = float64(mirrorLag)
	m.coordinatorStatus[labelKey(backend, "outbox_backlog")] = float64(outbox)
	m.coordinatorStatus[labelKey(backend, "derived_backlog")] = float64(derived)
}

func (m *Metrics) RecordQuery(tenantID string, op string, status string, duration time.Duration, slow bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := labelKey(tenantID, op, status)
	m.queries[key]++
	m.observe(m.queryDuration, key, queryBuckets, duration.Seconds())
	if slow {
		m.slowQueries[labelKey(tenantID, op)]++
	}
}

func (m *Metrics) RecordReadAdmissionQueue(tenantID string, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe(m.readAdmission, labelKey(tenantID, status), writeBuckets, duration.Seconds())
}

func (m *Metrics) RecordReaderNotFresh(tenantID string, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readerNotFresh[labelKey(tenantID, reason)]++
}

func (m *Metrics) RecordReaderVisibleVersion(tenantID string, version int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readerVisibleVersion[tenantID] = float64(version)
}

func (m *Metrics) RecordReaderCatchup(tenantID string, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := labelKey(tenantID, status)
	m.readerCatchup[key]++
	m.observe(m.readerCatchupLatency, key, queryBuckets, duration.Seconds())
}

func (m *Metrics) RecordReaderCache(tenantID string, status string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readerCache[labelKey(tenantID, status)]++
}

func (m *Metrics) RecordSuppressed(tenantID string, resourceType string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suppressed[labelKey(tenantID, resourceType)] += float64(count)
}

func (m *Metrics) RecordIngestSuppressed(tenantID string, source string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingestSuppressed[labelKey(tenantID, source)] += float64(count)
}

func (m *Metrics) RecordIngestSkipped(tenantID string, source string, reason string) {
	if m == nil || reason == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ingestSkipped[labelKey(tenantID, source, reason)]++
}

func (m *Metrics) RecordIndexHealth(tenantID string, status string, issues int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.indexHealthChecks[labelKey(tenantID, status)]++
	m.indexHealthStatus[tenantID] = status
	m.indexHealthIssues[tenantID] = float64(issues)
}

func (m *Metrics) SnapshotPrometheus() []byte {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var b bytes.Buffer
	writeCounter(&b, "graphdb_http_requests_total", "HTTP requests by method, route and status.", []string{"method", "route", "status"}, m.httpRequests)
	writeHistogram(&b, "graphdb_http_request_duration_seconds", "HTTP request latency.", []string{"method", "route", "status"}, m.httpDuration)
	writeHistogram(&b, "graphdb_object_store_operation_seconds", "Object store operation latency.", []string{"operation", "status"}, m.objectDuration)
	writeCounter(&b, "graphdb_queries_total", "Queries by tenant, operation and status.", []string{"tenant", "op", "status"}, m.queries)
	writeHistogram(&b, "graphdb_query_duration_seconds", "Query latency.", []string{"tenant", "op", "status"}, m.queryDuration)
	writeCounter(&b, "graphdb_slow_queries_total", "Slow queries by tenant and operation.", []string{"tenant", "op"}, m.slowQueries)
	writeHistogram(&b, "graphdb_read_admission_queue_seconds", "Non-query read admission queue wait time.", []string{"tenant", "status"}, m.readAdmission)
	writeCounter(&b, "graphdb_reader_not_fresh_total", "Reader freshness rejections by tenant and reason.", []string{"tenant", "reason"}, m.readerNotFresh)
	writeGaugeValues(&b, "graphdb_reader_visible_version", "Latest reader-visible version by tenant observed by this process.", []string{"tenant"}, m.readerVisibleVersion)
	writeCounter(&b, "graphdb_reader_catchup_total", "Reader request-time catch-up attempts by tenant and status.", []string{"tenant", "status"}, m.readerCatchup)
	writeHistogram(&b, "graphdb_reader_catchup_seconds", "Reader request-time catch-up latency.", []string{"tenant", "status"}, m.readerCatchupLatency)
	writeCounter(&b, "graphdb_reader_cache_total", "Reader cache events by tenant and status.", []string{"tenant", "status"}, m.readerCache)
	writeCounter(&b, "graphdb_write_backpressure_total", "Rejected writes by tenant and backpressure reason.", []string{"tenant", "reason"}, m.writeBackpressure)
	writeHistogram(&b, "graphdb_write_admission_queue_seconds", "Write admission queue wait time.", []string{"tenant", "status"}, m.writeAdmission)
	writeCounter(&b, "graphdb_manifest_cas_conflicts_total", "Manifest compare-and-swap conflicts by tenant.", []string{"tenant"}, m.manifestConflicts)
	writeCounter(&b, "graphdb_coordinator_cas_total", "PostgreSQL head CAS results by tenant and status.", []string{"tenant", "status"}, m.coordinatorCAS)
	writeCounter(&b, "graphdb_coordinator_cleanup_runs_total", "PostgreSQL coordination cleanup runs by status.", []string{"status"}, m.coordinatorCleanupRuns)
	writeCounter(&b, "graphdb_coordinator_cleanup_deleted_total", "Expired PostgreSQL coordination rows deleted by table.", []string{"table"}, m.coordinatorCleanup)
	writeGaugeValues(&b, "graphdb_coordinator_head_revision", "Latest PostgreSQL head revision observed by this process.", []string{"tenant"}, m.coordinatorHeads)
	writeGaugeValues(&b, "graphdb_coordinator_status", "Coordinator availability and queue gauges by backend and metric.", []string{"backend", "metric"}, m.coordinatorStatus)
	writeGaugeValues(&b, "graphdb_commit_tail_length", "Latest commit tail length by tenant.", []string{"tenant"}, m.commitTailLength)
	writeCounter(&b, "graphdb_write_suppressed_conflicts_total", "Suppressed write conflicts by tenant and resource type.", []string{"tenant", "resource_type"}, m.suppressed)
	writeCounter(&b, "graphdb_ingest_suppressed_conflicts_total", "Suppressed ingest conflicts by tenant and source.", []string{"tenant", "source"}, m.ingestSuppressed)
	writeCounter(&b, "graphdb_ingest_skipped_total", "Ingestion batches skipped by tenant, source and reason.", []string{"tenant", "source", "reason"}, m.ingestSkipped)
	writeCounter(&b, "graphdb_index_health_checks_total", "Index health checks by tenant and status.", []string{"tenant", "status"}, m.indexHealthChecks)
	writeGaugeStrings(&b, "graphdb_index_health_status", "Latest index health status by tenant.", m.indexHealthStatus)
	writeGaugeValues(&b, "graphdb_index_health_issues", "Latest index health issue count by tenant.", []string{"tenant"}, m.indexHealthIssues)
	return b.Bytes()
}

func (m *Metrics) observe(values map[string]*histogram, key string, buckets []float64, value float64) {
	h := values[key]
	if h == nil {
		h = &histogram{Buckets: append([]float64(nil), buckets...), Counts: make([]uint64, len(buckets)+1)}
		values[key] = h
	}
	for i, bucket := range h.Buckets {
		if value <= bucket {
			h.Counts[i]++
		}
	}
	h.Counts[len(h.Counts)-1]++
	h.Sum += value
	h.Count++
}

func writeCounter(b *bytes.Buffer, name string, help string, labels []string, values map[string]float64) {
	writeMetricHeader(b, name, help, "counter")
	for _, key := range sortedKeys(values) {
		fmt.Fprintf(b, "%s%s %g\n", name, formatLabels(labels, splitKey(key)), values[key])
	}
}

func writeGaugeValues(b *bytes.Buffer, name string, help string, labels []string, values map[string]float64) {
	writeMetricHeader(b, name, help, "gauge")
	for _, key := range sortedKeys(values) {
		fmt.Fprintf(b, "%s%s %g\n", name, formatLabels(labels, splitKey(key)), values[key])
	}
}

func writeGaugeStrings(b *bytes.Buffer, name string, help string, values map[string]string) {
	writeMetricHeader(b, name, help, "gauge")
	statuses := []string{"ready", "missing", "stale", "error", "unknown"}
	tenants := sortedKeysString(values)
	for _, tenant := range tenants {
		current := values[tenant]
		if current == "" {
			current = "unknown"
		}
		seen := map[string]struct{}{}
		for _, status := range append(statuses, current) {
			if _, ok := seen[status]; ok {
				continue
			}
			seen[status] = struct{}{}
			value := 0
			if status == current {
				value = 1
			}
			fmt.Fprintf(b, "%s%s %d\n", name, formatLabels([]string{"tenant", "status"}, []string{tenant, status}), value)
		}
	}
}

func writeHistogram(b *bytes.Buffer, name string, help string, labels []string, values map[string]*histogram) {
	writeMetricHeader(b, name, help, "histogram")
	for _, key := range sortedKeysHist(values) {
		h := values[key]
		base := splitKey(key)
		for i, bucket := range h.Buckets {
			allLabels := append(append([]string(nil), labels...), "le")
			allValues := append(append([]string(nil), base...), strconv.FormatFloat(bucket, 'f', -1, 64))
			fmt.Fprintf(b, "%s_bucket%s %d\n", name, formatLabels(allLabels, allValues), h.Counts[i])
		}
		allLabels := append(append([]string(nil), labels...), "le")
		allValues := append(append([]string(nil), base...), "+Inf")
		fmt.Fprintf(b, "%s_bucket%s %d\n", name, formatLabels(allLabels, allValues), h.Counts[len(h.Counts)-1])
		fmt.Fprintf(b, "%s_sum%s %g\n", name, formatLabels(labels, base), h.Sum)
		fmt.Fprintf(b, "%s_count%s %d\n", name, formatLabels(labels, base), h.Count)
	}
}

func writeMetricHeader(b *bytes.Buffer, name string, help string, metricType string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func formatLabels(names []string, values []string) string {
	if len(names) == 0 {
		return ""
	}
	parts := make([]string, 0, len(names))
	for i, name := range names {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		parts = append(parts, name+"=\""+escapeLabel(value)+"\"")
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

func labelKey(values ...string) string {
	return strings.Join(values, "\xff")
}

func splitKey(key string) []string {
	return strings.Split(key, "\xff")
}

func sortedKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysHist(values map[string]*histogram) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysString(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
