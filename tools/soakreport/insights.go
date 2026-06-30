package main

import (
	"fmt"
	"io"
	"sort"
)

type maintenanceEffect struct {
	count        int
	bytesDelta   int64
	objectsDelta int
	tailDelta    int
}

func (r *report) printCapacity(w io.Writer) {
	if len(r.usageSamples) == 0 {
		return
	}
	maxObjects := 0
	var maxBytes int64
	maxTail := 0
	for _, sample := range r.usageSamples {
		maxObjects = maxInt(maxObjects, sample.objectCount)
		maxBytes = maxInt64(maxBytes, sample.totalBytes)
		maxTail = maxInt(maxTail, sample.commitTailLength)
	}
	fmt.Fprintf(w, "capacity_curve samples=%d max_objects=%d max_bytes=%d max_commit_tail=%d\n",
		len(r.usageSamples), maxObjects, maxBytes, maxTail)
}

func (r *report) printIndexBloat(w io.Writer) {
	if len(r.usageSamples) == 0 || len(r.indexSamples) == 0 {
		return
	}
	usage := r.usageSamples[len(r.usageSamples)-1]
	index := r.indexSamples[len(r.indexSamples)-1]
	indexBytes := usage.categories["indexes_bytes"]
	entryCount := maxInt(index.indexEntries, 1)
	entityRows := maxInt(index.entityRows, 1)
	fmt.Fprintf(w, "index_bloat indexes_bytes=%d bytes_per_entry=%.2f bytes_per_entity=%.2f index_to_total=%.4f\n",
		indexBytes,
		float64(indexBytes)/float64(entryCount),
		float64(indexBytes)/float64(entityRows),
		ratio(indexBytes, usage.totalBytes))
}

func (r *report) printMaintenanceEffects(w io.Writer) {
	for _, kind := range []string{"compact_ok", "gc_ok"} {
		effect := r.maintenanceEffect(kind)
		if effect.count == 0 {
			continue
		}
		fmt.Fprintf(w, "maintenance_effect kind=%s samples=%d last_bytes_delta=%d last_objects_delta=%d last_commit_tail_delta=%d\n",
			kind, effect.count, effect.bytesDelta, effect.objectsDelta, effect.tailDelta)
	}
}

func (r *report) printOperations(w io.Writer) {
	if len(r.operations) == 0 {
		return
	}
	classified := r.classifiedErrors()
	names := make([]string, 0, len(r.operations))
	for name := range r.operations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		op := r.operations[name]
		errors, plannedErrors, shutdownErrors := effectiveOperationErrors(op, classified.plannedRestart, classified.shutdown)
		fmt.Fprintf(w, "operation name=%s count=%d errors=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f max_ms=%.3f\n",
			op.name, op.count, errors, op.p50MS, op.p95MS, op.p99MS, op.maxMS)
		if plannedErrors > 0 {
			fmt.Fprintf(w, "operation_planned_restart_errors name=%s count=%d\n", op.name, plannedErrors)
		}
		if shutdownErrors > 0 {
			fmt.Fprintf(w, "operation_shutdown_errors name=%s count=%d\n", op.name, shutdownErrors)
		}
	}
}

func (r *report) maintenanceEffect(kind string) maintenanceEffect {
	var result maintenanceEffect
	for _, event := range r.maintEvents {
		if event.kind != kind {
			continue
		}
		before, after, ok := r.usageAround(event)
		if !ok {
			continue
		}
		result.count++
		result.bytesDelta = after.totalBytes - before.totalBytes
		result.objectsDelta = after.objectCount - before.objectCount
		result.tailDelta = after.commitTailLength - before.commitTailLength
	}
	return result
}

func (r *report) usageAround(event maintenanceEvent) (usageSample, usageSample, bool) {
	var before usageSample
	var after usageSample
	for _, sample := range r.usageSamples {
		if sample.at.After(event.at) {
			after = sample
			break
		}
		before = sample
	}
	if before.at.IsZero() || after.at.IsZero() {
		return usageSample{}, usageSample{}, false
	}
	return before, after, true
}

func ratio(value int64, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(value) / float64(total)
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
