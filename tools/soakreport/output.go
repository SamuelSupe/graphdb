package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

func (r *report) print(w io.Writer) {
	classified := r.classifiedErrors()
	fmt.Fprintf(w, "events=%d duration=%s\n", r.events, r.duration().Round(time.Second))
	if first, last, ok := r.usageRange(); ok {
		fmt.Fprintf(w, "usage first_version=%d last_version=%d objects=%d->%d bytes=%d->%d commit_tail=%d\n",
			first.manifestVersion, last.manifestVersion, first.objectCount, last.objectCount, first.totalBytes, last.totalBytes, last.commitTailLength)
		fmt.Fprintf(w, "usage_growth bytes_per_hour=%.2f objects_per_hour=%.2f\n",
			perHour(float64(last.totalBytes-first.totalBytes), r.duration()), perHour(float64(last.objectCount-first.objectCount), r.duration()))
		printCategoryDelta(w, first, last)
	}
	r.printCapacity(w)
	if first, last, ok := r.indexRange(); ok {
		fmt.Fprintf(w, "index version=%d->%d entries=%d->%d edge_rows=%d->%d entity_rows=%d->%d shards=%d pages=%d\n",
			first.version, last.version, first.indexEntries, last.indexEntries, first.edgeRows, last.edgeRows, first.entityRows, last.entityRows, last.edgeShards, last.entityPages)
	}
	r.printIndexBloat(w)
	fmt.Fprintf(w, "reader samples=%d unready=%d unready_ratio=%.3f max_version_lag=%d max_lag_ms=%d\n",
		r.reader.total, r.reader.unready, r.readerUnreadyRatio(), r.reader.maxVersionLag, r.reader.maxLagMS)
	fmt.Fprintf(w, "index_health samples=%d unhealthy=%d stale=%d last_status=%s\n",
		r.health.total, r.health.unhealthy, r.health.stale, r.health.lastStatus)
	fmt.Fprintf(w, "maintenance compact=%d gc=%d index_rebuild=%d reader_restart=%d\n",
		r.maintenance["compact_ok"], r.maintenance["gc_ok"], r.maintenance["index_rebuild_ok"], r.maintenance["reader_restart_ok"])
	r.printMaintenanceEffects(w)
	r.printOperations(w)
	if len(classified.active) > 0 {
		fmt.Fprintf(w, "error_events %v\n", classified.active)
	}
	if len(classified.warmup) > 0 {
		fmt.Fprintf(w, "warmup_error_events %v\n", classified.warmup)
	}
	if len(classified.plannedRestart) > 0 {
		fmt.Fprintf(w, "planned_restart_error_events %v\n", classified.plannedRestart)
	}
	if len(classified.shutdown) > 0 {
		fmt.Fprintf(w, "shutdown_error_events %v\n", classified.shutdown)
	}
}

func printCategoryDelta(w io.Writer, first usageSample, last usageSample) {
	keys := make([]string, 0, len(last.categories))
	for key := range last.categories {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !strings.HasSuffix(key, "_bytes") {
			continue
		}
		fmt.Fprintf(w, "category %s=%d delta=%d\n", key, last.categories[key], last.categories[key]-first.categories[key])
	}
}
