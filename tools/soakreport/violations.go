package main

import "fmt"

func (r *report) violations() []string {
	var result []string
	classified := r.classifiedErrors()
	if len(r.usageSamples) == 0 {
		result = append(result, "missing usage_sample")
	}
	if len(r.indexSamples) == 0 {
		result = append(result, "missing index_catalog_sample")
	}
	if r.cfg.failOnErrorEvents && len(classified.active) > 0 {
		result = append(result, fmt.Sprintf("error events present: %v", classified.active))
	}
	if r.cfg.minDuration > 0 && r.duration() < r.cfg.minDuration {
		result = append(result, fmt.Sprintf("duration %s < %s", r.duration(), r.cfg.minDuration))
	}
	if r.cfg.requireCompact && r.maintenance["compact_ok"] == 0 {
		result = append(result, "missing compact_ok")
	}
	if r.cfg.requireGC && r.maintenance["gc_ok"] == 0 {
		result = append(result, "missing gc_ok")
	}
	if r.cfg.requireIndexRebuild && r.maintenance["index_rebuild_ok"] == 0 {
		result = append(result, "missing index_rebuild_ok")
	}
	if r.cfg.requireReaderRestart && r.maintenance["reader_restart_ok"] == 0 {
		result = append(result, "missing reader_restart_ok")
	}
	for _, name := range r.cfg.requiredOperations {
		operation, ok := r.operations[name]
		if !ok || operation.count == 0 {
			result = append(result, fmt.Sprintf("missing operation_metric %s", name))
			continue
		}
		if errors, _, _ := effectiveOperationErrors(operation, classified.plannedRestart, classified.shutdown); errors > 0 {
			result = append(result, fmt.Sprintf("operation %s errors %d", name, errors))
		}
	}
	if last, ok := r.lastUsage(); ok && last.commitTailLength > r.cfg.maxFinalCommitTail {
		result = append(result, fmt.Sprintf("final commit tail %d > %d", last.commitTailLength, r.cfg.maxFinalCommitTail))
	}
	if r.reader.total > 0 && r.readerUnreadyRatio() > r.cfg.maxReaderUnreadyRatio {
		result = append(result, fmt.Sprintf("reader unready ratio %.3f > %.3f", r.readerUnreadyRatio(), r.cfg.maxReaderUnreadyRatio))
	}
	if r.health.unhealthy > r.cfg.maxIndexUnhealthySamples {
		result = append(result, fmt.Sprintf("index unhealthy samples %d > %d", r.health.unhealthy, r.cfg.maxIndexUnhealthySamples))
	}
	return result
}
