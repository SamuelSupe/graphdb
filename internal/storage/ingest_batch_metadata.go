package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

func (s *TenantStore) saveIngestBatchResultMetadata(
	ctx context.Context,
	tenantID string,
	candidates []*ingestBatchCandidate,
) error {
	return s.saveIngestBatchResultMetadataWithFailures(ctx, tenantID, candidates, true)
}

func (s *TenantStore) saveIngestBatchResultMetadataWithFailures(
	ctx context.Context,
	tenantID string,
	candidates []*ingestBatchCandidate,
	saveFailures bool,
) error {
	type collectorKey struct {
		source      string
		collectorID string
	}
	type collectorMetadataGroup struct {
		updates       []ingestCollectorStatusUpdate
		records       sync.WaitGroup
		recordsMu     sync.Mutex
		recordsFailed bool
	}
	collectorOrder := make([]collectorKey, 0)
	collectorGroups := make(map[collectorKey]*collectorMetadataGroup)
	recordJobs := make([]func() error, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.skipMetadata {
			continue
		}
		candidate := candidate
		finished := mutationTime(ctx)
		key := collectorKey{source: candidate.request.Source, collectorID: candidate.request.CollectorID}
		group := collectorGroups[key]
		if group == nil {
			group = &collectorMetadataGroup{}
			collectorGroups[key] = group
			collectorOrder = append(collectorOrder, key)
		}
		group.records.Add(1)
		recordJobs = append(recordJobs, func() error {
			defer group.records.Done()
			var result error
			if err := s.saveIngestBatch(ctx, tenantID, IngestBatchRecord{
				Request: candidate.request, Result: candidate.result,
				StartedAt: candidate.started, FinishedAt: finished,
			}); err != nil {
				result = errors.Join(result, fmt.Errorf("save ingest batch: %w", err))
			}
			if saveFailures && candidate.result.Failed > 0 {
				if err := s.saveDeadLetter(ctx, tenantID, candidate.request, candidate.result); err != nil {
					result = errors.Join(result, fmt.Errorf("save dead letter: %w", err))
				}
			}
			if result != nil {
				group.recordsMu.Lock()
				group.recordsFailed = true
				group.recordsMu.Unlock()
			}
			return result
		})
		group.updates = append(group.updates, ingestCollectorStatusUpdate{
			request: candidate.request, result: candidate.result,
			started: candidate.started, finished: finished,
		})
	}
	jobs := append(make([]func() error, 0, len(recordJobs)+len(collectorOrder)), recordJobs...)
	for _, key := range collectorOrder {
		group := collectorGroups[key]
		jobs = append(jobs, func() error {
			group.records.Wait()
			group.recordsMu.Lock()
			recordsFailed := group.recordsFailed
			group.recordsMu.Unlock()
			if recordsFailed {
				return nil
			}
			if err := s.saveCollectorStatusBatch(ctx, tenantID, group.updates); err != nil {
				return fmt.Errorf("save collector status: %w", err)
			}
			return nil
		})
	}
	return runIngestMetadataJobs(jobs)
}

func runIngestMetadataJobs(jobs []func() error) error {
	if len(jobs) == 0 {
		return nil
	}
	workerCount := min(len(jobs), 8)
	jobCh := make(chan func() error)
	errCh := make(chan error, len(jobs))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for job := range jobCh {
				if err := job(); err != nil {
					errCh <- err
				}
			}
		}()
	}
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)
	workers.Wait()
	close(errCh)
	var result error
	for err := range errCh {
		result = errors.Join(result, err)
	}
	return result
}
