package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *TenantStore) CommitWithReport(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (result CommitResult, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.commit", append([]attribute.KeyValue{
		tenantTraceAttr(tenantID),
	}, append(commitOptionTraceAttrs(opts), mutationTraceAttrs(mutations)...)...)...)
	defer func() {
		span.SetAttributes(
			attribute.Int64("graphdb.commit.version", result.Version),
			attribute.Int64("graphdb.commit.readable_version", result.ReadableVersion),
			attribute.Bool("graphdb.commit.skipped", result.Skipped),
			attribute.Bool("graphdb.commit.idempotent_replay", result.IdempotentReplay),
			attribute.Int("graphdb.commit.index_warnings", len(result.IndexWarnings)),
		)
		endStorageSpan(span, err)
	}()
	if err := ValidateTenantID(tenantID); err != nil {
		return CommitResult{}, err
	}

	ensureCtx, ensureSpan := startStorageSpan(ctx, "graphdb.storage.commit.ensure_tenant_writable", tenantTraceAttr(tenantID))
	err = s.EnsureTenantWritable(ensureCtx, tenantID)
	endStorageSpan(ensureSpan, err)
	if err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	}
	request := directCommitRequest(mutations, opts)

	_, lockSpan := startStorageSpan(ctx, "graphdb.storage.commit.lock_tenant", tenantTraceAttr(tenantID))
	unlock := s.lockTenant(tenantID)
	endStorageSpan(lockSpan, nil)
	defer unlock()

	idemCtx, idemSpan := startStorageSpan(ctx, "graphdb.storage.commit.load_idempotency_record", tenantTraceAttr(tenantID), attribute.Bool("graphdb.commit.idempotency_key_present", opts.IdempotencyKey != ""))
	record, ok, err := s.loadDirectCommitRecord(idemCtx, tenantID, request)
	idemSpan.SetAttributes(attribute.Bool("graphdb.commit.idempotency_replay_found", ok))
	endStorageSpan(idemSpan, err)
	if err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	}
	if ok {
		return replayDirectCommitResult(record), nil
	}

	registryCtx, registrySpan := startStorageSpan(ctx, "graphdb.storage.commit.add_tenant_registry", tenantTraceAttr(tenantID))
	err = s.addTenantToRegistry(registryCtx, tenantID)
	endStorageSpan(registrySpan, err)
	if err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	}

	backpressureCtx, backpressureSpan := startStorageSpan(ctx, "graphdb.storage.commit.check_backpressure.initial", tenantTraceAttr(tenantID))
	err = s.CheckWriteBackpressure(backpressureCtx, tenantID)
	endStorageSpan(backpressureSpan, err)
	if err != nil {
		return CommitResult{}, err
	}

	leaseCtx, leaseSpan := startStorageSpan(ctx, "graphdb.storage.commit.acquire_writer_lease", tenantTraceAttr(tenantID))
	err = s.acquireWriterLease(leaseCtx, tenantID)
	endStorageSpan(leaseSpan, err)
	if err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, pressure
		}
		return CommitResult{}, err
	}

	backpressureCtx, backpressureSpan = startStorageSpan(ctx, "graphdb.storage.commit.check_backpressure.after_lease", tenantTraceAttr(tenantID))
	err = s.CheckWriteBackpressure(backpressureCtx, tenantID)
	endStorageSpan(backpressureSpan, err)
	if err != nil {
		return CommitResult{}, err
	}
	started := time.Now().UTC()
	retryCtx, retrySpan := startStorageSpan(ctx, "graphdb.storage.commit.retry_loop", tenantTraceAttr(tenantID))
	result, err = s.commitWithRetryLocked(retryCtx, tenantID, mutations, opts)
	endStorageSpan(retrySpan, err)
	finished := time.Now().UTC()
	if err != nil {
		return CommitResult{}, err
	}

	saveCtx, saveSpan := startStorageSpan(ctx, "graphdb.storage.commit.save_idempotency_record", tenantTraceAttr(tenantID), attribute.Int64("graphdb.commit.version", result.Version))
	err = s.saveDirectCommitRecord(saveCtx, tenantID, request, result, started, finished)
	endStorageSpan(saveSpan, err)
	if err != nil {
		return result, fmt.Errorf("save commit idempotency record: %w", err)
	}
	return result, nil
}

func (s *TenantStore) commitWithRetryLocked(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (result CommitResult, err error) {
	attempts := s.MaxRetries
	if attempts < 1 {
		attempts = 1
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Int("graphdb.commit.max_attempts", attempts))
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		attemptCtx, attemptSpan := startStorageSpan(ctx, "graphdb.storage.commit.attempt",
			tenantTraceAttr(tenantID),
			attribute.Int("graphdb.commit.attempt", attempt+1),
			attribute.Int("graphdb.commit.max_attempts", attempts),
		)
		result, err = s.commitOnceLocked(attemptCtx, tenantID, mutations, opts)
		attemptSpan.SetAttributes(
			attribute.Int64("graphdb.commit.version", result.Version),
			attribute.Bool("graphdb.commit.skipped", result.Skipped),
		)
		endStorageSpan(attemptSpan, err)
		if err == nil {
			return result, nil
		}
		last = err
		if !errors.Is(err, ErrConflict) {
			return CommitResult{}, err
		}
		s.deleteWriteCache(tenantID)
		if attempt+1 >= attempts {
			break
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return CommitResult{}, err
		}
		leaseCtx, leaseSpan := startStorageSpan(ctx, "graphdb.storage.commit.reacquire_writer_lease",
			tenantTraceAttr(tenantID),
			attribute.Int("graphdb.commit.attempt", attempt+1),
		)
		err = s.acquireWriterLease(leaseCtx, tenantID)
		endStorageSpan(leaseSpan, err)
		if err != nil {
			if pressure := s.objectStoreBackpressureError(err); pressure != nil {
				return CommitResult{}, pressure
			}
			return CommitResult{}, err
		}
	}
	return CommitResult{}, last
}

func (s *TenantStore) commitOnceLocked(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (result CommitResult, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.commit.once", append([]attribute.KeyValue{
		tenantTraceAttr(tenantID),
	}, append(commitOptionTraceAttrs(opts), mutationTraceAttrs(mutations)...)...)...)
	defer func() {
		span.SetAttributes(
			attribute.Int64("graphdb.commit.version", result.Version),
			attribute.Bool("graphdb.commit.skipped", result.Skipped),
			attribute.Int("graphdb.commit.index_warnings", len(result.IndexWarnings)),
		)
		endStorageSpan(span, err)
	}()

	loadCtx, loadSpan := startStorageSpan(ctx, "graphdb.storage.commit.load_for_write", tenantTraceAttr(tenantID))
	loaded, err := s.loadForWriteLocked(loadCtx, tenantID)
	if err == nil {
		loadSpan.SetAttributes(append(manifestTraceAttrs("graphdb.loaded_manifest", loaded.Manifest), graphTraceAttrs("graphdb.loaded_graph", loaded.Graph)...)...)
	}
	endStorageSpan(loadSpan, err)
	if err != nil {
		return CommitResult{}, err
	}

	var policyReport graph.ApplyReport
	policyCtx, policySpan := startStorageSpan(ctx, "graphdb.storage.commit.resolve_source_policy", tenantTraceAttr(tenantID))
	mutations, policyReport, err = s.resolveSourcePolicy(policyCtx, tenantID, mutations)
	policySpan.SetAttributes(attribute.Int("graphdb.commit.policy_suppressed", len(policyReport.Suppressed)))
	endStorageSpan(policySpan, err)
	if err != nil {
		return CommitResult{}, err
	}
	manifest := loaded.Manifest
	span.SetAttributes(manifestTraceAttrs("graphdb.loaded_manifest", manifest)...)
	if opts.ExpectedVersion != nil && *opts.ExpectedVersion != manifest.Version {
		return CommitResult{}, fmt.Errorf("expected version %d, current version %d", *opts.ExpectedVersion, manifest.Version)
	}
	version := manifest.Version + 1
	commitID, err := newCommitID()
	if err != nil {
		return CommitResult{}, err
	}
	commit := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		ID:            commitID,
		TenantID:      tenantID,
		Version:       version,
		CreatedAt:     time.Now().UTC(),
		Mutations:     mutations,
	}

	_, applySpan := startStorageSpan(ctx, "graphdb.storage.commit.apply_mutations",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.commit.version", version),
	)
	nextGraph, report, err := loaded.Graph.ApplyCommitCopyWithOptions(commit, graph.ApplyOptions{})
	if err == nil {
		applySpan.SetAttributes(append(graphTraceAttrs("graphdb.next_graph", nextGraph),
			attribute.Int("graphdb.commit.suppressed", len(report.Suppressed)),
			attribute.Int("graphdb.commit.canonical_entities", len(report.CanonicalEntities)),
			attribute.Int("graphdb.commit.canonical_edges", len(report.CanonicalEdges)),
			attribute.Int("graphdb.commit.affected_entities", len(report.AffectedEntityIDs)),
		)...)
	}
	endStorageSpan(applySpan, err)
	if err != nil {
		return CommitResult{}, err
	}
	report.Suppressed = append(policyReport.Suppressed, report.Suppressed...)

	quotaCtx, quotaSpan := startStorageSpan(ctx, "graphdb.storage.commit.check_quota_after_apply", tenantTraceAttr(tenantID))
	err = s.checkQuotaAfterApply(quotaCtx, tenantID, loaded.Graph, nextGraph)
	endStorageSpan(quotaSpan, err)
	if err != nil {
		return CommitResult{}, err
	}

	_, md5Span := startStorageSpan(ctx, "graphdb.storage.commit.compute_content_md5", tenantTraceAttr(tenantID))
	previousMD5, err := loaded.Graph.ContentMD5()
	if err != nil {
		endStorageSpan(md5Span, err)
		return CommitResult{}, err
	}
	nextMD5, err := nextGraph.ContentMD5()
	md5Span.SetAttributes(attribute.Bool("graphdb.commit.content_changed", previousMD5 != nextMD5))
	endStorageSpan(md5Span, err)
	if err != nil {
		return CommitResult{}, err
	}
	if previousMD5 == nextMD5 {
		return CommitResult{
			Manifest:          manifest,
			ReadableVersion:   manifest.Version,
			Skipped:           true,
			DataMD5:           previousMD5,
			Suppressed:        report.Suppressed,
			CanonicalEntities: report.CanonicalEntities,
			CanonicalEdges:    report.CanonicalEdges,
		}, nil
	}
	commitKey := s.commitKey(tenantID, version, commitID)
	putCommitCtx, putCommitSpan := startStorageSpan(ctx, "graphdb.storage.commit.put_commit_object",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.commit.version", version),
	)
	err = s.putCommitObjectIfAbsent(putCommitCtx, commitKey, commit)
	endStorageSpan(putCommitSpan, err)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}
	manifest.TenantID = tenantID
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.Version = version
	manifest.HeadCommitID = commitID
	manifest.CommitKeys = append(append([]string(nil), manifest.CommitKeys...), commitKey)
	manifest.UpdatedAt = commit.CreatedAt
	segmentCtx, segmentSpan := startStorageSpan(ctx, "graphdb.storage.commit.segment_tail",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.commit_tail.length_before", manifestCommitTailLength(manifest)),
		attribute.Int("graphdb.commit_keys.before_segment", len(manifest.CommitKeys)),
	)
	err = s.segmentCommitTailIfNeeded(segmentCtx, tenantID, &manifest)
	segmentSpan.SetAttributes(
		attribute.Int("graphdb.commit_tail.length_after", manifestCommitTailLength(manifest)),
		attribute.Int("graphdb.commit_segments.after", len(manifest.CommitSegments)),
		attribute.Int("graphdb.commit_keys.after_segment", len(manifest.CommitKeys)),
	)
	endStorageSpan(segmentSpan, err)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}

	putManifestCtx, putManifestSpan := startStorageSpan(ctx, "graphdb.storage.commit.put_manifest",
		append([]attribute.KeyValue{
			tenantTraceAttr(tenantID),
		}, manifestTraceAttrs("graphdb.manifest", manifest)...)...,
	)
	meta, err := s.putManifestMeta(putManifestCtx, tenantID, manifest, loaded.Meta)
	endStorageSpan(putManifestSpan, err)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}
	s.setWriteCache(tenantID, loadedGraph{Graph: nextGraph, Manifest: manifest, Meta: meta})
	result = CommitResult{
		Manifest:          manifest,
		ReadableVersion:   version,
		ReadAfterCommitID: commitID,
		DataMD5:           nextMD5,
		Suppressed:        report.Suppressed,
		CanonicalEntities: report.CanonicalEntities,
		CanonicalEdges:    report.CanonicalEdges,
	}
	indexCtx, indexSpan := startStorageSpan(ctx, "graphdb.storage.commit.update_indexes",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.commit.version", version),
		attribute.Int("graphdb.commit.affected_entities", len(report.AffectedEntityIDs)),
	)
	indexErr := s.updateIndexesAfterCommit(indexCtx, tenantID, loaded.Graph, nextGraph, mutations, report, version)
	endStorageSpan(indexSpan, indexErr)
	if indexErr != nil {
		result.IndexWarnings = append(result.IndexWarnings, "incremental index update failed: "+indexErr.Error())
	}
	return result, nil
}
