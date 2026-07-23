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
	preflightCtx, preflightSpan := startStorageSpan(ctx, "graphdb.storage.commit.preflight",
		tenantTraceAttr(tenantID),
		attribute.Bool("graphdb.commit.write_backpressure_reused", opts.WriteBackpressureChecked),
	)
	if !opts.WriteBackpressureChecked {
		checkCtx, checkSpan := startStorageSpan(preflightCtx, "graphdb.storage.commit.check_backpressure", tenantTraceAttr(tenantID))
		err = s.CheckWriteBackpressure(checkCtx, tenantID)
		endStorageSpan(checkSpan, err)
		if err != nil {
			endStorageSpan(preflightSpan, err)
			return CommitResult{}, err
		}
	}
	endStorageSpan(preflightSpan, nil)

	result, reservation, err := s.commitWithinTenantLock(ctx, tenantID, mutations, opts, request)
	if renewalErr := stopCommitReservationRenewal(reservation); renewalErr != nil {
		if err == nil {
			err = renewalErr
		} else {
			err = errors.Join(err, renewalErr)
		}
	}
	if err != nil {
		if abortErr := s.abortDirectCommit(reservation, err); abortErr != nil {
			err = errors.Join(err, fmt.Errorf("release commit idempotency reservation: %w", abortErr))
		}
		return CommitResult{}, err
	}
	finished := time.Now().UTC()
	completeCtx, completeSpan := startStorageSpan(ctx, "graphdb.storage.commit.complete_idempotency_record",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.commit.version", result.Version),
		attribute.Bool("graphdb.commit.outside_tenant_lock", true),
	)
	err = s.completeDirectCommit(completeCtx, reservation, result, finished)
	endStorageSpan(completeSpan, err)
	if err != nil {
		return result, fmt.Errorf("complete commit idempotency record: %w", err)
	}
	return result, nil
}

func (s *TenantStore) commitWithinTenantLock(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions, request DirectCommitRequest) (result CommitResult, reservation *directCommitReservation, err error) {
	if s.coordinated() {
		return s.commitWithoutTenantLock(ctx, tenantID, mutations, opts, request)
	}
	_, lockSpan := startStorageSpan(ctx, "graphdb.storage.commit.lock_tenant", tenantTraceAttr(tenantID))
	lockStarted := time.Now()
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	lockSpan.SetAttributes(
		attribute.Int64("graphdb.commit.lock_wait_ms", time.Since(lockStarted).Milliseconds()),
		attribute.Bool("graphdb.commit.lock_acquired", err == nil),
		attribute.String("graphdb.commit.lock_priority", "foreground"),
	)
	endStorageSpan(lockSpan, err)
	if err != nil {
		return CommitResult{}, nil, err
	}
	criticalCtx, criticalSpan := startStorageSpan(ctx, "graphdb.storage.commit.critical_section", tenantTraceAttr(tenantID))
	criticalStarted := time.Now()
	defer func() {
		unlock()
		criticalSpan.SetAttributes(
			attribute.Int64("graphdb.commit.lock_held_ms", time.Since(criticalStarted).Milliseconds()),
			attribute.Int64("graphdb.commit.version", result.Version),
		)
		endStorageSpan(criticalSpan, err)
	}()

	leaseCtx, leaseSpan := startStorageSpan(criticalCtx, "graphdb.storage.commit.acquire_writer_lease", tenantTraceAttr(tenantID))
	err = s.acquireWriterLease(leaseCtx, tenantID)
	endStorageSpan(leaseSpan, err)
	if err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, nil, pressure
		}
		return CommitResult{}, nil, err
	}
	criticalCtx, err = s.bindCurrentWriterFence(criticalCtx, tenantID)
	if err != nil {
		return CommitResult{}, nil, err
	}
	revalidateCtx, revalidateSpan := startStorageSpan(criticalCtx, "graphdb.storage.commit.revalidate_tenant_writable", tenantTraceAttr(tenantID))
	err = s.EnsureTenantWritable(revalidateCtx, tenantID)
	endStorageSpan(revalidateSpan, err)
	if err != nil {
		return CommitResult{}, nil, err
	}
	registryCtx, registrySpan := startStorageSpan(criticalCtx, "graphdb.storage.commit.add_tenant_registry", tenantTraceAttr(tenantID))
	err = s.addTenantToRegistry(registryCtx, tenantID)
	endStorageSpan(registrySpan, err)
	if err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, nil, pressure
		}
		return CommitResult{}, nil, err
	}
	started := time.Now().UTC()
	idemCtx, idemSpan := startStorageSpan(criticalCtx, "graphdb.storage.commit.reserve_idempotency_record", tenantTraceAttr(tenantID), attribute.Bool("graphdb.commit.idempotency_key_present", request.IdempotencyKey != ""))
	reservation, replay, err := s.beginDirectCommit(idemCtx, tenantID, request, started)
	idemSpan.SetAttributes(attribute.Bool("graphdb.commit.idempotency_replay_found", replay != nil))
	endStorageSpan(idemSpan, err)
	if err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return CommitResult{}, nil, pressure
		}
		return CommitResult{}, nil, err
	}
	if replay != nil {
		return *replay, nil, nil
	}
	opts.directCommit = reservation

	retryCtx, retrySpan := startStorageSpan(criticalCtx, "graphdb.storage.commit.retry_loop", tenantTraceAttr(tenantID))
	result, err = s.commitWithRetryLocked(retryCtx, tenantID, mutations, opts)
	endStorageSpan(retrySpan, err)
	if err != nil {
		return CommitResult{}, reservation, err
	}
	return result, reservation, nil
}

func (s *TenantStore) commitWithoutTenantLock(
	ctx context.Context,
	tenantID string,
	mutations graph.Mutations,
	opts CommitOptions,
	request DirectCommitRequest,
) (CommitResult, *directCommitReservation, error) {
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return CommitResult{}, nil, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return CommitResult{}, nil, err
	}
	started := time.Now().UTC()
	reservation, replay, err := s.beginDirectCommit(ctx, tenantID, request, started)
	if err != nil {
		return CommitResult{}, nil, err
	}
	if replay != nil {
		return *replay, nil, nil
	}
	ctx = s.startCommitReservationRenewal(ctx, reservation)
	opts.directCommit = reservation
	result, err := s.commitWithRetryLocked(ctx, tenantID, mutations, opts)
	if err != nil {
		return CommitResult{}, reservation, err
	}
	return result, reservation, nil
}

func (s *TenantStore) commitWithRetryLocked(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (result CommitResult, err error) {
	attempts := s.MaxRetries
	if s.coordinated() {
		// The public setting is the maximum number of replays after the
		// initial optimistic attempt.
		attempts = s.CoordinatorRetryLimit + 1
	}
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
		if s.coordinated() && opts.ExpectedVersion != nil {
			return CommitResult{}, fmt.Errorf("%w: expected version %d changed while publishing", ErrVersionConflict, *opts.ExpectedVersion)
		}
		if !s.coordinated() {
			s.deleteWriteCache(tenantID)
		}
		if attempt+1 >= attempts {
			break
		}
		var delayErr error
		if s.coordinated() {
			delayErr = coordinatorRetryDelay(ctx, attempt)
		} else {
			delayErr = retryDelay(ctx, attempt)
		}
		if delayErr != nil {
			return CommitResult{}, delayErr
		}
		if s.coordinated() {
			continue
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
	if s.coordinated() && errors.Is(last, ErrConflict) {
		return CommitResult{}, fmt.Errorf("%w: tenant %q head changed after %d attempts", ErrWriteConflict, tenantID, attempts)
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
	var loaded loadedGraph
	if opts.ExpectedVersion != nil {
		manifest, meta, manifestErr := s.getManifest(ctx, tenantID)
		if manifestErr != nil {
			return CommitResult{}, manifestErr
		}
		if *opts.ExpectedVersion != manifest.Version {
			return CommitResult{}, fmt.Errorf("%w: expected version %d, current version %d", ErrVersionConflict, *opts.ExpectedVersion, manifest.Version)
		}
		loaded, err = s.loadForExpectedVersionLocked(ctx, tenantID, *opts.ExpectedVersion, manifest, meta)
	} else {
		loaded, err = s.loadForWriteLocked(ctx, tenantID)
	}
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
	mutations, relationSchemas, relationSchemaMeta, err := s.prepareRelationSchemaMutations(ctx, tenantID, mutations)
	if err != nil {
		return CommitResult{}, err
	}
	manifest := loaded.Manifest
	span.SetAttributes(manifestTraceAttrs("graphdb.loaded_manifest", manifest)...)
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
	nextGraph, report, err := loaded.Graph.ApplyCommitStorageCopyWithOptions(commit, graph.ApplyOptions{})
	if err == nil {
		if relationSchemas.GraphVersion == loaded.Manifest.Version {
			err = validateRelationSchemaCommit(nextGraph, relationSchemas, report.AffectedEdgeIDs)
		} else {
			err = validateRelationSchemaGraph(nextGraph, relationSchemas)
		}
	}
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

	_, fingerprintSpan := startStorageSpan(ctx, "graphdb.storage.commit.evaluate_content_change", tenantTraceAttr(tenantID))
	fingerprintSpan.SetAttributes(attribute.Bool("graphdb.commit.content_changed", report.Changed))
	endStorageSpan(fingerprintSpan, nil)
	if !report.Changed {
		if err := s.ensureCoordinationPointCurrent(ctx, tenantID, loaded.Meta); err != nil {
			return CommitResult{}, err
		}
		previousMD5 := loaded.DataMD5
		if previousMD5 == "" {
			previousMD5 = manifest.DataMD5
		}
		if previousMD5 == "" || loaded.CacheBytes <= 0 {
			computedMD5, logicalBytes, hashErr := loaded.Graph.ContentMD5WithLogicalSize()
			if hashErr != nil {
				return CommitResult{}, hashErr
			}
			if previousMD5 == "" {
				previousMD5 = computedMD5
			}
			loaded.CacheBytes = writeCacheBytesForGraph(loaded.Graph, logicalBytes)
		}
		if previousMD5 == "" {
			return CommitResult{}, fmt.Errorf("logical graph content md5 is empty")
		}
		loaded.DataMD5 = previousMD5
		loaded.Manifest.DataMD5 = previousMD5
		manifest.DataMD5 = previousMD5
		result := CommitResult{
			Manifest:          manifest,
			ReadableVersion:   manifest.Version,
			Skipped:           true,
			DataMD5:           previousMD5,
			Suppressed:        report.Suppressed,
			CanonicalEntities: report.CanonicalEntities,
			CanonicalEdges:    report.CanonicalEdges,
		}
		if err := s.prepareDirectCommit(ctx, opts.directCommit, result, time.Now().UTC()); err != nil {
			return CommitResult{}, err
		}
		if s.coordinated() && opts.directCommit != nil {
			token, err := parseCoordinatedHeadToken(loaded.Meta)
			if err != nil {
				return CommitResult{}, err
			}
			request := HeadPublishRequest{
				TenantID:                     tenantID,
				ExpectedRevision:             token.Revision,
				ExpectedGeneration:           token.Generation,
				ExpectedWriteContextRevision: token.ContextRevision,
				CommitID:                     manifest.HeadCommitID,
			}
			if err := attachCoordinatorCommitMetadata(
				&request, opts.directCommit, result, manifest.Version,
			); err != nil {
				return CommitResult{}, err
			}
			committed, err := s.Coordinator.CompleteNoop(ctx, request)
			if err != nil {
				s.observeCoordinatorCAS(tenantID, "error", 0)
				return CommitResult{}, err
			}
			if !committed {
				s.observeCoordinatorCAS(tenantID, "conflict", 0)
				return CommitResult{}, fmt.Errorf("%w: tenant %q changed while completing no-op commit", ErrConflict, tenantID)
			}
			s.observeCoordinatorCAS(tenantID, "committed", token.Revision)
		}
		s.setWriteCache(tenantID, loaded)
		return result, nil
	}
	_, md5Span := startStorageSpan(ctx, "graphdb.storage.commit.compute_content_md5", tenantTraceAttr(tenantID))
	nextMD5, logicalBytes, err := nextGraph.ContentMD5WithLogicalSize()
	endStorageSpan(md5Span, err)
	if err != nil {
		return CommitResult{}, err
	}
	commitKey := s.commitKey(tenantID, version, commitID)
	putCommitCtx, putCommitSpan := startStorageSpan(ctx, "graphdb.storage.commit.put_commit_object",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.commit.version", version),
	)
	commitMeta, putErr := s.putCommitObjectIfAbsentMeta(putCommitCtx, commitKey, commit)
	err = putErr
	endStorageSpan(putCommitSpan, err)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}
	commitPublished := false
	defer func() {
		if err == nil || commitPublished || !commitMeta.Exists || commitMeta.ETag == "" {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		current, _, manifestErr := s.getManifest(cleanupCtx, tenantID)
		if manifestErr != nil || current.Version >= commit.Version {
			return
		}
		cleanupErr := s.Objects.DeleteConditional(cleanupCtx, commitKey, PutCondition{IfMatch: commitMeta.ETag})
		if cleanupErr != nil && !errors.Is(cleanupErr, ErrConflict) && !errors.Is(cleanupErr, ErrNotFound) {
			err = errors.Join(err, fmt.Errorf("rollback unpublished commit object %q: %w", commitKey, cleanupErr))
		}
	}()
	manifest.TenantID = tenantID
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.Version = version
	manifest.HeadCommitID = commitID
	manifest.CommitKeys = append(append([]string(nil), manifest.CommitKeys...), commitKey)
	manifest.UpdatedAt = commit.CreatedAt
	manifest.DataMD5 = nextMD5
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
	result = CommitResult{
		Manifest:          manifest,
		ReadableVersion:   version,
		ReadAfterCommitID: commitID,
		DataMD5:           nextMD5,
		Suppressed:        report.Suppressed,
		CanonicalEntities: report.CanonicalEntities,
		CanonicalEdges:    report.CanonicalEdges,
	}
	prepareCtx, prepareSpan := startStorageSpan(ctx, "graphdb.storage.commit.prepare_idempotency_record",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.commit.version", version),
	)
	err = s.prepareDirectCommit(prepareCtx, opts.directCommit, result, time.Now().UTC())
	endStorageSpan(prepareSpan, err)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return CommitResult{}, err
	}

	putManifestCtx, putManifestSpan := startStorageSpan(ctx, "graphdb.storage.commit.put_manifest",
		append([]attribute.KeyValue{
			tenantTraceAttr(tenantID),
		}, manifestTraceAttrs("graphdb.manifest", manifest)...)...,
	)
	meta, err := s.putManifestForCommit(putManifestCtx, tenantID, manifest, loaded.Meta, opts.directCommit)
	endStorageSpan(putManifestSpan, err)
	if err != nil {
		s.handleManifestPublishFailureCache(tenantID, loaded, err)
		return CommitResult{}, err
	}
	commitPublished = true
	s.setWriteCache(tenantID, loadedGraph{
		Graph: nextGraph, Manifest: manifest, Meta: meta, DataMD5: nextMD5,
		CacheBytes: writeCacheBytesForGraph(nextGraph, logicalBytes),
	})
	if schemaErr := s.advanceRelationSchemaValidation(ctx, tenantID, relationSchemas, relationSchemaMeta, version); schemaErr != nil {
		result.IndexWarnings = append(result.IndexWarnings, "relation schema validation checkpoint update failed: "+schemaErr.Error())
	}
	if s.coordinated() {
		return result, nil
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
