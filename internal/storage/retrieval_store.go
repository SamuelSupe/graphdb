package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"
)

func (s *TenantStore) GetRetrievalDefinitions(
	ctx context.Context,
	tenantID string,
) (RetrievalDefinitionRecord, error) {
	state, err := s.retrievalTenantState(ctx, tenantID)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	record, _, err := s.getRetrievalDefinitionsWithMeta(ctx, tenantID)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	if !retrievalGenerationMatches(record.TenantGeneration, state.generation) {
		return RetrievalDefinitionRecord{}, ErrNotFound
	}
	return record, nil
}

func (s *TenantStore) PublishRetrievalDefinitions(
	ctx context.Context,
	tenantID string,
	expectedRevision int64,
	definitions []RetrievalDefinition,
) (RetrievalDefinitionRecord, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	if expectedRevision < 0 {
		return RetrievalDefinitionRecord{}, fmt.Errorf(
			"expected retrieval definition revision must be >= 0",
		)
	}
	normalized, err := normalizeRetrievalDefinitions(definitions)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	defer unlock()
	ctx, err = s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	state, err := s.retrievalTenantState(ctx, tenantID)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	current, meta, err := s.getRetrievalDefinitionsWithMeta(ctx, tenantID)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	if !retrievalGenerationMatches(
		current.TenantGeneration,
		state.generation,
	) {
		current = RetrievalDefinitionRecord{TenantID: tenantID}
	}
	if current.Revision != expectedRevision {
		return RetrievalDefinitionRecord{}, fmt.Errorf(
			"%w: retrieval definition revision is %d, expected %d",
			ErrConflict,
			current.Revision,
			expectedRevision,
		)
	}
	now := time.Now().UTC()
	createdAt := make(map[string]time.Time, len(current.Definitions))
	for _, definition := range current.Definitions {
		createdAt[definition.Name] = definition.CreatedAt
	}
	for i := range normalized {
		normalized[i].CreatedAt = createdAt[normalized[i].Name]
		if normalized[i].CreatedAt.IsZero() {
			normalized[i].CreatedAt = now
		}
		normalized[i].UpdatedAt = now
	}
	next := RetrievalDefinitionRecord{
		LayoutVersion:    RetrievalExtensionLayoutVersion,
		TenantID:         tenantID,
		TenantGeneration: state.generation,
		Revision:         expectedRevision + 1,
		Definitions:      normalized,
		UpdatedAt:        now,
	}
	data, err := marshalParquetRetrievalObject(
		ctx,
		retrievalObjectDefinitions,
		tenantID,
		next.Revision,
		0,
		next,
	)
	if err != nil {
		return RetrievalDefinitionRecord{}, err
	}
	key := s.retrievalDefinitionsKey(tenantID)
	if meta.Key != key {
		meta = ObjectMeta{Key: key}
	}
	if _, err := s.putTenantBytesWithMetaResult(
		ctx,
		tenantID,
		key,
		data,
		meta,
	); err != nil {
		if errors.Is(err, ErrConflict) {
			return RetrievalDefinitionRecord{}, fmt.Errorf(
				"%w: retrieval definitions for tenant %q changed while publishing",
				ErrConflict,
				tenantID,
			)
		}
		return RetrievalDefinitionRecord{}, err
	}
	s.deleteCachedRetrievalSnapshot(tenantID)
	return next, nil
}

func (s *TenantStore) getRetrievalDefinitionsWithMeta(
	ctx context.Context,
	tenantID string,
) (RetrievalDefinitionRecord, ObjectMeta, error) {
	key := s.retrievalDefinitionsKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return RetrievalDefinitionRecord{
			LayoutVersion: RetrievalExtensionLayoutVersion,
			TenantID:      tenantID,
			Definitions:   []RetrievalDefinition{},
		}, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return RetrievalDefinitionRecord{}, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return RetrievalDefinitionRecord{}, ObjectMeta{}, fmt.Errorf(
			"unsupported retrieval definitions: only parquet is readable",
		)
	}
	var record RetrievalDefinitionRecord
	envelope, err := decodeParquetRetrievalObject(
		ctx,
		data,
		retrievalObjectDefinitions,
		&record,
	)
	if err != nil {
		return RetrievalDefinitionRecord{}, ObjectMeta{}, err
	}
	normalized, err := normalizeRetrievalDefinitions(record.Definitions)
	if err != nil {
		return RetrievalDefinitionRecord{}, ObjectMeta{}, err
	}
	if envelope.TenantID != tenantID ||
		record.TenantID != tenantID ||
		envelope.Revision != record.Revision ||
		record.LayoutVersion != RetrievalExtensionLayoutVersion ||
		record.Revision <= 0 {
		return RetrievalDefinitionRecord{}, ObjectMeta{}, fmt.Errorf(
			"retrieval definitions identity mismatch",
		)
	}
	record.Definitions = normalized
	return record, meta, nil
}

func (s *TenantStore) PutRetrievalSegment(
	ctx context.Context,
	tenantID string,
	embeddingGeneration string,
	graphVersion int64,
	kind string,
	shard string,
	codec string,
	rowCount int64,
	schemaHash string,
	data []byte,
) (RetrievalSegmentRef, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RetrievalSegmentRef{}, err
	}
	embeddingGeneration = strings.TrimSpace(embeddingGeneration)
	kind = strings.TrimSpace(kind)
	shard = strings.TrimSpace(shard)
	codec = strings.TrimSpace(codec)
	if embeddingGeneration == "" ||
		shard == "" ||
		codec == "" ||
		graphVersion < 0 ||
		rowCount < 0 {
		return RetrievalSegmentRef{}, fmt.Errorf(
			"retrieval segment generation, shard, codec, and versions are required",
		)
	}
	switch kind {
	case RetrievalSegmentChunks,
		RetrievalSegmentVector,
		RetrievalSegmentLexical:
	default:
		return RetrievalSegmentRef{}, fmt.Errorf(
			"unsupported retrieval segment kind %q",
			kind,
		)
	}
	if !isParquetBytes(data) {
		return RetrievalSegmentRef{}, fmt.Errorf(
			"retrieval segment must be parquet",
		)
	}
	state, err := s.retrievalTenantState(ctx, tenantID)
	if err != nil {
		return RetrievalSegmentRef{}, err
	}
	if graphVersion > state.graphVersion {
		return RetrievalSegmentRef{}, fmt.Errorf(
			"retrieval segment graph version %d is ahead of tenant version %d",
			graphVersion,
			state.graphVersion,
		)
	}
	key := s.retrievalSegmentKey(
		tenantID,
		embeddingGeneration,
		graphVersion,
		kind,
		shard,
	)
	if err := s.putImmutableRetrievalObject(
		ctx,
		tenantID,
		key,
		data,
	); err != nil {
		return RetrievalSegmentRef{}, err
	}
	return RetrievalSegmentRef{
		Kind:        kind,
		Key:         key,
		Format:      "parquet",
		Codec:       codec,
		RowCount:    rowCount,
		ContentHash: objectContentHash(data),
		SchemaHash:  strings.TrimSpace(schemaHash),
	}, nil
}

func (s *TenantStore) PublishRetrievalCatalog(
	ctx context.Context,
	tenantID string,
	expectedRevision int64,
	catalog RetrievalCatalog,
) (RetrievalHead, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return RetrievalHead{}, err
	}
	if expectedRevision < 0 {
		return RetrievalHead{}, fmt.Errorf(
			"expected retrieval revision must be >= 0",
		)
	}
	state, err := s.retrievalTenantState(ctx, tenantID)
	if err != nil {
		return RetrievalHead{}, err
	}
	if current, _, currentErr := s.getRetrievalHeadWithMeta(
		ctx,
		tenantID,
	); currentErr == nil &&
		retrievalGenerationMatches(
			current.TenantGeneration,
			state.generation,
		) &&
		current.Revision != expectedRevision {
		return RetrievalHead{}, fmt.Errorf(
			"%w: retrieval head revision is %d, expected %d",
			ErrConflict,
			current.Revision,
			expectedRevision,
		)
	} else if currentErr != nil && !errors.Is(currentErr, ErrNotFound) {
		return RetrievalHead{}, currentErr
	}
	catalog.LayoutVersion = RetrievalExtensionLayoutVersion
	catalog.TenantID = tenantID
	catalog.TenantGeneration = state.generation
	catalog.Revision = expectedRevision + 1
	catalog.UpdatedAt = time.Now().UTC()
	catalog, err = normalizeRetrievalCatalog(catalog)
	if err != nil {
		return RetrievalHead{}, err
	}
	if catalog.GraphVersion > state.graphVersion {
		return RetrievalHead{}, fmt.Errorf(
			"retrieval catalog graph version %d is ahead of tenant version %d",
			catalog.GraphVersion,
			state.graphVersion,
		)
	}
	if err := s.verifyRetrievalCatalogArtifacts(
		ctx,
		tenantID,
		catalog,
	); err != nil {
		return RetrievalHead{}, err
	}

	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return RetrievalHead{}, err
	}
	defer unlock()
	ctx, err = s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return RetrievalHead{}, err
	}
	currentState, err := s.retrievalTenantState(ctx, tenantID)
	if err != nil {
		return RetrievalHead{}, err
	}
	if currentState.generation != catalog.TenantGeneration {
		return RetrievalHead{}, fmt.Errorf(
			"%w: tenant generation changed while building retrieval catalog",
			ErrConflict,
		)
	}
	current, meta, err := s.getRetrievalHeadWithMeta(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		current = RetrievalHead{TenantID: tenantID}
		meta = ObjectMeta{Key: s.retrievalHeadKey(tenantID)}
	} else if err != nil {
		return RetrievalHead{}, err
	}
	if !retrievalGenerationMatches(
		current.TenantGeneration,
		currentState.generation,
	) {
		current = RetrievalHead{TenantID: tenantID}
	}
	if current.Revision != expectedRevision {
		return RetrievalHead{}, fmt.Errorf(
			"%w: retrieval head revision is %d, expected %d",
			ErrConflict,
			current.Revision,
			expectedRevision,
		)
	}
	if catalog.GraphVersion < current.GraphVersion {
		return RetrievalHead{}, fmt.Errorf(
			"%w: retrieval graph version cannot move from %d to %d",
			ErrConflict,
			current.GraphVersion,
			catalog.GraphVersion,
		)
	}
	definitions, _, err := s.getRetrievalDefinitionsWithMeta(
		ctx,
		tenantID,
	)
	if err != nil {
		return RetrievalHead{}, err
	}
	if !retrievalGenerationMatches(
		definitions.TenantGeneration,
		currentState.generation,
	) ||
		definitions.Revision != catalog.DefinitionRevision ||
		!retrievalDefinitionProfileEnabled(
			definitions,
			catalog.EmbeddingProfile,
		) {
		return RetrievalHead{}, fmt.Errorf(
			"%w: retrieval definitions changed or profile %q is disabled",
			ErrConflict,
			catalog.EmbeddingProfile,
		)
	}
	catalogData, err := marshalParquetRetrievalObject(
		ctx,
		retrievalObjectCatalog,
		tenantID,
		catalog.Revision,
		catalog.GraphVersion,
		catalog,
	)
	if err != nil {
		return RetrievalHead{}, err
	}
	catalogHash := objectContentHash(catalogData)
	catalogKey := s.retrievalCatalogKey(
		tenantID,
		catalog.Revision,
		catalog.GraphVersion,
		catalogHash,
	)
	if err := s.putImmutableRetrievalObject(
		ctx,
		tenantID,
		catalogKey,
		catalogData,
	); err != nil {
		return RetrievalHead{}, err
	}
	head := retrievalHeadFromCatalog(catalog, catalogKey, catalogHash)
	headData, err := marshalParquetRetrievalObject(
		ctx,
		retrievalObjectHead,
		tenantID,
		head.Revision,
		head.GraphVersion,
		head,
	)
	if err != nil {
		return RetrievalHead{}, err
	}
	key := s.retrievalHeadKey(tenantID)
	if meta.Key != key {
		meta = ObjectMeta{Key: key}
	}
	nextMeta, err := s.putTenantBytesWithMetaResult(
		ctx,
		tenantID,
		key,
		headData,
		meta,
	)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return RetrievalHead{}, fmt.Errorf(
				"%w: retrieval head for tenant %q changed while publishing",
				ErrConflict,
				tenantID,
			)
		}
		return RetrievalHead{}, err
	}
	if _, err := s.resolveRetrievalSnapshotAtHead(
		ctx,
		tenantID,
		head,
	); err != nil {
		rollbackCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		rollbackErr := s.Objects.DeleteConditional(
			rollbackCtx,
			key,
			PutCondition{IfMatch: nextMeta.ETag},
		)
		if rollbackErr != nil &&
			!errors.Is(rollbackErr, ErrConflict) &&
			!errors.Is(rollbackErr, ErrNotFound) {
			return RetrievalHead{}, errors.Join(
				err,
				fmt.Errorf(
					"rollback incomplete retrieval head %q: %w",
					key,
					rollbackErr,
				),
			)
		}
		return RetrievalHead{}, err
	}
	s.deleteCachedRetrievalSnapshot(tenantID)
	return head, nil
}

func (s *TenantStore) GetRetrievalHead(
	ctx context.Context,
	tenantID string,
) (RetrievalHead, error) {
	head, _, err := s.getRetrievalHeadWithMeta(ctx, tenantID)
	return head, err
}

func (s *TenantStore) getRetrievalHeadWithMeta(
	ctx context.Context,
	tenantID string,
) (RetrievalHead, ObjectMeta, error) {
	key := s.retrievalHeadKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return RetrievalHead{}, ObjectMeta{Key: key}, err
	}
	if !isParquetBytes(data) {
		return RetrievalHead{}, ObjectMeta{}, fmt.Errorf(
			"unsupported retrieval head: only parquet is readable",
		)
	}
	var head RetrievalHead
	envelope, err := decodeParquetRetrievalObject(
		ctx,
		data,
		retrievalObjectHead,
		&head,
	)
	if err != nil {
		return RetrievalHead{}, ObjectMeta{}, err
	}
	if envelope.TenantID != tenantID ||
		head.TenantID != tenantID ||
		envelope.Revision != head.Revision ||
		envelope.GraphVersion != head.GraphVersion ||
		head.LayoutVersion != RetrievalExtensionLayoutVersion ||
		head.Revision <= 0 ||
		head.GraphVersion < 0 ||
		head.DefinitionRevision <= 0 ||
		head.CatalogKey == "" ||
		head.CatalogHash == "" ||
		head.EmbeddingProfile == "" ||
		head.EmbeddingGeneration == "" ||
		head.EmbeddingDimensions <= 0 {
		return RetrievalHead{}, ObjectMeta{}, fmt.Errorf(
			"retrieval head identity mismatch",
		)
	}
	return head, meta, nil
}

func (s *TenantStore) getRetrievalCatalogAtHead(
	ctx context.Context,
	tenantID string,
	head RetrievalHead,
) (RetrievalCatalog, error) {
	data, err := s.Objects.Get(ctx, head.CatalogKey)
	if err != nil {
		return RetrievalCatalog{}, err
	}
	if objectContentHash(data) != head.CatalogHash {
		return RetrievalCatalog{}, fmt.Errorf(
			"retrieval catalog object hash mismatch",
		)
	}
	if !isParquetBytes(data) {
		return RetrievalCatalog{}, fmt.Errorf(
			"unsupported retrieval catalog: only parquet is readable",
		)
	}
	var catalog RetrievalCatalog
	envelope, err := decodeParquetRetrievalObject(
		ctx,
		data,
		retrievalObjectCatalog,
		&catalog,
	)
	if err != nil {
		return RetrievalCatalog{}, err
	}
	catalog, err = normalizeRetrievalCatalog(catalog)
	if err != nil {
		return RetrievalCatalog{}, err
	}
	if envelope.TenantID != tenantID ||
		envelope.Revision != head.Revision ||
		envelope.GraphVersion != head.GraphVersion ||
		catalog.TenantID != tenantID ||
		catalog.TenantGeneration != head.TenantGeneration ||
		catalog.Revision != head.Revision ||
		catalog.GraphVersion != head.GraphVersion ||
		catalog.DefinitionRevision != head.DefinitionRevision ||
		catalog.EmbeddingProfile != head.EmbeddingProfile ||
		catalog.EmbeddingGeneration != head.EmbeddingGeneration ||
		catalog.EmbeddingDimensions != head.EmbeddingDimensions {
		return RetrievalCatalog{}, fmt.Errorf(
			"retrieval head and catalog identity mismatch",
		)
	}
	return catalog, nil
}

func (s *TenantStore) ResolveRetrievalSnapshot(
	ctx context.Context,
	tenantID string,
	minVersion int64,
) (retrieval.Snapshot, error) {
	return s.loadRetrievalSnapshot(ctx, tenantID, minVersion)
}

func (s *TenantStore) resolveRetrievalSnapshotUncached(
	ctx context.Context,
	tenantID string,
) (retrieval.Snapshot, error) {
	head, _, err := s.getRetrievalHeadWithMeta(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return retrieval.Snapshot{}, fmt.Errorf(
			"%w: tenant %q has no published retrieval head",
			retrieval.ErrNotReady,
			tenantID,
		)
	}
	if err != nil {
		return retrieval.Snapshot{}, retrievalSnapshotResolutionError(
			"load retrieval head",
			err,
		)
	}
	return s.resolveRetrievalSnapshotAtHead(ctx, tenantID, head)
}

func (s *TenantStore) resolveRetrievalSnapshotAtHead(
	ctx context.Context,
	tenantID string,
	head RetrievalHead,
) (retrieval.Snapshot, error) {
	state, err := s.retrievalTenantState(ctx, tenantID)
	if err != nil {
		return retrieval.Snapshot{}, retrievalSnapshotResolutionError(
			"load tenant state",
			err,
		)
	}
	if !retrievalGenerationMatches(
		head.TenantGeneration,
		state.generation,
	) ||
		head.GraphVersion > state.graphVersion {
		return retrieval.Snapshot{}, fmt.Errorf(
			"%w: retrieval head belongs to a stale tenant generation",
			retrieval.ErrNotReady,
		)
	}
	catalog, err := s.getRetrievalCatalogAtHead(ctx, tenantID, head)
	if err != nil {
		return retrieval.Snapshot{}, fmt.Errorf(
			"%w: load retrieval catalog: %w",
			retrieval.ErrNotReady,
			err,
		)
	}
	definitions, _, err := s.getRetrievalDefinitionsWithMeta(
		ctx,
		tenantID,
	)
	if err != nil {
		return retrieval.Snapshot{}, retrievalSnapshotResolutionError(
			"load retrieval definitions",
			err,
		)
	}
	if !retrievalGenerationMatches(
		definitions.TenantGeneration,
		state.generation,
	) ||
		definitions.Revision != catalog.DefinitionRevision ||
		!retrievalDefinitionProfileEnabled(
			definitions,
			catalog.EmbeddingProfile,
		) {
		return retrieval.Snapshot{}, fmt.Errorf(
			"%w: retrieval definitions no longer match the published catalog",
			retrieval.ErrNotReady,
		)
	}
	return retrieval.Snapshot{
		TenantGeneration:    head.TenantGeneration,
		Revision:            head.Revision,
		GraphVersion:        head.GraphVersion,
		DefinitionRevision:  head.DefinitionRevision,
		CatalogKey:          head.CatalogKey,
		CatalogHash:         head.CatalogHash,
		EmbeddingProfile:    head.EmbeddingProfile,
		EmbeddingGeneration: head.EmbeddingGeneration,
		EmbeddingDimensions: head.EmbeddingDimensions,
	}, nil
}

func (s *TenantStore) verifyRetrievalCatalogArtifacts(
	ctx context.Context,
	tenantID string,
	catalog RetrievalCatalog,
) error {
	versionPrefix := s.parquetVersionPrefix(
		tenantID,
		catalog.GraphVersion,
	) + "/"
	if path.Clean(catalog.IndexCatalogKey) != catalog.IndexCatalogKey ||
		!strings.HasPrefix(catalog.IndexCatalogKey, versionPrefix) {
		return fmt.Errorf(
			"retrieval graph index key %q is not an immutable version %d object",
			catalog.IndexCatalogKey,
			catalog.GraphVersion,
		)
	}
	indexData, err := s.Objects.Get(ctx, catalog.IndexCatalogKey)
	if err != nil {
		return fmt.Errorf("load retrieval graph index catalog: %w", err)
	}
	if objectContentHash(indexData) != catalog.IndexCatalogHash {
		return fmt.Errorf("retrieval graph index catalog hash mismatch")
	}
	indexCatalog, err := decodeIndexCatalogObject(ctx, indexData)
	if err != nil {
		return fmt.Errorf("decode retrieval graph index catalog: %w", err)
	}
	if indexCatalog.TenantID != "" && indexCatalog.TenantID != tenantID {
		return fmt.Errorf("retrieval graph index catalog tenant mismatch")
	}
	if indexCatalog.Version != catalog.GraphVersion {
		return fmt.Errorf(
			"retrieval graph index catalog version %d, want %d",
			indexCatalog.Version,
			catalog.GraphVersion,
		)
	}
	segmentPrefix := s.retrievalGenerationPrefix(
		tenantID,
		catalog.EmbeddingGeneration,
		catalog.GraphVersion,
	) + "/"
	for _, segment := range catalog.Segments {
		if path.Clean(segment.Key) != segment.Key ||
			!strings.HasPrefix(segment.Key, segmentPrefix) ||
			path.Ext(segment.Key) != ".parquet" {
			return fmt.Errorf(
				"retrieval segment key %q is outside generation %q version %d",
				segment.Key,
				catalog.EmbeddingGeneration,
				catalog.GraphVersion,
			)
		}
		data, err := s.Objects.Get(ctx, segment.Key)
		if err != nil {
			return fmt.Errorf(
				"load retrieval segment %q: %w",
				segment.Key,
				err,
			)
		}
		if !isParquetBytes(data) ||
			objectContentHash(data) != segment.ContentHash {
			return fmt.Errorf(
				"retrieval segment %q content mismatch",
				segment.Key,
			)
		}
	}
	return nil
}

func (s *TenantStore) putImmutableRetrievalObject(
	ctx context.Context,
	tenantID string,
	key string,
	data []byte,
) error {
	if _, err := s.putTenantConditional(
		ctx,
		tenantID,
		key,
		data,
		PutCondition{IfNoneMatch: true},
	); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf(
			"%w: retrieval object %q already exists with different content",
			ErrConflict,
			key,
		)
	}
	return nil
}

type retrievalTenantStateSnapshot struct {
	generation   int64
	graphVersion int64
}

func (s *TenantStore) retrievalTenantState(
	ctx context.Context,
	tenantID string,
) (retrievalTenantStateSnapshot, error) {
	if s.coordinated() {
		head, exists, err := s.Coordinator.Head(ctx, tenantID)
		if err != nil {
			return retrievalTenantStateSnapshot{}, err
		}
		if !exists {
			return retrievalTenantStateSnapshot{}, ErrCoordinatorHeadMissing
		}
		if head.Status != TenantStatusActive {
			return retrievalTenantStateSnapshot{}, ErrTenantDeleted
		}
		return retrievalTenantStateSnapshot{
			generation:   head.Generation,
			graphVersion: head.GraphVersion,
		}, nil
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return retrievalTenantStateSnapshot{}, err
	}
	manifest, meta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return retrievalTenantStateSnapshot{}, err
	}
	if !meta.Exists {
		return retrievalTenantStateSnapshot{}, ErrNotFound
	}
	return retrievalTenantStateSnapshot{
		graphVersion: manifest.Version,
	}, nil
}

func retrievalGenerationMatches(stored int64, current int64) bool {
	return current == 0 || stored == current
}

func retrievalDefinitionProfileEnabled(
	record RetrievalDefinitionRecord,
	profile string,
) bool {
	for _, definition := range record.Definitions {
		if definition.Enabled &&
			definition.EmbeddingProfile == profile {
			return true
		}
	}
	return false
}

func retrievalSnapshotResolutionError(stage string, err error) error {
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", retrieval.ErrNotReady, stage, err)
}
