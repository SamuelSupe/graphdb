package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (s *Server) listEntities(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	options := entityScanOptions(r)
	enforceTarget := options.Cursor == "" || hasExplicitReadMinVersion(r)
	if enforceTarget {
		options.MinVersion = target.TargetVersion
	}
	result, err := s.listEntitiesPage(r.Context(), tenantID, options, target)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if enforceTarget && !target.requiresVersion(result.Version) {
		writeReadError(w, s.readerNotFresh(tenantID, result.Version, target.TargetVersion, "version_lag", nil))
		return
	}
	s.recordReaderVisible(tenantID, result.Version)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) streamEntities(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	options := entityScanOptions(r)
	enforceTarget := options.Cursor == "" || hasExplicitReadMinVersion(r)
	if enforceTarget {
		options.MinVersion = target.TargetVersion
	}
	if options.Cursor == "" {
		if catalog, err := s.Store.CurrentScanCatalog(r.Context(), tenantID); err == nil && target.requiresVersion(catalog.Version) {
			s.recordReaderVisible(tenantID, catalog.Version)
			s.streamEntitiesFromCatalog(w, r, tenantID, catalog, options)
			return
		}
	}
	if !enforceTarget {
		options.MinVersion = 0
	}
	options.Limit = scanStreamPageSize
	result, err := s.listEntitiesPage(r.Context(), tenantID, options, target)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if enforceTarget && !target.requiresVersion(result.Version) {
		writeReadError(w, s.readerNotFresh(tenantID, result.Version, target.TargetVersion, "version_lag", nil))
		return
	}
	s.recordReaderVisible(tenantID, result.Version)
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	headerSent := false
	for {
		if !headerSent {
			headerSent = true
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "entity", "tenant_id": tenantID, "version": result.Version,
			}, flush))
		}
		for _, entity := range result.Entities {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"entity": entity.JSONValue()}, flush))
		}
		if result.NextCursor == "" {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": result.Version}, flush))
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.listEntitiesPage(r.Context(), tenantID, options, target)
		if err != nil {
			abortScanWrite(encodeStreamError(w, encoder, readStreamError(err)))
			return
		}
	}
}

func readStreamError(err error) StreamErrorResponse {
	if freshness, ok := asReaderNotFresh(err); ok {
		return streamErrorResponse(buildErrorResponse(ErrorCodeReaderNotFresh, err.Error(), true, freshness.detail()))
	}
	if errors.Is(err, context.Canceled) {
		return streamErrorResponse(errorResponseFor(statusClientClosedRequest, err, "", nil))
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return streamErrorResponse(errorResponseFor(http.StatusGatewayTimeout, err, "", nil))
	}
	if errors.Is(err, storage.ErrReaderLoadBusy) {
		return streamErrorResponse(buildErrorResponse(ErrorCodeTooManyRequests, err.Error(), true, nil))
	}
	response := errorResponseFor(http.StatusInternalServerError, err, "", nil)
	if response.Code == ErrorCodeInternal {
		response = buildErrorResponse(ErrorCodeInternal, "internal storage error", false, nil)
	}
	return streamErrorResponse(response)
}

func abortScanWrite(err error) {
	if err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) streamEntitiesFromCatalog(w http.ResponseWriter, r *http.Request, tenantID string, catalog storage.IndexCatalog, options storage.EntityScanOptions) {
	options.Limit = scanStreamPageSize
	result, err := s.Store.ListEntitiesFromCatalog(r.Context(), tenantID, catalog, options)
	if err != nil {
		writeReadError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	for {
		if options.Cursor == "" {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "entity", "tenant_id": tenantID, "version": catalog.Version,
			}, flush))
		}
		for _, entity := range result.Entities {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"entity": entity.JSONValue()}, flush))
		}
		if result.NextCursor == "" {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version}, flush))
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.Store.ListEntitiesFromCatalog(r.Context(), tenantID, catalog, options)
		if err != nil {
			abortScanWrite(encodeStreamError(w, encoder, readStreamError(err)))
			return
		}
	}
}

func (s *Server) listEdges(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	options := edgeScanOptions(r)
	enforceTarget := options.Cursor == "" || hasExplicitReadMinVersion(r)
	if enforceTarget {
		options.MinVersion = target.TargetVersion
	}
	result, err := s.listEdgesPage(r.Context(), tenantID, options, target)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if enforceTarget && !target.requiresVersion(result.Version) {
		writeReadError(w, s.readerNotFresh(tenantID, result.Version, target.TargetVersion, "version_lag", nil))
		return
	}
	s.recordReaderVisible(tenantID, result.Version)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) streamEdges(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	options := edgeScanOptions(r)
	enforceTarget := options.Cursor == "" || hasExplicitReadMinVersion(r)
	if enforceTarget {
		options.MinVersion = target.TargetVersion
	}
	if options.Cursor == "" {
		if catalog, err := s.Store.CurrentScanCatalog(r.Context(), tenantID); err == nil && target.requiresVersion(catalog.Version) {
			s.recordReaderVisible(tenantID, catalog.Version)
			s.streamEdgesFromCatalog(w, r, tenantID, catalog, options)
			return
		}
	}
	if !enforceTarget {
		options.MinVersion = 0
	}
	options.Limit = scanStreamPageSize
	result, err := s.listEdgesPage(r.Context(), tenantID, options, target)
	if err != nil {
		writeReadError(w, err)
		return
	}
	if enforceTarget && !target.requiresVersion(result.Version) {
		writeReadError(w, s.readerNotFresh(tenantID, result.Version, target.TargetVersion, "version_lag", nil))
		return
	}
	s.recordReaderVisible(tenantID, result.Version)
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	headerSent := false
	for {
		if !headerSent {
			headerSent = true
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "edge", "tenant_id": tenantID, "version": result.Version,
			}, flush))
		}
		for _, edge := range result.Edges {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"edge": edge}, flush))
		}
		if result.NextCursor == "" {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": result.Version}, flush))
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.listEdgesPage(r.Context(), tenantID, options, target)
		if err != nil {
			abortScanWrite(encodeStreamError(w, encoder, readStreamError(err)))
			return
		}
	}
}

func (s *Server) streamEdgesFromCatalog(w http.ResponseWriter, r *http.Request, tenantID string, catalog storage.IndexCatalog, options storage.EdgeScanOptions) {
	options.Limit = scanStreamPageSize
	result, err := s.Store.ListEdgesFromCatalog(r.Context(), tenantID, catalog, options)
	if err != nil {
		writeReadError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	for {
		if options.Cursor == "" {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "edge", "tenant_id": tenantID, "version": catalog.Version,
			}, flush))
		}
		for _, edge := range result.Edges {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"edge": edge}, flush))
		}
		if result.NextCursor == "" {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version}, flush))
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.Store.ListEdgesFromCatalog(r.Context(), tenantID, catalog, options)
		if err != nil {
			abortScanWrite(encodeStreamError(w, encoder, readStreamError(err)))
			return
		}
	}
}

func (s *Server) listEntitiesPage(ctx context.Context, tenantID string, options storage.EntityScanOptions, target readTarget) (result storage.EntityScanResult, err error) {
	binding, err := s.Store.ValidateScanCursor(ctx, tenantID, options.Cursor)
	if err != nil {
		return result, err
	}
	defer func() {
		if err == nil {
			result.NextCursor, err = binding.PinGeneration(result.NextCursor)
		}
	}()
	if options.Cursor == "" {
		var result storage.EntityScanResult
		used, err := s.withCachedScanGraph(ctx, tenantID, options.MinVersion, func(g *graph.Graph, manifest storage.Manifest, binding storage.ScanCursorBinding) error {
			var err error
			result, err = s.Cache.ListEntitiesFromReadView(ctx, tenantID, g, manifest, options)
			if err == nil {
				result.NextCursor, err = binding.Pin(result.NextCursor)
			}
			return err
		})
		if used || err != nil {
			return result, err
		}
	}
	indexedOptions := options
	indexedOptions.SkipGraphFallback = s.Cache != nil
	result, err = s.Store.ListEntities(ctx, tenantID, indexedOptions)
	if !errors.Is(err, storage.ErrGraphScanFallbackRequired) || s.Cache == nil {
		return result, err
	}
	target.TargetVersion = options.MinVersion
	err = s.withReadOnlyGraphForRead(ctx, tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		result, err = s.Cache.ListEntitiesFromReadView(ctx, tenantID, g, manifest, options)
		return err
	})
	return result, err
}

func (s *Server) listEdgesPage(ctx context.Context, tenantID string, options storage.EdgeScanOptions, target readTarget) (result storage.EdgeScanResult, err error) {
	binding, err := s.Store.ValidateScanCursor(ctx, tenantID, options.Cursor)
	if err != nil {
		return result, err
	}
	defer func() {
		if err == nil {
			result.NextCursor, err = binding.PinGeneration(result.NextCursor)
		}
	}()
	if options.Cursor == "" {
		var result storage.EdgeScanResult
		used, err := s.withCachedScanGraph(ctx, tenantID, options.MinVersion, func(g *graph.Graph, manifest storage.Manifest, binding storage.ScanCursorBinding) error {
			var err error
			result, err = storage.ListEdgesFromGraph(ctx, tenantID, g, manifest, options)
			if err == nil {
				result.NextCursor, err = binding.Pin(result.NextCursor)
			}
			return err
		})
		if used || err != nil {
			return result, err
		}
	}
	indexedOptions := options
	indexedOptions.SkipGraphFallback = s.Cache != nil
	result, err = s.Store.ListEdges(ctx, tenantID, indexedOptions)
	if !errors.Is(err, storage.ErrGraphScanFallbackRequired) || s.Cache == nil {
		return result, err
	}
	target.TargetVersion = options.MinVersion
	err = s.withReadOnlyGraphForRead(ctx, tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		result, err = storage.ListEdgesFromGraph(ctx, tenantID, g, manifest, options)
		return err
	})
	return result, err
}

func (s *Server) exportSnapshot(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	started := false
	err = s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		if err := r.Context().Err(); err != nil {
			return err
		}
		started = true
		return writeGraphSnapshotJSON(w, r.Context(), tenantID, manifest.Version, g)
	})
	if err != nil {
		if started {
			panic(http.ErrAbortHandler)
		}
		writeReadError(w, err)
	}
}

func (s *Server) streamSnapshot(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	release, ok := s.enterRead(w, r, tenantID)
	if !ok {
		return
	}
	defer release()
	target, err := s.readTarget(r, tenantID, readFreshness{})
	if err != nil {
		writeReadError(w, err)
		return
	}
	if !parseBoolQuery(r, "inline") {
		if catalog, _, err := s.Store.CurrentShardedSnapshotCatalog(r.Context(), tenantID); err == nil {
			if target.requiresVersion(catalog.Version) {
				s.recordReaderVisible(tenantID, catalog.Version)
				s.streamSnapshotRefs(w, r, tenantID, catalog)
				return
			}
		}
	}
	if s.Mode == "all" && s.Cache != nil {
		used, err := s.Cache.WithCachedReadOnlyGraph(r.Context(), tenantID, target.TargetVersion, func(g *graph.Graph, manifest storage.Manifest) error {
			s.recordReaderVisible(tenantID, manifest.Version)
			s.streamGraphSnapshot(w, r, tenantID, g, manifest.Version)
			return nil
		})
		if err != nil {
			writeReadError(w, err)
			return
		}
		if used {
			return
		}
	}
	if catalog, err := s.Store.CurrentScanCatalog(r.Context(), tenantID); err == nil {
		if target.requiresVersion(catalog.Version) {
			s.recordReaderVisible(tenantID, catalog.Version)
			s.streamIndexedSnapshot(w, r, tenantID, catalog)
			return
		}
	}
	s.streamLoadedSnapshot(w, r, tenantID, target)
}

func (s *Server) streamSnapshotRefs(w http.ResponseWriter, r *http.Request, tenantID string, catalog storage.ShardedSnapshotCatalog) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{
		"stream": "snapshot", "tenant_id": tenantID, "version": catalog.Version, "mode": "refs", "sharded": true,
	}, flush))
	abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"schema": catalog.Schema}, flush))
	for _, page := range catalog.EntityPages {
		abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"entity_page": page}, flush))
	}
	for _, shard := range catalog.EdgeShards {
		abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"edge_shard": shard}, flush))
	}
	abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version, "mode": "refs", "sharded": true}, flush))
}

func (s *Server) streamIndexedSnapshot(w http.ResponseWriter, r *http.Request, tenantID string, catalog storage.IndexCatalog) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{
		"stream": "snapshot", "tenant_id": tenantID, "version": catalog.Version, "indexed_read": true,
	}, flush))
	if schemaGraph, _, err := s.Store.ReadCurrentSchema(r.Context(), tenantID); err == nil && schemaGraph != nil {
		for _, ciType := range schemaGraph.ListCITypes() {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"ci_type": ciType}, flush))
		}
		for _, relType := range schemaGraph.ListRelationTypes() {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"relation_type": relType}, flush))
		}
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		abortScanWrite(encodeStreamError(w, encoder, readStreamError(err)))
		return
	}
	entityOptions := storage.EntityScanOptions{Limit: scanStreamPageSize}
	for {
		result, err := s.Store.ListEntitiesFromCatalog(r.Context(), tenantID, catalog, entityOptions)
		if err != nil {
			abortScanWrite(encodeStreamError(w, encoder, readStreamError(err)))
			return
		}
		for _, entity := range result.Entities {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"entity": entity.JSONValue()}, flush))
		}
		if result.NextCursor == "" {
			break
		}
		entityOptions.Cursor = result.NextCursor
	}
	edgeOptions := storage.EdgeScanOptions{Limit: scanStreamPageSize}
	for {
		result, err := s.Store.ListEdgesFromCatalog(r.Context(), tenantID, catalog, edgeOptions)
		if err != nil {
			abortScanWrite(encodeStreamError(w, encoder, readStreamError(err)))
			return
		}
		for _, edge := range result.Edges {
			abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"edge": edge}, flush))
		}
		if result.NextCursor == "" {
			break
		}
		edgeOptions.Cursor = result.NextCursor
	}
	abortScanWrite(encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version, "indexed_read": true}, flush))
}

func (s *Server) streamLoadedSnapshot(w http.ResponseWriter, r *http.Request, tenantID string, target readTarget) {
	err := s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		s.streamGraphSnapshot(w, r, tenantID, g, manifest.Version)
		return nil
	})
	if err != nil {
		writeReadError(w, err)
	}
}

const scanStreamPageSize = 500

func entityScanOptions(r *http.Request) storage.EntityScanOptions {
	query := r.URL.Query()
	return storage.EntityScanOptions{
		Kind:   query.Get("kind"),
		Source: query.Get("source"),
		Shard:  firstQuery(query.Get("shard"), query.Get("entity_shard")),
		Limit:  parsePositiveInt(query.Get("limit")),
		Cursor: query.Get("cursor"),
	}
}

func edgeScanOptions(r *http.Request) storage.EdgeScanOptions {
	query := r.URL.Query()
	return storage.EdgeScanOptions{
		Type:      firstQuery(query.Get("type"), query.Get("relation_type")),
		From:      query.Get("from"),
		FromShard: firstQuery(query.Get("from_shard"), query.Get("shard")),
		Source:    query.Get("source"),
		Limit:     parsePositiveInt(query.Get("limit")),
		Cursor:    query.Get("cursor"),
	}
}

func parsePositiveInt(raw string) int {
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func firstQuery(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func parseBoolQuery(r *http.Request, key string) bool {
	value := r.URL.Query().Get(key)
	switch value {
	case "1", "true", "TRUE", "True", "yes", "on":
		return true
	default:
		return false
	}
}
