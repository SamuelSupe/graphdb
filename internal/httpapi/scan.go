package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
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
	result, err := s.Store.ListEntities(r.Context(), tenantID, options)
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
	result, err := s.Store.ListEntities(r.Context(), tenantID, options)
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
	for {
		if options.Cursor == "" {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "entity", "tenant_id": tenantID, "version": result.Version,
			}, flush); err != nil {
				return
			}
		}
		for _, entity := range result.Entities {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{"entity": entity}, flush); err != nil {
				return
			}
		}
		if result.NextCursor == "" {
			_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": result.Version}, flush)
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.Store.ListEntities(r.Context(), tenantID, options)
		if err != nil {
			_ = encodeStreamItem(r.Context(), encoder, streamErrorResponse(buildErrorResponse(ErrorCodeBadRequest, err.Error(), false, nil)), flush)
			return
		}
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
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "entity", "tenant_id": tenantID, "version": catalog.Version,
			}, flush); err != nil {
				return
			}
		}
		for _, entity := range result.Entities {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{"entity": entity}, flush); err != nil {
				return
			}
		}
		if result.NextCursor == "" {
			_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version}, flush)
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.Store.ListEntitiesFromCatalog(r.Context(), tenantID, catalog, options)
		if err != nil {
			_ = encodeStreamItem(r.Context(), encoder, streamErrorResponse(buildErrorResponse(ErrorCodeBadRequest, err.Error(), false, nil)), flush)
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
	result, err := s.Store.ListEdges(r.Context(), tenantID, options)
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
	result, err := s.Store.ListEdges(r.Context(), tenantID, options)
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
	for {
		if options.Cursor == "" {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "edge", "tenant_id": tenantID, "version": result.Version,
			}, flush); err != nil {
				return
			}
		}
		for _, edge := range result.Edges {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{"edge": edge}, flush); err != nil {
				return
			}
		}
		if result.NextCursor == "" {
			_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": result.Version}, flush)
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.Store.ListEdges(r.Context(), tenantID, options)
		if err != nil {
			_ = encodeStreamItem(r.Context(), encoder, streamErrorResponse(buildErrorResponse(ErrorCodeBadRequest, err.Error(), false, nil)), flush)
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
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{
				"stream": true, "resource": "edge", "tenant_id": tenantID, "version": catalog.Version,
			}, flush); err != nil {
				return
			}
		}
		for _, edge := range result.Edges {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{"edge": edge}, flush); err != nil {
				return
			}
		}
		if result.NextCursor == "" {
			_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version}, flush)
			return
		}
		options.Cursor = result.NextCursor
		result, err = s.Store.ListEdgesFromCatalog(r.Context(), tenantID, catalog, options)
		if err != nil {
			_ = encodeStreamItem(r.Context(), encoder, streamErrorResponse(buildErrorResponse(ErrorCodeBadRequest, err.Error(), false, nil)), flush)
			return
		}
	}
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
	var snapshot graph.Snapshot
	var version int64
	err = s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		snapshot = g.Snapshot()
		version = manifest.Version
		return nil
	})
	if err != nil {
		writeReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id": tenantID,
		"version":   version,
		"snapshot":  snapshot,
	})
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
	if err := encodeStreamItem(r.Context(), encoder, map[string]any{
		"stream": "snapshot", "tenant_id": tenantID, "version": catalog.Version, "mode": "refs", "sharded": true,
	}, flush); err != nil {
		return
	}
	if err := encodeStreamItem(r.Context(), encoder, map[string]any{"schema": catalog.Schema}, flush); err != nil {
		return
	}
	for _, page := range catalog.EntityPages {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"entity_page": page}, flush); err != nil {
			return
		}
	}
	for _, shard := range catalog.EdgeShards {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"edge_shard": shard}, flush); err != nil {
			return
		}
	}
	_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version, "mode": "refs", "sharded": true}, flush)
}

func (s *Server) streamIndexedSnapshot(w http.ResponseWriter, r *http.Request, tenantID string, catalog storage.IndexCatalog) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	if err := encodeStreamItem(r.Context(), encoder, map[string]any{
		"stream": "snapshot", "tenant_id": tenantID, "version": catalog.Version, "indexed_read": true,
	}, flush); err != nil {
		return
	}
	entityOptions := storage.EntityScanOptions{Limit: scanStreamPageSize}
	for {
		result, err := s.Store.ListEntitiesFromCatalog(r.Context(), tenantID, catalog, entityOptions)
		if err != nil {
			_ = encodeStreamItem(r.Context(), encoder, streamErrorResponse(buildErrorResponse(ErrorCodeBadRequest, err.Error(), false, nil)), flush)
			return
		}
		for _, entity := range result.Entities {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{"entity": entity}, flush); err != nil {
				return
			}
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
			_ = encodeStreamItem(r.Context(), encoder, streamErrorResponse(buildErrorResponse(ErrorCodeBadRequest, err.Error(), false, nil)), flush)
			return
		}
		for _, edge := range result.Edges {
			if err := encodeStreamItem(r.Context(), encoder, map[string]any{"edge": edge}, flush); err != nil {
				return
			}
		}
		if result.NextCursor == "" {
			break
		}
		edgeOptions.Cursor = result.NextCursor
	}
	_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": catalog.Version, "indexed_read": true}, flush)
}

func (s *Server) streamLoadedSnapshot(w http.ResponseWriter, r *http.Request, tenantID string, target readTarget) {
	var snapshot graph.Snapshot
	var version int64
	err := s.withReadOnlyGraphForRead(r.Context(), tenantID, target, func(g *graph.Graph, manifest storage.Manifest) error {
		snapshot = g.Snapshot()
		version = manifest.Version
		return nil
	})
	if err != nil {
		writeReadError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	flush := streamFlush(w)
	if err := encodeStreamItem(r.Context(), encoder, map[string]any{
		"stream": "snapshot", "tenant_id": tenantID, "version": version,
	}, flush); err != nil {
		return
	}
	for _, item := range snapshot.CITypes {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"ci_type": item}, flush); err != nil {
			return
		}
	}
	for _, item := range snapshot.RelationTypes {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"relation_type": item}, flush); err != nil {
			return
		}
	}
	for _, item := range snapshot.Entities {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"entity": item}, flush); err != nil {
			return
		}
	}
	for _, item := range snapshot.Edges {
		if err := encodeStreamItem(r.Context(), encoder, map[string]any{"edge": item}, flush); err != nil {
			return
		}
	}
	_ = encodeStreamItem(r.Context(), encoder, map[string]any{"done": true, "version": version}, flush)
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
