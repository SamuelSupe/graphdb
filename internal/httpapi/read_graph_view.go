package httpapi

import (
	"context"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) withReadOnlyGraphForRead(ctx context.Context, tenantID string, target readTarget, fn func(*graph.Graph, storage.Manifest) error) error {
	if s.Cache == nil {
		g, manifest, err := s.loadGraphForRead(ctx, tenantID, target)
		if err != nil {
			return err
		}
		return fn(g, manifest)
	}
	loadCtx := ctx
	cancel := func() {}
	catchupStart := time.Time{}
	if target.TargetVersion > 0 {
		loadCtx, cancel = context.WithTimeout(ctx, s.readerCatchupTimeout())
		catchupStart = time.Now()
	}
	defer cancel()
	var callbackErr error
	err := s.Cache.WithReadOnlyGraphAtLeast(loadCtx, tenantID, target.TargetVersion, func(g *graph.Graph, manifest storage.Manifest) error {
		if target.TargetVersion > 0 && manifest.Version < target.TargetVersion {
			callbackErr = s.readerNotFresh(tenantID, manifest.Version, target.TargetVersion, "version_lag", nil)
			return nil
		}
		if target.TargetVersion > 0 {
			s.recordReaderCatchup(tenantID, "ok", catchupStart)
		}
		s.recordReaderVisible(tenantID, manifest.Version)
		callbackErr = fn(g, manifest)
		return nil
	})
	if err == nil {
		return callbackErr
	}
	if target.TargetVersion <= 0 || !readCatchupFailed(loadCtx, err) {
		if target.TargetVersion > 0 {
			s.recordReaderCatchup(tenantID, "error", catchupStart)
		}
		return err
	}
	if cached, manifest, ok, cacheErr := s.cachedGraphAtLeast(tenantID, target.TargetVersion); cacheErr != nil {
		return cacheErr
	} else if ok {
		s.recordReaderCatchup(tenantID, "ok", catchupStart)
		s.recordReaderVisible(tenantID, manifest.Version)
		return fn(cached, manifest)
	}
	reason := readCatchupReason(loadCtx, err)
	s.recordReaderCatchup(tenantID, reason, catchupStart)
	visible := visibleVersionFromError(err, s.visibleVersion(tenantID, target))
	return s.readerNotFresh(tenantID, visible, target.TargetVersion, reason, err)
}
