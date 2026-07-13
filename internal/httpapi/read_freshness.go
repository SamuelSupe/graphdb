package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"

	"go.opentelemetry.io/otel/attribute"
)

const (
	headerReadMinVersion  = "X-GraphDB-Min-Version"
	headerReadAllowStale  = "X-GraphDB-Allow-Stale"
	defaultCatchupTimeout = 2 * time.Second
	defaultReadRetryAfter = time.Second
	unconstrainedVersion  = int64(^uint64(0) >> 1)
)

type readFreshness struct {
	MinVersion int64
	AllowStale bool
}

type readTarget struct {
	ManifestVersion int64
	TargetVersion   int64
	AllowStale      bool
}

type readerNotFreshError struct {
	VisibleVersion  int64
	RequiredVersion int64
	RetryAfter      time.Duration
	Reason          string
	Err             error
}

func (e *readerNotFreshError) Error() string {
	if e.RequiredVersion > 0 && e.VisibleVersion >= e.RequiredVersion {
		return fmt.Sprintf("reader not fresh: required version %d is not available in reader cache", e.RequiredVersion)
	}
	return fmt.Sprintf("reader not fresh: visible version %d is below required version %d", e.VisibleVersion, e.RequiredVersion)
}

func (e *readerNotFreshError) Unwrap() error {
	return e.Err
}

func (s *Server) readTarget(r *http.Request, tenantID string, body readFreshness) (target readTarget, err error) {
	ctx, span := startAPIPhase(r.Context(), "read_target",
		attribute.String("graphdb.tenant", tenantID),
		attribute.Int64("graphdb.read.body_min_version", body.MinVersion),
		attribute.Bool("graphdb.read.body_allow_stale", body.AllowStale),
	)
	r = r.WithContext(ctx)
	defer func() {
		if span != nil {
			span.SetAttributes(
				attribute.Int64("graphdb.read.manifest_version", target.ManifestVersion),
				attribute.Int64("graphdb.read.target_version", target.TargetVersion),
				attribute.Bool("graphdb.read.allow_stale", target.AllowStale),
			)
		}
		endHTTPSpan(span, err)
	}()

	freshness, err := parseReadFreshness(r, body)
	if err != nil {
		return readTarget{}, err
	}
	if freshness.AllowStale && freshness.MinVersion == 0 {
		return readTarget{ManifestVersion: unconstrainedVersion, AllowStale: true}, nil
	}
	manifest, err := s.currentQueryManifest(r.Context(), tenantID)
	if err != nil {
		return readTarget{}, err
	}
	if freshness.MinVersion > manifest.Version {
		return readTarget{}, s.readerNotFresh(tenantID, manifest.Version, freshness.MinVersion, "manifest_behind", nil)
	}
	targetVersion := manifest.Version
	if freshness.AllowStale {
		targetVersion = freshness.MinVersion
	}
	return readTarget{ManifestVersion: manifest.Version, TargetVersion: targetVersion, AllowStale: freshness.AllowStale}, nil
}

func parseReadFreshness(r *http.Request, body readFreshness) (readFreshness, error) {
	freshness := body
	if raw := strings.TrimSpace(firstQuery(r.Header.Get(headerReadMinVersion), r.URL.Query().Get("min_version"))); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			return readFreshness{}, fmt.Errorf("min_version must be a non-negative integer")
		}
		freshness.MinVersion = value
	}
	if raw := strings.TrimSpace(firstQuery(r.Header.Get(headerReadAllowStale), r.URL.Query().Get("allow_stale"))); raw != "" {
		value, err := parseReadBool(raw)
		if err != nil {
			return readFreshness{}, err
		}
		freshness.AllowStale = value
	}
	return freshness, nil
}

func hasExplicitReadMinVersion(r *http.Request) bool {
	return strings.TrimSpace(firstQuery(r.Header.Get(headerReadMinVersion), r.URL.Query().Get("min_version"))) != ""
}

func parseReadBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true, nil
	case "0", "false", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("allow_stale must be a boolean")
	}
}

func (target readTarget) requiresVersion(version int64) bool {
	return target.TargetVersion == 0 || version >= target.TargetVersion
}

func (s *Server) loadGraphForRead(ctx context.Context, tenantID string, target readTarget) (g *graph.Graph, manifest storage.Manifest, err error) {
	ctx, span := startAPIPhase(ctx, "load_graph",
		attribute.String("graphdb.tenant", tenantID),
		attribute.Int64("graphdb.read.target_version", target.TargetVersion),
		attribute.Bool("graphdb.read.allow_stale", target.AllowStale),
		attribute.Bool("graphdb.reader_cache.configured", s.Cache != nil),
	)
	defer func() {
		if span != nil {
			span.SetAttributes(attribute.Int64("graphdb.read.visible_version", manifest.Version))
		}
		endHTTPSpan(span, err)
	}()

	loadCtx := ctx
	cancel := func() {}
	catchupStart := time.Time{}
	if target.TargetVersion > 0 {
		loadCtx, cancel = context.WithTimeout(ctx, s.readerCatchupTimeout())
		catchupStart = time.Now()
	}
	defer cancel()
	g, manifest, err = s.loadGraphAtLeast(loadCtx, tenantID, target.TargetVersion)
	if err != nil {
		if target.TargetVersion > 0 && readCatchupFailed(loadCtx, err) {
			if cached, cachedManifest, ok, cacheErr := s.cachedGraphAtLeast(tenantID, target.TargetVersion); cacheErr != nil {
				return nil, storage.Manifest{}, cacheErr
			} else if ok {
				s.recordReaderCatchup(tenantID, "ok", catchupStart)
				s.recordReaderVisible(tenantID, cachedManifest.Version)
				return cached, cachedManifest, nil
			}
			reason := readCatchupReason(loadCtx, err)
			s.recordReaderCatchup(tenantID, reason, catchupStart)
			visible := visibleVersionFromError(err, s.visibleVersion(tenantID, target))
			return nil, storage.Manifest{}, s.readerNotFresh(tenantID, visible, target.TargetVersion, reason, err)
		}
		if target.TargetVersion > 0 {
			s.recordReaderCatchup(tenantID, "error", catchupStart)
		}
		return nil, storage.Manifest{}, err
	}
	if target.TargetVersion > 0 && manifest.Version < target.TargetVersion {
		s.recordReaderCatchup(tenantID, "version_lag", catchupStart)
		return nil, storage.Manifest{}, s.readerNotFresh(tenantID, manifest.Version, target.TargetVersion, "version_lag", nil)
	}
	if target.TargetVersion > 0 {
		s.recordReaderCatchup(tenantID, "ok", catchupStart)
	}
	s.recordReaderVisible(tenantID, manifest.Version)
	return g, manifest, nil
}

func (s *Server) recordReaderCatchup(tenantID string, status string, start time.Time) {
	if start.IsZero() {
		return
	}
	s.obs().Metrics.RecordReaderCatchup(tenantID, status, time.Since(start))
}

func (s *Server) recordReaderVisible(tenantID string, version int64) {
	if version > 0 {
		s.obs().Metrics.RecordReaderVisibleVersion(tenantID, version)
	}
}

func readCatchupFailed(ctx context.Context, err error) bool {
	return errors.Is(ctx.Err(), context.DeadlineExceeded) || strings.Contains(err.Error(), "below required version")
}

func readCatchupReason(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "catchup_timeout"
	}
	if strings.Contains(err.Error(), "below required version") {
		return "version_lag"
	}
	return "catchup_failed"
}

func visibleVersionFromError(err error, fallback int64) int64 {
	var visible, required int64
	if _, scanErr := fmt.Sscanf(err.Error(), "loaded graph version %d is below required version %d", &visible, &required); scanErr == nil {
		return visible
	}
	return fallback
}

func (s *Server) visibleVersion(tenantID string, target readTarget) int64 {
	if s.Cache == nil {
		return 0
	}
	if cached, ok := s.Cache.CachedVersion(tenantID); ok {
		return cached
	}
	return 0
}

func (s *Server) cachedGraphAtLeast(tenantID string, minVersion int64) (*graph.Graph, storage.Manifest, bool, error) {
	if s.Cache == nil {
		return nil, storage.Manifest{}, false, nil
	}
	return s.Cache.CachedAtLeast(tenantID, minVersion)
}

func (s *Server) readerCatchupTimeout() time.Duration {
	if s.ReaderCatchupTimeout > 0 {
		return s.ReaderCatchupTimeout
	}
	return defaultCatchupTimeout
}

func (s *Server) readerNotFresh(tenantID string, visible int64, required int64, reason string, err error) error {
	s.obs().Metrics.RecordReaderNotFresh(tenantID, reason)
	return &readerNotFreshError{
		VisibleVersion:  visible,
		RequiredVersion: required,
		RetryAfter:      defaultReadRetryAfter,
		Reason:          reason,
		Err:             err,
	}
}

func asReaderNotFresh(err error) (*readerNotFreshError, bool) {
	var target *readerNotFreshError
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

func writeReadError(w http.ResponseWriter, err error) {
	if writeReaderNotFresh(w, err) {
		return
	}
	writeErrorErr(w, http.StatusBadRequest, err)
}

func writeReaderNotFresh(w http.ResponseWriter, err error) bool {
	freshness, ok := asReaderNotFresh(err)
	if !ok {
		return false
	}
	retryAfter := int((freshness.RetryAfter + time.Second - 1) / time.Second)
	if retryAfter < 1 {
		retryAfter = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeErrorDetail(w, http.StatusServiceUnavailable, ErrorCodeReaderNotFresh, err.Error(), true, freshness.detail())
	return true
}

func (e *readerNotFreshError) detail() map[string]any {
	return map[string]any{
		"visible_version":  e.VisibleVersion,
		"required_version": e.RequiredVersion,
		"retry_after_ms":   e.RetryAfter.Milliseconds(),
		"reason":           e.Reason,
	}
}
