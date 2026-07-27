package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

type DeadLetter struct {
	ID         string        `json:"id"`
	TenantID   string        `json:"tenant_id"`
	Source     string        `json:"source"`
	BatchID    string        `json:"batch_id"`
	Request    IngestRequest `json:"request"`
	LastResult IngestResult  `json:"last_result"`
	Attempts   int           `json:"attempts"`
	Status     string        `json:"status"`
	Error      string        `json:"error,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
	ReplayedAt time.Time     `json:"replayed_at,omitempty"`

	objectKey  string
	objectMeta ObjectMeta
}

const deadLetterReplayLease = 15 * time.Minute

type ReplayReport struct {
	Scanned    int              `json:"scanned"`
	Replayed   int              `json:"replayed"`
	Resolved   int              `json:"resolved"`
	Failed     int              `json:"failed"`
	Results    []IngestResult   `json:"results,omitempty"`
	Checkpoint ReplayCheckpoint `json:"checkpoint,omitempty"`
}

func (s *TenantStore) saveDeadLetter(ctx context.Context, tenantID string, request IngestRequest, result IngestResult) error {
	now := time.Now().UTC()
	failedRequest := deadLetterRequest(request, result)
	record := DeadLetter{
		ID:         deadLetterID(request),
		TenantID:   tenantID,
		Source:     request.Source,
		BatchID:    request.BatchID,
		Request:    failedRequest,
		LastResult: result,
		Attempts:   0,
		Status:     "pending",
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	key := s.deadLetterKey(tenantID, request.Source, record.ID)
	_, err := s.putDeadLetterWithMeta(ctx, tenantID, key, record, ObjectMeta{Key: key})
	return err
}

func (s *TenantStore) ensureDeadLetterAfterSkip(ctx context.Context, tenantID string, request IngestRequest, result IngestResult) error {
	key := s.deadLetterKey(
		tenantID,
		request.Source,
		deadLetterID(request),
	)
	s.clearCoordinatedWriterObjectKey(key)
	_, _, err := s.Objects.GetWithMeta(ctx, key)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	return s.saveDeadLetter(ctx, tenantID, request, result)
}

func deadLetterID(request IngestRequest) string {
	collectorID := strings.TrimSpace(request.CollectorID)
	if collectorID == "" {
		return request.BatchID
	}
	return collectorID + "/" + request.BatchID
}

func (s *TenantStore) ListDeadLetters(ctx context.Context, tenantID string, source string) ([]DeadLetter, error) {
	normalizedSource, err := normalizeDeadLetterScope(tenantID, source)
	if err != nil {
		return nil, err
	}
	source = normalizedSource
	items := make([]DeadLetter, 0)
	if err := s.scanDeadLetters(ctx, tenantID, source, func(item DeadLetter) error {
		items = append(items, item)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool {
		return deadLetterBefore(items[i], items[j])
	})
	return items, nil
}

func (s *TenantStore) ReplayDeadLetters(ctx context.Context, tenantID string, source string, limit int) (ReplayReport, error) {
	normalizedSource, err := normalizeDeadLetterScope(tenantID, source)
	if err != nil {
		return ReplayReport{}, err
	}
	source = normalizedSource
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return ReplayReport{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return ReplayReport{}, err
	}
	if limit < 0 {
		return ReplayReport{}, fmt.Errorf("limit must be a non-negative integer")
	}
	letters, err := s.deadLettersForReplay(ctx, tenantID, source, limit)
	if err != nil {
		return ReplayReport{}, err
	}
	report := ReplayReport{}
	for _, letter := range letters {
		if limit > 0 && report.Scanned >= limit {
			break
		}
		if letter.Status == "resolved" || letter.Status == "invalid" {
			continue
		}
		report.Scanned++
		result, replayed, err := s.replayDeadLetter(ctx, tenantID, source, letter)
		if replayed {
			recordReplayResult(&report, result)
		}
		if err != nil {
			return report, err
		}
		if !replayed {
			continue
		}
	}
	return report, nil
}

func recordReplayResult(report *ReplayReport, result IngestResult) {
	report.Replayed++
	report.Results = append(report.Results, result)
	if result.Failed == 0 {
		report.Resolved++
		return
	}
	report.Failed++
}

func (s *TenantStore) replayDeadLetter(ctx context.Context, tenantID string, source string, letter DeadLetter) (IngestResult, bool, error) {
	claimed, meta, ok, err := s.claimDeadLetterReplay(ctx, tenantID, source, letter)
	if err != nil || !ok {
		return IngestResult{}, ok, err
	}
	request := claimed.Request
	request.BatchID = request.BatchID + "-replay-" + time.Now().UTC().Format("20060102150405.000000000")
	request.IdempotencyKey = ""
	result, err := s.ingest(ctx, tenantID, request, false)
	claimed.LastResult = result
	claimed.UpdatedAt = time.Now().UTC()
	finalizeDeadLetterReplay(&claimed, result)
	if err != nil {
		claimed.Error = err.Error()
		_, saveErr := s.putDeadLetterWithMeta(ctx, tenantID, deadLetterObjectKey(s, tenantID, source, claimed), claimed, meta)
		return result, true, errors.Join(err, saveErr)
	}
	claimed.Error = ""
	_, err = s.putDeadLetterWithMeta(ctx, tenantID, deadLetterObjectKey(s, tenantID, source, claimed), claimed, meta)
	return result, true, err
}

func finalizeDeadLetterReplay(letter *DeadLetter, result IngestResult) {
	if result.Failed == 0 {
		letter.Status = "resolved"
		letter.ReplayedAt = letter.UpdatedAt
		return
	}
	letter.Status = "pending"
}

func (s *TenantStore) claimDeadLetterReplay(ctx context.Context, tenantID string, source string, letter DeadLetter) (DeadLetter, ObjectMeta, bool, error) {
	status := strings.TrimSpace(letter.Status)
	if status == "resolved" || status == "invalid" {
		return DeadLetter{}, ObjectMeta{}, false, nil
	}
	if status == "replaying" && time.Now().UTC().Before(letter.UpdatedAt.Add(deadLetterReplayLease)) {
		return DeadLetter{}, ObjectMeta{}, false, nil
	}
	key := deadLetterObjectKey(s, tenantID, source, letter)
	meta := letter.objectMeta
	if !meta.Exists || meta.ETag == "" {
		var current DeadLetter
		data, loadedMeta, err := s.Objects.GetWithMeta(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return DeadLetter{}, ObjectMeta{}, false, nil
		}
		if err != nil {
			return DeadLetter{}, ObjectMeta{}, false, err
		}
		if !isParquetBytes(data) {
			return DeadLetter{}, ObjectMeta{}, false, fmt.Errorf("unsupported deadletter object: only parquet deadletters are readable")
		}
		current, err = decodeParquetDeadLetter(ctx, data)
		if err != nil {
			return DeadLetter{}, ObjectMeta{}, false, err
		}
		if err := validateDeadLetterRecord(tenantID, source, current); err != nil {
			return DeadLetter{}, ObjectMeta{}, false, err
		}
		current.objectKey = key
		current.objectMeta = loadedMeta
		letter = current
		meta = loadedMeta
	}
	claimed := letter
	claimed.TenantID = tenantID
	if claimed.Source == "" {
		claimed.Source = source
	}
	claimed.Attempts++
	claimed.Status = "replaying"
	claimed.Error = ""
	claimed.UpdatedAt = time.Now().UTC()
	nextMeta, err := s.putDeadLetterWithMeta(ctx, tenantID, key, claimed, meta)
	if errors.Is(err, ErrConflict) {
		return DeadLetter{}, ObjectMeta{}, false, nil
	}
	if err != nil {
		return DeadLetter{}, ObjectMeta{}, false, err
	}
	claimed.objectKey = key
	claimed.objectMeta = nextMeta
	return claimed, nextMeta, true, nil
}

func deadLetterObjectKey(s *TenantStore, tenantID string, source string, letter DeadLetter) string {
	if letter.objectKey != "" {
		return letter.objectKey
	}
	return s.deadLetterKey(tenantID, source, letter.ID)
}

func (s *TenantStore) putDeadLetterWithMeta(ctx context.Context, tenantID string, key string, letter DeadLetter, meta ObjectMeta) (ObjectMeta, error) {
	data, err := marshalParquetDeadLetter(ctx, letter)
	if err != nil {
		return ObjectMeta{}, err
	}
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	return s.putTenantGenerationConditional(ctx, tenantID, key, data, condition)
}

func deadLetterRequest(request IngestRequest, result IngestResult) IngestRequest {
	if len(result.Failures) == 0 {
		return request
	}
	failed := map[int]struct{}{}
	for _, failure := range result.Failures {
		if failure.Index < 0 || failure.Index >= len(request.Items) {
			return request
		}
		failed[failure.Index] = struct{}{}
	}
	if len(failed) == 0 {
		return request
	}
	next := request
	next.Items = make([]IngestItem, 0, len(failed))
	for idx, item := range request.Items {
		if _, ok := failed[idx]; ok {
			next.Items = append(next.Items, item)
		}
	}
	return next
}

func (s *TenantStore) DeleteDeadLetter(ctx context.Context, tenantID string, source string, id string) error {
	normalizedSource, err := normalizeDeadLetterScope(tenantID, source)
	if err != nil {
		return err
	}
	source = normalizedSource
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return err
	}
	return s.deleteTenantObject(ctx, tenantID, s.deadLetterKey(tenantID, source, id))
}

func invalidDeadLetter(tenantID string, source string, key string, err error) DeadLetter {
	return DeadLetter{
		ID:       deadLetterIDFromKey(key),
		TenantID: tenantID,
		Source:   source,
		Status:   "invalid",
		Error:    err.Error(),
	}
}

func validateDeadLetterRecord(tenantID string, source string, item DeadLetter) error {
	if item.TenantID != "" && item.TenantID != tenantID {
		return fmt.Errorf("deadletter tenant mismatch: path tenant %q contains tenant %q", tenantID, item.TenantID)
	}
	if item.Source != "" && item.Source != source {
		return fmt.Errorf("deadletter source mismatch: path source %q contains source %q", source, item.Source)
	}
	if item.Request.Source != "" && item.Request.Source != source {
		return fmt.Errorf("deadletter request source mismatch: path source %q contains request source %q", source, item.Request.Source)
	}
	return nil
}

func deadLetterIDFromKey(key string) string {
	name := strings.TrimSuffix(path.Base(key), ".parquet")
	decoded, err := url.PathUnescape(name)
	if err != nil {
		return name
	}
	return decoded
}

func validateDeadLetterScope(tenantID string, source string) error {
	_, err := normalizeDeadLetterScope(tenantID, source)
	return err
}

func normalizeDeadLetterScope(tenantID string, source string) (string, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return "", err
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return "", fmt.Errorf("source is required")
	}
	return source, nil
}
