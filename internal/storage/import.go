package storage

import (
	"context"
	"fmt"
	"path"
	"strings"
)

const (
	TaskTypeBulkImport   = "bulk_import"
	defaultImportBatch   = 500
	maxImportBatch       = 5000
	maxImportSourceBytes = 32 << 20
)

type ImportOptions struct {
	Format      string `json:"format"`
	Source      string `json:"source,omitempty"`
	CollectorID string `json:"collector_id,omitempty"`
	BatchSize   int    `json:"batch_size,omitempty"`
	OnError     string `json:"on_error,omitempty"`
}

func (s *TenantStore) StartImport(ctx context.Context, tenantID string, data []byte, options ImportOptions) (Task, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Task{}, err
	}
	options, err := normalizeImportOptions(options)
	if err != nil {
		return Task{}, err
	}
	if len(data) == 0 {
		return Task{}, fmt.Errorf("import source must not be empty")
	}
	if len(data) > maxImportSourceBytes {
		return Task{}, fmt.Errorf("import source exceeds %d bytes", maxImportSourceBytes)
	}
	importID, err := newCommitID()
	if err != nil {
		return Task{}, err
	}
	key := s.importSourceKey(tenantID, importID, options.Format)
	if err := s.stageImportSource(ctx, tenantID, key, data); err != nil {
		return Task{}, err
	}
	params := map[string]any{
		"import_id":    importID,
		"source_key":   key,
		"format":       options.Format,
		"source":       options.Source,
		"collector_id": options.CollectorID,
		"batch_size":   options.BatchSize,
		"on_error":     options.OnError,
	}
	task, err := s.StartTask(ctx, tenantID, TaskTypeBulkImport, params)
	if err != nil {
		s.cleanupImportSource(ctx, key)
		return Task{}, err
	}
	if stringTaskParam(task.Params, "import_id") != importID {
		s.cleanupImportSource(ctx, key)
		return Task{}, fmt.Errorf("%w: another bulk import is already active", ErrConflict)
	}
	return task, nil
}

func (s *TenantStore) cleanupImportSource(ctx context.Context, key string) {
	cleanupCtx, cancel := s.taskFinalizationContext(ctx)
	defer cancel()
	_ = s.Objects.Delete(cleanupCtx, key)
}

func normalizeImportOptions(options ImportOptions) (ImportOptions, error) {
	options.Format = strings.ToLower(strings.TrimSpace(options.Format))
	if options.Format == "ndjson" {
		options.Format = "jsonl"
	}
	if options.Format != "jsonl" && options.Format != "csv" {
		return ImportOptions{}, fmt.Errorf("import format must be jsonl or csv")
	}
	options.Source = strings.TrimSpace(options.Source)
	if options.Source == "" {
		options.Source = "bulk_import"
	}
	options.CollectorID = strings.TrimSpace(options.CollectorID)
	if options.CollectorID == "" {
		options.CollectorID = "file"
	}
	if options.BatchSize == 0 {
		options.BatchSize = defaultImportBatch
	}
	if options.BatchSize < 1 || options.BatchSize > maxImportBatch {
		return ImportOptions{}, fmt.Errorf("import batch_size must be between 1 and %d", maxImportBatch)
	}
	options.OnError = strings.ToLower(strings.TrimSpace(options.OnError))
	if options.OnError == "" {
		options.OnError = "abort"
	}
	if options.OnError != "abort" && options.OnError != "continue" {
		return ImportOptions{}, fmt.Errorf("import on_error must be abort or continue")
	}
	return options, nil
}

func (s *TenantStore) stageImportSource(ctx context.Context, tenantID string, key string, data []byte) error {
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return err
	}
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return err
	}
	if err := s.EnsureTenantWritable(boundCtx, tenantID); err != nil {
		return err
	}
	if _, err := s.putTenantConditional(boundCtx, tenantID, key, data, PutCondition{IfNoneMatch: true}); err != nil {
		return err
	}
	s.markObjectKeyCached(key)
	return nil
}

func (s *TenantStore) importSourcePrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "tasks", "imports") + "/"
}

func (s *TenantStore) importSourceKey(tenantID string, importID string, format string) string {
	return path.Join(s.importSourcePrefix(tenantID), objectSegment(importID)+"."+format)
}

func (s *TenantStore) validateImportSourceKey(tenantID string, key string) error {
	if err := s.validateTenantObjectKey(tenantID, key); err != nil {
		return err
	}
	if !strings.HasPrefix(key, s.importSourcePrefix(tenantID)) {
		return fmt.Errorf("import source key %q is outside the import source prefix", key)
	}
	return nil
}
