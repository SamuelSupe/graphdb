package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type IndexDefinition struct {
	Name      string    `json:"name"`
	Kind      string    `json:"kind"`
	Field     string    `json:"field"`
	Unique    bool      `json:"unique,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type IndexDefinitionRecord struct {
	LayoutVersion int               `json:"layout_version,omitempty"`
	TenantID      string            `json:"tenant_id,omitempty"`
	Indexes       []IndexDefinition `json:"indexes,omitempty"`
}

type IndexDefinitionResult struct {
	Definition IndexDefinition `json:"definition"`
	Task       IndexTask       `json:"task,omitempty"`
}

func (s *TenantStore) ListIndexDefinitions(ctx context.Context, tenantID string) ([]IndexDefinition, error) {
	record, _, err := s.getIndexDefinitionsWithMeta(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return append([]IndexDefinition(nil), record.Indexes...), nil
}

func (s *TenantStore) CreateIndex(ctx context.Context, tenantID string, definition IndexDefinition) (IndexDefinitionResult, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexDefinitionResult{}, err
	}
	normalized, err := normalizeIndexDefinition(definition)
	if err != nil {
		return IndexDefinitionResult{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return IndexDefinitionResult{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return IndexDefinitionResult{}, err
	}
	record, meta, err := s.getIndexDefinitionsWithMeta(ctx, tenantID)
	if err != nil {
		return IndexDefinitionResult{}, err
	}
	now := time.Now().UTC()
	normalized.CreatedAt = now
	normalized.UpdatedAt = now
	for _, existing := range record.Indexes {
		if existing.Name == normalized.Name {
			return IndexDefinitionResult{}, fmt.Errorf("index %q already exists", normalized.Name)
		}
	}
	record.TenantID = tenantID
	record.Indexes = append(record.Indexes, normalized)
	sortIndexDefinitions(record.Indexes)
	if err := s.putIndexDefinitionsWithMeta(ctx, tenantID, record, meta); err != nil {
		return IndexDefinitionResult{}, err
	}
	task, err := s.startIndexRebuildAfterDefinitionChangeLocked(ctx, tenantID)
	if err != nil {
		return IndexDefinitionResult{}, err
	}
	return IndexDefinitionResult{Definition: normalized, Task: task}, nil
}

func (s *TenantStore) DropIndex(ctx context.Context, tenantID string, name string) (IndexDefinitionResult, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexDefinitionResult{}, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return IndexDefinitionResult{}, fmt.Errorf("index name is required")
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return IndexDefinitionResult{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return IndexDefinitionResult{}, err
	}
	record, meta, err := s.getIndexDefinitionsWithMeta(ctx, tenantID)
	if err != nil {
		return IndexDefinitionResult{}, err
	}
	next := record.Indexes[:0]
	var removed IndexDefinition
	for _, definition := range record.Indexes {
		if definition.Name == name {
			removed = definition
			continue
		}
		next = append(next, definition)
	}
	if removed.Name == "" {
		return IndexDefinitionResult{}, fmt.Errorf("index %q not found", name)
	}
	record.TenantID = tenantID
	record.Indexes = next
	if err := s.putIndexDefinitionsWithMeta(ctx, tenantID, record, meta); err != nil {
		return IndexDefinitionResult{}, err
	}
	task, err := s.startIndexRebuildAfterDefinitionChangeLocked(ctx, tenantID)
	if err != nil {
		return IndexDefinitionResult{}, err
	}
	return IndexDefinitionResult{Definition: removed, Task: task}, nil
}

func (s *TenantStore) getIndexDefinitions(ctx context.Context, tenantID string) ([]IndexDefinition, error) {
	record, _, err := s.getIndexDefinitionsWithMeta(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return record.Indexes, nil
}

func (s *TenantStore) getIndexDefinitionsWithMeta(ctx context.Context, tenantID string) (IndexDefinitionRecord, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexDefinitionRecord{}, ObjectMeta{}, err
	}
	key := s.indexDefinitionsKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return IndexDefinitionRecord{TenantID: tenantID}, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return IndexDefinitionRecord{}, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return IndexDefinitionRecord{}, ObjectMeta{}, fmt.Errorf("unsupported index definitions: only parquet definitions are readable")
	}
	record, err := decodeParquetIndexDefinitions(ctx, data)
	if err != nil {
		return IndexDefinitionRecord{}, ObjectMeta{}, err
	}
	if record.TenantID != "" && record.TenantID != tenantID {
		return IndexDefinitionRecord{}, ObjectMeta{}, fmt.Errorf("index definitions tenant mismatch: path tenant %q contains tenant %q", tenantID, record.TenantID)
	}
	record.TenantID = tenantID
	for i := range record.Indexes {
		normalized, err := normalizeIndexDefinition(record.Indexes[i])
		if err != nil {
			return IndexDefinitionRecord{}, ObjectMeta{}, err
		}
		normalized.CreatedAt = record.Indexes[i].CreatedAt
		normalized.UpdatedAt = record.Indexes[i].UpdatedAt
		record.Indexes[i] = normalized
	}
	sortIndexDefinitions(record.Indexes)
	return record, meta, nil
}

func (s *TenantStore) putIndexDefinitionsWithMeta(ctx context.Context, tenantID string, record IndexDefinitionRecord, meta ObjectMeta) error {
	record.TenantID = tenantID
	key := s.indexDefinitionsKey(tenantID)
	data, err := marshalParquetIndexDefinitions(ctx, record)
	if err != nil {
		return err
	}
	writeMeta := meta
	if writeMeta.Key != key {
		writeMeta = ObjectMeta{Key: key}
	}
	return s.putBytesWithMeta(ctx, key, data, writeMeta)
}

func normalizeIndexDefinition(definition IndexDefinition) (IndexDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.Kind = strings.TrimSpace(definition.Kind)
	definition.Field = strings.TrimSpace(definition.Field)
	if definition.Kind == "" || definition.Field == "" {
		return IndexDefinition{}, fmt.Errorf("index kind and field are required")
	}
	if definition.Name == "" {
		definition.Name = definition.Kind + "." + definition.Field
	}
	if strings.Contains(definition.Name, "/") || strings.Contains(definition.Name, "..") {
		return IndexDefinition{}, fmt.Errorf("invalid index name %q", definition.Name)
	}
	return definition, nil
}

func sortIndexDefinitions(definitions []IndexDefinition) {
	for i := 1; i < len(definitions); i++ {
		value := definitions[i]
		j := i - 1
		for j >= 0 && definitions[j].Name > value.Name {
			definitions[j+1] = definitions[j]
			j--
		}
		definitions[j+1] = value
	}
}
