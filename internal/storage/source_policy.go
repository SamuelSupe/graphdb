package storage

import (
	"context"
	"errors"
	"fmt"

	"graphdb/internal/graph"
)

type sourcePolicyRecord struct {
	TenantID string `json:"tenant_id,omitempty"`
	graph.SourcePolicy
}

func (s *TenantStore) GetSourcePolicy(ctx context.Context, tenantID string) (graph.SourcePolicy, bool, error) {
	policy, configured, _, err := s.getSourcePolicyWithMeta(ctx, tenantID)
	return policy, configured, err
}

func (s *TenantStore) getSourcePolicyWithMeta(ctx context.Context, tenantID string) (graph.SourcePolicy, bool, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return graph.SourcePolicy{}, false, ObjectMeta{}, err
	}
	var record sourcePolicyRecord
	key := s.sourcePolicyKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return graph.SourcePolicy{}, false, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return graph.SourcePolicy{}, false, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return graph.SourcePolicy{}, false, ObjectMeta{}, fmt.Errorf("unsupported source policy: only parquet policies are readable")
	}
	record, err = decodeParquetSourcePolicy(ctx, data)
	if err != nil {
		return graph.SourcePolicy{}, false, ObjectMeta{}, err
	}
	if record.TenantID != "" && record.TenantID != tenantID {
		return graph.SourcePolicy{}, false, ObjectMeta{}, fmt.Errorf("source policy tenant mismatch: path tenant %q contains tenant %q", tenantID, record.TenantID)
	}
	normalized, err := graph.NormalizeSourcePolicy(record.SourcePolicy)
	if err != nil {
		return graph.SourcePolicy{}, false, ObjectMeta{}, err
	}
	return normalized, true, meta, nil
}

func (s *TenantStore) PutSourcePolicy(ctx context.Context, tenantID string, policy graph.SourcePolicy) (graph.SourcePolicy, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return graph.SourcePolicy{}, err
	}
	normalized, err := graph.NormalizeSourcePolicy(policy)
	if err != nil {
		return graph.SourcePolicy{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return graph.SourcePolicy{}, err
	}
	_, _, meta, err := s.getSourcePolicyWithMeta(ctx, tenantID)
	if err != nil {
		return graph.SourcePolicy{}, err
	}
	record := sourcePolicyRecord{TenantID: tenantID, SourcePolicy: normalized}
	if err := s.putSourcePolicyRecordWithMeta(ctx, tenantID, record, meta); err != nil {
		if errors.Is(err, ErrConflict) {
			return graph.SourcePolicy{}, fmt.Errorf("%w: source policy for tenant %q changed while publishing", ErrConflict, tenantID)
		}
		return graph.SourcePolicy{}, err
	}
	return normalized, nil
}

func (s *TenantStore) putSourcePolicyRecordWithMeta(ctx context.Context, tenantID string, record sourcePolicyRecord, meta ObjectMeta) error {
	record.TenantID = tenantID
	data, err := marshalParquetSourcePolicy(ctx, record)
	if err != nil {
		return err
	}
	return s.putBytesWithMeta(ctx, s.sourcePolicyKey(tenantID), data, meta)
}

func (s *TenantStore) resolveSourcePriorities(ctx context.Context, tenantID string, mutations graph.Mutations) (graph.Mutations, error) {
	policy, ok, err := s.GetSourcePolicy(ctx, tenantID)
	if err != nil {
		return graph.Mutations{}, err
	}
	if !ok {
		return mutations, nil
	}
	return graph.ApplySourcePolicy(mutations, policy), nil
}
