package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
)

const coordinatedTenantCandidateLayoutVersion = 1

type coordinatedTenantCandidate struct {
	LayoutVersion  int    `json:"layout_version"`
	Operation      string `json:"operation"`
	SourceTenantID string `json:"source_tenant_id"`
	SourcePrefix   string `json:"source_prefix"`
	TargetTenantID string `json:"target_tenant_id"`
}

func newCoordinatedTenantCandidate(
	operation string,
	sourceTenantID string,
	sourcePrefix string,
	targetTenantID string,
) coordinatedTenantCandidate {
	return coordinatedTenantCandidate{
		LayoutVersion:  coordinatedTenantCandidateLayoutVersion,
		Operation:      operation,
		SourceTenantID: sourceTenantID,
		SourcePrefix:   sourcePrefix,
		TargetTenantID: targetTenantID,
	}
}

func (s *TenantStore) coordinatedTenantCandidateKey(tenantID string) string {
	return path.Join(
		s.Prefix, "tenants", tenantID, "coordination", "lifecycle-candidate.json",
	)
}

func (s *TenantStore) prepareCoordinatedTenantCandidate(
	ctx context.Context,
	tenantID string,
	candidate coordinatedTenantCandidate,
) (bool, error) {
	current, exists, _, err := s.getCoordinatedTenantCandidate(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if exists {
		if current == candidate {
			return true, nil
		}
		return false, fmt.Errorf(
			"%w: target tenant %q has a different lifecycle candidate",
			ErrConflict, tenantID,
		)
	}
	dataExists, err := s.tenantDataExists(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if dataExists {
		return false, fmt.Errorf(
			"%w: target tenant %q has unowned data",
			ErrCoordinatorHeadMissing, tenantID,
		)
	}
	data, err := json.Marshal(candidate)
	if err != nil {
		return false, err
	}
	key := s.coordinatedTenantCandidateKey(tenantID)
	if _, err := s.Objects.PutConditional(
		ctx, key, data, PutCondition{IfNoneMatch: true},
	); err == nil {
		return false, nil
	} else if !errors.Is(err, ErrConflict) {
		return false, err
	}
	current, exists, _, err = s.getCoordinatedTenantCandidate(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if exists && current == candidate {
		return true, nil
	}
	return false, fmt.Errorf(
		"%w: target tenant %q lifecycle candidate changed",
		ErrConflict, tenantID,
	)
}

func (s *TenantStore) getCoordinatedTenantCandidate(
	ctx context.Context,
	tenantID string,
) (coordinatedTenantCandidate, bool, ObjectMeta, error) {
	key := s.coordinatedTenantCandidateKey(tenantID)
	s.clearCoordinatedWriterObjectKey(key)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return coordinatedTenantCandidate{}, false, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return coordinatedTenantCandidate{}, false, ObjectMeta{Key: key}, err
	}
	var candidate coordinatedTenantCandidate
	if err := json.Unmarshal(data, &candidate); err != nil {
		return coordinatedTenantCandidate{}, false, meta, err
	}
	if candidate.LayoutVersion != coordinatedTenantCandidateLayoutVersion ||
		candidate.TargetTenantID != tenantID {
		return coordinatedTenantCandidate{}, false, meta, fmt.Errorf(
			"invalid lifecycle candidate for tenant %q", tenantID,
		)
	}
	return candidate, true, meta, nil
}

func (s *TenantStore) putCoordinatedCandidateObject(
	ctx context.Context,
	key string,
	data []byte,
) error {
	if _, err := s.Objects.PutConditional(
		ctx, key, data, PutCondition{IfNoneMatch: true},
	); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return err
	}
	if bytes.Equal(existing, data) {
		return nil
	}
	if meta.ETag == "" {
		return fmt.Errorf("%w: candidate object %q has no ETag", ErrConflict, key)
	}
	_, err = s.Objects.PutConditional(
		ctx, key, data, PutCondition{IfMatch: meta.ETag},
	)
	return err
}

func (s *TenantStore) completeCoordinatedTenantCandidate(
	ctx context.Context,
	tenantID string,
	expected coordinatedTenantCandidate,
) error {
	current, exists, meta, err := s.getCoordinatedTenantCandidate(ctx, tenantID)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if current != expected || meta.ETag == "" {
		return fmt.Errorf(
			"%w: target tenant %q lifecycle candidate changed",
			ErrConflict, tenantID,
		)
	}
	err = s.Objects.DeleteConditional(
		ctx,
		s.coordinatedTenantCandidateKey(tenantID),
		PutCondition{IfMatch: meta.ETag},
	)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}
