package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const writeContextLayoutVersion = 1

type WriteContextSnapshot struct {
	LayoutVersion          int                   `json:"layout_version"`
	TenantID               string                `json:"tenant_id"`
	Revision               int64                 `json:"revision"`
	SourcePolicy           graph.SourcePolicy    `json:"source_policy,omitempty"`
	SourcePolicyConfigured bool                  `json:"source_policy_configured,omitempty"`
	TenantConfig           TenantConfig          `json:"tenant_config,omitempty"`
	TenantConfigConfigured bool                  `json:"tenant_config_configured,omitempty"`
	RelationSchemas        RelationSchemaCatalog `json:"relation_schemas"`
	UpdatedAt              time.Time             `json:"updated_at"`
}

func emptyWriteContext(tenantID string) WriteContextSnapshot {
	return WriteContextSnapshot{
		LayoutVersion:   writeContextLayoutVersion,
		TenantID:        tenantID,
		RelationSchemas: emptyRelationSchemaCatalog(tenantID),
	}
}

func (s *TenantStore) loadCoordinatedWriteContext(
	ctx context.Context,
	tenantID string,
) (WriteContextSnapshot, CoordinationHead, error) {
	if memo := coordinatedWriteContextMemoFrom(ctx); memo != nil {
		return memo.load(ctx, s, tenantID)
	}
	return s.loadCoordinatedWriteContextFresh(ctx, tenantID)
}

func (s *TenantStore) loadCoordinatedWriteContextFresh(
	ctx context.Context,
	tenantID string,
) (WriteContextSnapshot, CoordinationHead, error) {
	head, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return WriteContextSnapshot{}, CoordinationHead{}, err
	}
	if !exists {
		return emptyWriteContext(tenantID), CoordinationHead{}, nil
	}
	if head.WriteContextRevision == 0 {
		if head.WriteContextKey != "" || head.WriteContextHash != "" {
			return WriteContextSnapshot{}, CoordinationHead{}, fmt.Errorf("tenant %q has an invalid empty write-context head", tenantID)
		}
		return emptyWriteContext(tenantID), head, nil
	}
	if head.WriteContextKey == "" || head.WriteContextHash == "" {
		return WriteContextSnapshot{}, CoordinationHead{}, fmt.Errorf("tenant %q write-context object is missing", tenantID)
	}
	data, err := s.Objects.Get(ctx, head.WriteContextKey)
	if err != nil {
		return WriteContextSnapshot{}, CoordinationHead{}, fmt.Errorf("load write-context %q: %w", head.WriteContextKey, err)
	}
	if got := objectContentHash(data); got != head.WriteContextHash {
		return WriteContextSnapshot{}, CoordinationHead{}, fmt.Errorf(
			"write-context hash mismatch: got %s want %s", got, head.WriteContextHash,
		)
	}
	var snapshot WriteContextSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return WriteContextSnapshot{}, CoordinationHead{}, fmt.Errorf("decode write-context: %w", err)
	}
	if err := validateWriteContextSnapshot(snapshot, tenantID, head.WriteContextRevision); err != nil {
		return WriteContextSnapshot{}, CoordinationHead{}, err
	}
	return snapshot, head, nil
}

func validateWriteContextSnapshot(snapshot WriteContextSnapshot, tenantID string, revision int64) error {
	if snapshot.LayoutVersion != writeContextLayoutVersion {
		return fmt.Errorf("unsupported write-context layout version %d", snapshot.LayoutVersion)
	}
	if snapshot.TenantID != tenantID {
		return fmt.Errorf("write-context tenant mismatch: path tenant %q contains tenant %q", tenantID, snapshot.TenantID)
	}
	if snapshot.Revision != revision {
		return fmt.Errorf("write-context revision mismatch: head %d object %d", revision, snapshot.Revision)
	}
	if snapshot.SourcePolicyConfigured {
		if _, err := graph.NormalizeSourcePolicy(snapshot.SourcePolicy); err != nil {
			return err
		}
	}
	if snapshot.TenantConfigConfigured {
		if err := validateTenantConfig(snapshot.TenantConfig); err != nil {
			return err
		}
	}
	_, err := normalizeRelationSchemaCatalog(snapshot.RelationSchemas)
	return err
}

func (s *TenantStore) publishCoordinatedWriteContext(
	ctx context.Context,
	head CoordinationHead,
	snapshot WriteContextSnapshot,
) (CoordinationHead, bool, error) {
	key, hash, err := s.putCoordinatedWriteContextSnapshot(
		ctx, head.TenantID, head.WriteContextRevision+1, snapshot,
	)
	if err != nil {
		return CoordinationHead{}, false, err
	}
	return s.Coordinator.PublishWriteContext(ctx, WriteContextPublishRequest{
		TenantID:           head.TenantID,
		ExpectedRevision:   head.Revision,
		ExpectedGeneration: head.Generation,
		ExpectedContext:    head.WriteContextRevision,
		WriteContextKey:    key,
		WriteContextHash:   hash,
	})
}

func (s *TenantStore) putCoordinatedWriteContextSnapshot(
	ctx context.Context,
	tenantID string,
	revision int64,
	snapshot WriteContextSnapshot,
) (string, string, error) {
	snapshot.LayoutVersion = writeContextLayoutVersion
	snapshot.TenantID = tenantID
	snapshot.Revision = revision
	snapshot.UpdatedAt = time.Now().UTC()
	snapshot.RelationSchemas.LayoutVersion = relationSchemaLayoutVersion
	snapshot.RelationSchemas.TenantID = tenantID
	normalized, err := normalizeRelationSchemaCatalog(snapshot.RelationSchemas)
	if err != nil {
		return "", "", err
	}
	snapshot.RelationSchemas = normalized
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", "", err
	}
	hash := objectContentHash(data)
	key := s.coordinatorWriteContextKey(tenantID, snapshot.Revision, hash)
	if err := s.putImmutableCoordinatorObject(ctx, key, data); err != nil {
		return "", "", err
	}
	return key, hash, nil
}

func (s *TenantStore) coordinatorWriteContextKey(tenantID string, revision int64, hash string) string {
	if len(hash) > 16 {
		hash = hash[:16]
	}
	name := fmt.Sprintf("%020d-%s.json", revision, hash)
	return path.Join(s.Prefix, "tenants", tenantID, "coordination", "write-contexts", name)
}

func (s *TenantStore) coordinatorWriteContextPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "coordination", "write-contexts") + "/"
}

func (s *TenantStore) ensureCoordinatedTenantHead(ctx context.Context, tenantID string) (CoordinationHead, error) {
	head, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return head, err
	}
	if exists && head.Status == TenantStatusActive {
		return head, nil
	}
	if exists && head.Status == TenantStatusDisabled {
		return CoordinationHead{}, ErrTenantDisabled
	}
	if exists && head.Status == TenantStatusDeleted {
		return CoordinationHead{}, ErrTenantDeleted
	}
	return s.publishEmptyCoordinatedTenantHead(ctx, tenantID)
}

func (s *TenantStore) ensureCoordinatedTenantHeadForCreate(
	ctx context.Context,
	tenantID string,
) (CoordinationHead, error) {
	head, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return CoordinationHead{}, err
	}
	if exists && head.Status == TenantStatusActive {
		return head, nil
	}
	if exists && head.Status == TenantStatusDisabled {
		return CoordinationHead{}, ErrTenantDisabled
	}
	if exists {
		residual, err := s.tenantResidualObjectsExist(ctx, tenantID)
		if err != nil {
			return CoordinationHead{}, err
		}
		if residual {
			return CoordinationHead{}, ErrTenantDeleted
		}
	}
	return s.publishEmptyCoordinatedTenantHead(ctx, tenantID)
}

func (s *TenantStore) publishEmptyCoordinatedTenantHead(
	ctx context.Context,
	tenantID string,
) (CoordinationHead, error) {
	if _, exists, _, err := s.getCoordinatedTenantCandidate(
		ctx, tenantID,
	); err != nil {
		return CoordinationHead{}, err
	} else if exists {
		return CoordinationHead{}, fmt.Errorf(
			"%w: tenant %q has an unfinished lifecycle candidate",
			ErrConflict, tenantID,
		)
	}
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      tenantID,
		UpdatedAt:     time.Now().UTC(),
	}
	if _, dataMD5, _, emptyErr := newEmptyTenantGraph(); emptyErr == nil {
		manifest.DataMD5 = dataMD5
	}
	_, err := s.putCoordinatedManifest(
		ctx, tenantID, manifest, ObjectMeta{Key: s.manifestKey(tenantID)}, nil, nil,
	)
	if err != nil && !errors.Is(err, ErrConflict) {
		return CoordinationHead{}, err
	}
	head, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return CoordinationHead{}, err
	}
	if !exists {
		return CoordinationHead{}, ErrCoordinatorHeadMissing
	}
	return head, nil
}

func sameCoordinationPoint(head CoordinationHead, token coordinatedHeadToken) bool {
	return head.Revision == token.Revision &&
		head.Generation == token.Generation &&
		head.WriteContextRevision == token.ContextRevision
}

func (s *TenantStore) ensureCoordinationPointCurrent(
	ctx context.Context,
	tenantID string,
	meta ObjectMeta,
) error {
	if !s.coordinated() {
		return nil
	}
	token, err := parseCoordinatedHeadToken(meta)
	if err != nil {
		return err
	}
	head, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return err
	}
	if !exists || !sameCoordinationPoint(head, token) || head.Status != TenantStatusActive {
		return fmt.Errorf("%w: tenant %q coordination point changed", ErrConflict, tenantID)
	}
	return nil
}
