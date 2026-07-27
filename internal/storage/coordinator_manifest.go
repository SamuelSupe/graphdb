package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
)

const coordinatorETagPrefix = "coord-revision-"

type coordinatedHeadToken struct {
	Revision        int64
	Generation      int64
	ContextRevision int64
}

func (s *TenantStore) getCoordinatedManifest(ctx context.Context, tenantID string) (Manifest, ObjectMeta, error) {
	head, exists, err := s.Coordinator.Head(ctx, tenantID)
	if err != nil {
		return Manifest{}, ObjectMeta{}, err
	}
	if !exists {
		_, candidateExists, _, candidateErr :=
			s.getCoordinatedTenantCandidate(ctx, tenantID)
		if candidateErr != nil {
			return Manifest{}, ObjectMeta{}, candidateErr
		}
		if candidateExists {
			return Manifest{}, ObjectMeta{}, fmt.Errorf(
				"%w: tenant %q has an unfinished lifecycle candidate",
				ErrConflict, tenantID,
			)
		}
		_, legacyMeta, legacyErr := s.Objects.GetWithMeta(ctx, s.manifestKey(tenantID))
		switch {
		case legacyErr == nil && legacyMeta.Exists:
			return Manifest{}, ObjectMeta{}, fmt.Errorf("%w: tenant %q must be bootstrapped", ErrCoordinatorHeadMissing, tenantID)
		case !errors.Is(legacyErr, ErrNotFound):
			return Manifest{}, ObjectMeta{}, legacyErr
		default:
			return Manifest{TenantID: tenantID}, ObjectMeta{Key: s.manifestKey(tenantID)}, nil
		}
	}
	return s.getCoordinatedManifestAtHead(ctx, tenantID, head)
}

func (s *TenantStore) getCoordinatedManifestAtHead(
	ctx context.Context,
	tenantID string,
	head CoordinationHead,
) (Manifest, ObjectMeta, error) {
	data, err := s.Objects.Get(ctx, head.ManifestKey)
	if err != nil {
		return Manifest{}, ObjectMeta{}, fmt.Errorf("load coordinator manifest %q: %w", head.ManifestKey, err)
	}
	if got := objectContentHash(data); got != head.ManifestHash {
		return Manifest{}, ObjectMeta{}, fmt.Errorf("coordinator manifest hash mismatch: got %s want %s", got, head.ManifestHash)
	}
	if !isParquetBytes(data) {
		return Manifest{}, ObjectMeta{}, fmt.Errorf("unsupported coordinator manifest: only parquet manifests are readable")
	}
	manifest, err := decodeParquetManifest(ctx, data)
	if err != nil {
		return Manifest{}, ObjectMeta{}, err
	}
	if manifest.TenantID != tenantID {
		return Manifest{}, ObjectMeta{}, fmt.Errorf("manifest tenant mismatch: key tenant %q contains tenant %q", tenantID, manifest.TenantID)
	}
	if manifest.Version != head.GraphVersion {
		return Manifest{}, ObjectMeta{}, fmt.Errorf("coordinator graph version %d does not match manifest version %d", head.GraphVersion, manifest.Version)
	}
	return manifest, coordinatedManifestMeta(head.ManifestKey, head), nil
}

func (s *TenantStore) putCoordinatedManifest(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	meta ObjectMeta,
	reservation *directCommitReservation,
	activationContext *WriteContextSnapshot,
) (ObjectMeta, error) {
	expected, err := parseCoordinatedHeadToken(meta)
	if err != nil {
		return ObjectMeta{}, err
	}
	if expected.Revision == 0 &&
		!coordinatorLeaseContextMatches(
			ctx, tenantID, coordinatorLifecycleTaskType,
		) {
		publicationCtx, stop, publicationErr :=
			s.startCoordinatedHeadPublication(ctx, tenantID)
		if publicationErr != nil {
			return ObjectMeta{}, publicationErr
		}
		defer stop()
		ctx = publicationCtx
	}
	var activationHead CoordinationHead
	activate := false
	if expected.Revision == 0 {
		current, exists, headErr := s.Coordinator.Head(ctx, tenantID)
		if headErr != nil {
			return ObjectMeta{}, headErr
		}
		if exists {
			switch current.Status {
			case TenantStatusDeleted:
				activationHead = current
				activate = true
			case TenantStatusDisabled:
				return ObjectMeta{}, ErrTenantDisabled
			default:
				return ObjectMeta{}, fmt.Errorf("%w: manifest for tenant %q changed while publishing", ErrConflict, tenantID)
			}
		}
	}

	manifest.TenantID = tenantID
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		return ObjectMeta{}, err
	}
	hash := objectContentHash(data)
	nextRevision := expected.Revision + 1
	if activate {
		nextRevision = activationHead.Revision + 1
	}
	key := s.coordinatorManifestKey(tenantID, manifest.Version, nextRevision, hash)
	if err := s.putImmutableCoordinatorObject(ctx, key, data); err != nil {
		return ObjectMeta{}, err
	}

	request := HeadPublishRequest{
		TenantID:                     tenantID,
		ExpectedRevision:             expected.Revision,
		ExpectedGeneration:           expected.Generation,
		ExpectedWriteContextRevision: expected.ContextRevision,
		GraphVersion:                 manifest.Version,
		ManifestKey:                  key,
		ManifestHash:                 hash,
		CommitID:                     manifest.HeadCommitID,
	}
	if activationContext != nil {
		if expected.Revision != 0 {
			return ObjectMeta{}, fmt.Errorf(
				"activation write-context requires a new or deleted tenant head",
			)
		}
		key, contextHash, contextErr := s.putCoordinatedWriteContextSnapshot(
			ctx, tenantID, 1, *activationContext,
		)
		if contextErr != nil {
			return ObjectMeta{}, contextErr
		}
		request.InitialWriteContextKey = key
		request.InitialWriteContextHash = contextHash
	}
	if activate {
		request.ExpectedGeneration = activationHead.Generation
	}
	if reservation != nil && reservation.coordinated {
		if err := attachCoordinatorCommitMetadata(
			&request, reservation, reservation.record.Result, manifest.Version,
		); err != nil {
			return ObjectMeta{}, err
		}
	}
	var next CoordinationHead
	var published bool
	if activate {
		next, published, err = s.Coordinator.ActivateTenantHead(ctx, request)
	} else {
		next, published, err = s.Coordinator.PublishHead(ctx, request)
	}
	if err != nil {
		s.observeCoordinatorCAS(tenantID, "error", 0)
		return ObjectMeta{}, err
	}
	if !published {
		s.observeCoordinatorCAS(tenantID, "conflict", 0)
		s.recordManifestCASConflict(tenantID)
		return ObjectMeta{}, fmt.Errorf("%w: manifest for tenant %q changed while publishing", ErrConflict, tenantID)
	}
	s.observeCoordinatorCAS(tenantID, "committed", next.Revision)
	return coordinatedManifestMeta(next.ManifestKey, next), nil
}

func (s *TenantStore) observeCoordinatorCAS(tenantID string, status string, revision int64) {
	if s.CoordinatorObserver == nil {
		return
	}
	s.CoordinatorObserver.RecordCoordinatorCAS(tenantID, status)
	if revision > 0 {
		s.CoordinatorObserver.RecordCoordinatorHeadRevision(tenantID, revision)
	}
}

func (s *TenantStore) coordinatorManifestKey(tenantID string, graphVersion, revision int64, hash string) string {
	if len(hash) > 16 {
		hash = hash[:16]
	}
	name := fmt.Sprintf("%020d-%020d-%s.parquet", graphVersion, revision, hash)
	return path.Join(s.Prefix, "tenants", tenantID, "coordination", "manifests", name)
}

func (s *TenantStore) coordinatorManifestPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "coordination", "manifests") + "/"
}

func (s *TenantStore) putImmutableCoordinatorObject(ctx context.Context, key string, data []byte) error {
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("%w: coordinator object %q already exists with different content", ErrConflict, key)
	}
	return nil
}

func coordinatedManifestMeta(key string, head CoordinationHead) ObjectMeta {
	return ObjectMeta{
		Key: key,
		ETag: coordinatorETagPrefix + strconv.FormatInt(head.Revision, 10) +
			"-generation-" + strconv.FormatInt(head.Generation, 10) +
			"-context-" + strconv.FormatInt(head.WriteContextRevision, 10),
		Exists: head.Revision > 0,
	}
}

func manifestMetaMatchesCoordinatorHead(
	manifest Manifest,
	meta ObjectMeta,
	head CoordinationHead,
) bool {
	token, err := parseCoordinatedHeadToken(meta)
	return err == nil &&
		token.Revision == head.Revision &&
		token.Generation == head.Generation &&
		token.ContextRevision == head.WriteContextRevision &&
		meta.Key == head.ManifestKey &&
		manifest.Version == head.GraphVersion
}

func parseCoordinatedRevision(meta ObjectMeta) (int64, error) {
	token, err := parseCoordinatedHeadToken(meta)
	return token.Revision, err
}

func parseCoordinatedHeadToken(meta ObjectMeta) (coordinatedHeadToken, error) {
	if !meta.Exists {
		return coordinatedHeadToken{}, nil
	}
	if len(meta.ETag) <= len(coordinatorETagPrefix) || meta.ETag[:len(coordinatorETagPrefix)] != coordinatorETagPrefix {
		return coordinatedHeadToken{}, fmt.Errorf("invalid coordinator manifest revision %q", meta.ETag)
	}
	parts := strings.Split(meta.ETag[len(coordinatorETagPrefix):], "-")
	if len(parts) != 5 || parts[1] != "generation" || parts[3] != "context" {
		return coordinatedHeadToken{}, fmt.Errorf("invalid coordinator manifest revision %q", meta.ETag)
	}
	revision, revisionErr := strconv.ParseInt(parts[0], 10, 64)
	generation, generationErr := strconv.ParseInt(parts[2], 10, 64)
	contextRevision, contextErr := strconv.ParseInt(parts[4], 10, 64)
	if revisionErr != nil || generationErr != nil || contextErr != nil ||
		revision <= 0 || generation <= 0 || contextRevision < 0 {
		return coordinatedHeadToken{}, fmt.Errorf("invalid coordinator manifest revision %q", meta.ETag)
	}
	return coordinatedHeadToken{
		Revision: revision, Generation: generation, ContextRevision: contextRevision,
	}, nil
}
