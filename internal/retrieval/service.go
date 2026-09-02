package retrieval

import (
	"context"
	"fmt"
)

type Snapshot struct {
	TenantGeneration    int64
	Revision            int64
	GraphVersion        int64
	DefinitionRevision  int64
	CatalogKey          string
	CatalogHash         string
	EmbeddingProfile    string
	EmbeddingGeneration string
	EmbeddingDimensions int
}

type SnapshotResolver interface {
	ResolveRetrievalSnapshot(context.Context, string, int64) (Snapshot, error)
}

type SnapshotEngine interface {
	SearchSnapshot(context.Context, string, Snapshot, SearchRequest) (SearchResponse, error)
}

type Service struct {
	Resolver SnapshotResolver
	Engine   SnapshotEngine
}

func NewService(resolver SnapshotResolver, engine SnapshotEngine) *Service {
	return &Service{Resolver: resolver, Engine: engine}
}

func (s *Service) SearchEvidence(
	ctx context.Context,
	tenantID string,
	request SearchRequest,
) (SearchResponse, error) {
	normalized, err := NormalizeRequest(request)
	if err != nil {
		return SearchResponse{}, err
	}
	if s == nil || s.Resolver == nil {
		return SearchResponse{}, ErrNotReady
	}
	snapshot, err := s.Resolver.ResolveRetrievalSnapshot(
		ctx,
		tenantID,
		normalized.MinVersion,
	)
	if err != nil {
		return SearchResponse{}, err
	}
	if snapshot.Revision <= 0 ||
		snapshot.GraphVersion < 0 ||
		snapshot.CatalogKey == "" ||
		snapshot.CatalogHash == "" ||
		snapshot.EmbeddingGeneration == "" {
		return SearchResponse{}, fmt.Errorf(
			"%w: published retrieval snapshot is incomplete",
			ErrNotReady,
		)
	}
	if snapshot.GraphVersion < normalized.MinVersion {
		return SearchResponse{}, &NotFreshError{
			VisibleVersion:  snapshot.GraphVersion,
			RequiredVersion: normalized.MinVersion,
		}
	}
	if s.Engine == nil {
		return SearchResponse{}, fmt.Errorf(
			"%w: retrieval snapshot executor is not configured",
			ErrNotReady,
		)
	}
	response, err := s.Engine.SearchSnapshot(
		ctx,
		tenantID,
		snapshot,
		normalized,
	)
	if err != nil {
		return SearchResponse{}, err
	}
	response.Version = snapshot.GraphVersion
	response.RetrievalRevision = snapshot.Revision
	response.EmbeddingGeneration = snapshot.EmbeddingGeneration
	if response.Evidence == nil {
		response.Evidence = []Evidence{}
	}
	response.Stats.Returned = len(response.Evidence)
	return response, nil
}
