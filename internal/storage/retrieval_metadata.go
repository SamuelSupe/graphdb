package storage

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	RetrievalExtensionLayoutVersion = 1
	RetrievalSegmentChunks          = "chunks"
	RetrievalSegmentVector          = "vector"
	RetrievalSegmentLexical         = "lexical"
	maxRetrievalSegments            = 256
)

type RetrievalDefinition struct {
	Name             string    `json:"name"`
	Kinds            []string  `json:"kinds,omitempty"`
	TextFields       []string  `json:"text_fields"`
	EmbeddingProfile string    `json:"embedding_profile"`
	Enabled          bool      `json:"enabled"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type RetrievalDefinitionRecord struct {
	LayoutVersion    int                   `json:"layout_version"`
	TenantID         string                `json:"tenant_id"`
	TenantGeneration int64                 `json:"tenant_generation,omitempty"`
	Revision         int64                 `json:"revision"`
	Definitions      []RetrievalDefinition `json:"definitions"`
	UpdatedAt        time.Time             `json:"updated_at"`
}

type RetrievalSegmentRef struct {
	Kind        string `json:"kind"`
	Key         string `json:"key"`
	Format      string `json:"format"`
	Codec       string `json:"codec"`
	RowCount    int64  `json:"row_count"`
	ContentHash string `json:"content_hash"`
	SchemaHash  string `json:"schema_hash,omitempty"`
}

type RetrievalCatalog struct {
	LayoutVersion       int                   `json:"layout_version"`
	TenantID            string                `json:"tenant_id"`
	TenantGeneration    int64                 `json:"tenant_generation,omitempty"`
	Revision            int64                 `json:"revision"`
	GraphVersion        int64                 `json:"graph_version"`
	DefinitionRevision  int64                 `json:"definition_revision"`
	EmbeddingProfile    string                `json:"embedding_profile"`
	EmbeddingGeneration string                `json:"embedding_generation"`
	EmbeddingDimensions int                   `json:"embedding_dimensions"`
	IndexCatalogKey     string                `json:"index_catalog_key"`
	IndexCatalogHash    string                `json:"index_catalog_hash"`
	Segments            []RetrievalSegmentRef `json:"segments"`
	UpdatedAt           time.Time             `json:"updated_at"`
}

type RetrievalHead struct {
	LayoutVersion       int       `json:"layout_version"`
	TenantID            string    `json:"tenant_id"`
	TenantGeneration    int64     `json:"tenant_generation,omitempty"`
	Revision            int64     `json:"revision"`
	GraphVersion        int64     `json:"graph_version"`
	DefinitionRevision  int64     `json:"definition_revision"`
	CatalogKey          string    `json:"catalog_key"`
	CatalogHash         string    `json:"catalog_hash"`
	EmbeddingProfile    string    `json:"embedding_profile"`
	EmbeddingGeneration string    `json:"embedding_generation"`
	EmbeddingDimensions int       `json:"embedding_dimensions"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func normalizeRetrievalDefinition(
	definition RetrievalDefinition,
) (RetrievalDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.EmbeddingProfile = strings.TrimSpace(definition.EmbeddingProfile)
	if definition.Name == "" {
		return RetrievalDefinition{}, fmt.Errorf("retrieval definition name is required")
	}
	if strings.Contains(definition.Name, "/") ||
		strings.Contains(definition.Name, "..") {
		return RetrievalDefinition{}, fmt.Errorf(
			"invalid retrieval definition name %q",
			definition.Name,
		)
	}
	var err error
	definition.Kinds, err = normalizeRetrievalNames(
		"retrieval definition kinds",
		definition.Kinds,
		false,
	)
	if err != nil {
		return RetrievalDefinition{}, err
	}
	definition.TextFields, err = normalizeRetrievalNames(
		"retrieval definition text fields",
		definition.TextFields,
		true,
	)
	if err != nil {
		return RetrievalDefinition{}, err
	}
	if definition.EmbeddingProfile == "" {
		return RetrievalDefinition{}, fmt.Errorf(
			"retrieval definition embedding profile is required",
		)
	}
	definition.CreatedAt = definition.CreatedAt.UTC()
	definition.UpdatedAt = definition.UpdatedAt.UTC()
	return definition, nil
}

func normalizeRetrievalDefinitions(
	definitions []RetrievalDefinition,
) ([]RetrievalDefinition, error) {
	normalized := make([]RetrievalDefinition, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for i, definition := range definitions {
		var err error
		normalized[i], err = normalizeRetrievalDefinition(definition)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized[i].Name]; ok {
			return nil, fmt.Errorf(
				"duplicate retrieval definition %q",
				normalized[i].Name,
			)
		}
		seen[normalized[i].Name] = struct{}{}
	}
	slices.SortFunc(normalized, func(left, right RetrievalDefinition) int {
		return strings.Compare(left.Name, right.Name)
	})
	return normalized, nil
}

func normalizeRetrievalNames(
	field string,
	values []string,
	required bool,
) ([]string, error) {
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%s cannot contain an empty value", field)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	if required && len(normalized) == 0 {
		return nil, fmt.Errorf("%s are required", field)
	}
	return normalized, nil
}

func normalizeRetrievalCatalog(
	catalog RetrievalCatalog,
) (RetrievalCatalog, error) {
	catalog.LayoutVersion = RetrievalExtensionLayoutVersion
	catalog.TenantID = strings.TrimSpace(catalog.TenantID)
	catalog.EmbeddingProfile = strings.TrimSpace(catalog.EmbeddingProfile)
	catalog.EmbeddingGeneration = strings.TrimSpace(
		catalog.EmbeddingGeneration,
	)
	catalog.IndexCatalogKey = strings.TrimSpace(catalog.IndexCatalogKey)
	catalog.IndexCatalogHash = strings.TrimSpace(catalog.IndexCatalogHash)
	catalog.UpdatedAt = catalog.UpdatedAt.UTC()
	catalog.Segments = append([]RetrievalSegmentRef(nil), catalog.Segments...)
	if catalog.TenantID == "" ||
		catalog.Revision <= 0 ||
		catalog.GraphVersion < 0 ||
		catalog.DefinitionRevision <= 0 {
		return RetrievalCatalog{}, fmt.Errorf(
			"retrieval catalog identity and revisions are required",
		)
	}
	if catalog.EmbeddingProfile == "" ||
		catalog.EmbeddingGeneration == "" ||
		catalog.EmbeddingDimensions <= 0 {
		return RetrievalCatalog{}, fmt.Errorf(
			"retrieval catalog embedding identity is incomplete",
		)
	}
	if catalog.IndexCatalogKey == "" || catalog.IndexCatalogHash == "" {
		return RetrievalCatalog{}, fmt.Errorf(
			"retrieval catalog graph index reference is required",
		)
	}
	if len(catalog.Segments) == 0 ||
		len(catalog.Segments) > maxRetrievalSegments {
		return RetrievalCatalog{}, fmt.Errorf(
			"retrieval catalog must contain between 1 and %d segments",
			maxRetrievalSegments,
		)
	}
	seenKeys := make(map[string]struct{}, len(catalog.Segments))
	kinds := make(map[string]struct{}, 3)
	for i := range catalog.Segments {
		segment := &catalog.Segments[i]
		segment.Kind = strings.TrimSpace(segment.Kind)
		segment.Key = strings.TrimSpace(segment.Key)
		segment.Format = strings.TrimSpace(segment.Format)
		segment.Codec = strings.TrimSpace(segment.Codec)
		segment.ContentHash = strings.TrimSpace(segment.ContentHash)
		segment.SchemaHash = strings.TrimSpace(segment.SchemaHash)
		switch segment.Kind {
		case RetrievalSegmentChunks,
			RetrievalSegmentVector,
			RetrievalSegmentLexical:
		default:
			return RetrievalCatalog{}, fmt.Errorf(
				"unsupported retrieval segment kind %q",
				segment.Kind,
			)
		}
		if segment.Key == "" ||
			segment.Format != "parquet" ||
			segment.Codec == "" ||
			segment.ContentHash == "" ||
			segment.RowCount < 0 {
			return RetrievalCatalog{}, fmt.Errorf(
				"retrieval segment %q is incomplete",
				segment.Key,
			)
		}
		if _, ok := seenKeys[segment.Key]; ok {
			return RetrievalCatalog{}, fmt.Errorf(
				"duplicate retrieval segment key %q",
				segment.Key,
			)
		}
		seenKeys[segment.Key] = struct{}{}
		kinds[segment.Kind] = struct{}{}
	}
	for _, kind := range []string{
		RetrievalSegmentChunks,
		RetrievalSegmentVector,
		RetrievalSegmentLexical,
	} {
		if _, ok := kinds[kind]; !ok {
			return RetrievalCatalog{}, fmt.Errorf(
				"retrieval catalog is missing %s segments",
				kind,
			)
		}
	}
	slices.SortFunc(
		catalog.Segments,
		func(left, right RetrievalSegmentRef) int {
			if left.Kind == right.Kind {
				return strings.Compare(left.Key, right.Key)
			}
			return strings.Compare(left.Kind, right.Kind)
		},
	)
	return catalog, nil
}

func retrievalHeadFromCatalog(
	catalog RetrievalCatalog,
	key string,
	hash string,
) RetrievalHead {
	return RetrievalHead{
		LayoutVersion:       RetrievalExtensionLayoutVersion,
		TenantID:            catalog.TenantID,
		TenantGeneration:    catalog.TenantGeneration,
		Revision:            catalog.Revision,
		GraphVersion:        catalog.GraphVersion,
		DefinitionRevision:  catalog.DefinitionRevision,
		CatalogKey:          key,
		CatalogHash:         hash,
		EmbeddingProfile:    catalog.EmbeddingProfile,
		EmbeddingGeneration: catalog.EmbeddingGeneration,
		EmbeddingDimensions: catalog.EmbeddingDimensions,
		UpdatedAt:           catalog.UpdatedAt,
	}
}
