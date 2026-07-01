package graphdb

import "time"

type Fields map[string]any

type Entity struct {
	ID             string                 `json:"id"`
	Kind           string                 `json:"kind"`
	Fields         Fields                 `json:"fields,omitempty"`
	FieldSources   map[string]FieldSource `json:"field_sources,omitempty"`
	Source         string                 `json:"source,omitempty"`
	ExternalID     string                 `json:"external_id,omitempty"`
	Identity       map[string]any         `json:"identity_keys,omitempty"`
	Confidence     float64                `json:"confidence,omitempty"`
	SourcePriority int                    `json:"source_priority,omitempty"`
	Version        int64                  `json:"version,omitempty"`
	CreatedAt      time.Time              `json:"created_at,omitempty"`
	UpdatedAt      time.Time              `json:"updated_at,omitempty"`
}

type Edge struct {
	ID              string                 `json:"id"`
	Type            string                 `json:"type"`
	From            string                 `json:"from"`
	To              string                 `json:"to"`
	Fields          Fields                 `json:"fields,omitempty"`
	FieldSources    map[string]FieldSource `json:"field_sources,omitempty"`
	Source          string                 `json:"source,omitempty"`
	ExternalID      string                 `json:"external_id,omitempty"`
	Confidence      float64                `json:"confidence,omitempty"`
	SourcePriority  int                    `json:"source_priority,omitempty"`
	ExistenceSource *FieldSource           `json:"existence_source,omitempty"`
	Version         int64                  `json:"version,omitempty"`
	CreatedAt       time.Time              `json:"created_at,omitempty"`
	UpdatedAt       time.Time              `json:"updated_at,omitempty"`
}

type FieldSource struct {
	Source     string    `json:"source,omitempty"`
	Priority   int       `json:"priority"`
	Confidence float64   `json:"confidence,omitempty"`
	Version    int64     `json:"version,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
}

type CIType struct {
	Name         string               `json:"name"`
	DisplayName  string               `json:"display_name,omitempty"`
	Extends      []string             `json:"extends,omitempty"`
	Fields       map[string]FieldSpec `json:"fields,omitempty"`
	IdentityKeys []IdentityKey        `json:"identity_keys,omitempty"`
}

type FieldSpec struct {
	Type          string `json:"type,omitempty"`
	MergeStrategy string `json:"merge_strategy,omitempty"`
	Required      bool   `json:"required,omitempty"`
	Indexed       bool   `json:"indexed,omitempty"`
	Unique        bool   `json:"unique,omitempty"`
	Enum          []any  `json:"enum,omitempty"`
	Default       any    `json:"default,omitempty"`
}

type IdentityKey struct {
	Name                string   `json:"name"`
	Fields              []string `json:"fields"`
	ConfidenceThreshold float64  `json:"confidence_threshold,omitempty"`
	Strategy            string   `json:"strategy,omitempty"`
}

type RelationType struct {
	Name            string   `json:"name"`
	DisplayName     string   `json:"display_name,omitempty"`
	ReverseName     string   `json:"reverse_name,omitempty"`
	FromKind        string   `json:"from_kind,omitempty"`
	ToKind          string   `json:"to_kind,omitempty"`
	FromKinds       []string `json:"from_kinds,omitempty"`
	ToKinds         []string `json:"to_kinds,omitempty"`
	Directed        bool     `json:"directed"`
	Cardinality     string   `json:"cardinality,omitempty"`
	ImpactDirection string   `json:"impact_direction,omitempty"`
	AllowCrossKind  bool     `json:"allow_cross_kind,omitempty"`
	Standard        bool     `json:"standard,omitempty"`
}

type Mutations struct {
	UpsertCITypes        []CIType              `json:"upsert_ci_types,omitempty"`
	DeleteCITypes        []string              `json:"delete_ci_types,omitempty"`
	UpsertRelationTypes  []RelationType        `json:"upsert_relation_types,omitempty"`
	DeleteRelationTypes  []string              `json:"delete_relation_types,omitempty"`
	UpsertEntities       []Entity              `json:"upsert_entities,omitempty"`
	DeleteEntities       []string              `json:"delete_entities,omitempty"`
	DeleteEntityRequests []EntityDeleteRequest `json:"delete_entity_requests,omitempty"`
	MarkSourceStale      []SourceStaleRequest  `json:"mark_source_stale,omitempty"`
	UpsertEdges          []Edge                `json:"upsert_edges,omitempty"`
	DeleteEdges          []string              `json:"delete_edges,omitempty"`
	DeleteEdgeRequests   []EdgeDeleteRequest   `json:"delete_edge_requests,omitempty"`
	MergeEntities        []MergeRequest        `json:"merge_entities,omitempty"`
	SplitEntities        []SplitRequest        `json:"split_entities,omitempty"`
}

type EntityDeleteRequest struct {
	ID             string  `json:"id,omitempty"`
	Kind           string  `json:"kind,omitempty"`
	Source         string  `json:"source,omitempty"`
	ExternalID     string  `json:"external_id,omitempty"`
	SourcePriority int     `json:"source_priority,omitempty"`
	Confidence     float64 `json:"confidence,omitempty"`
	Reason         string  `json:"reason,omitempty"`
}

type EdgeDeleteRequest struct {
	ID             string  `json:"id,omitempty"`
	Type           string  `json:"type,omitempty"`
	From           string  `json:"from,omitempty"`
	To             string  `json:"to,omitempty"`
	Source         string  `json:"source,omitempty"`
	SourcePriority int     `json:"source_priority,omitempty"`
	Confidence     float64 `json:"confidence,omitempty"`
	Reason         string  `json:"reason,omitempty"`
}

type SourceStaleRequest struct {
	Source              string   `json:"source"`
	Kind                string   `json:"kind,omitempty"`
	ObservedExternalIDs []string `json:"observed_external_ids,omitempty"`
	Action              string   `json:"action,omitempty"`
	SourcePriority      int      `json:"source_priority,omitempty"`
	Confidence          float64  `json:"confidence,omitempty"`
	Reason              string   `json:"reason,omitempty"`
}

type MergeRequest struct {
	TargetID  string   `json:"target_id"`
	SourceIDs []string `json:"source_ids"`
}

type SplitRequest struct {
	SourceID string   `json:"source_id"`
	Entities []Entity `json:"entities"`
}

type FieldConflict struct {
	ResourceType     string `json:"resource_type,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	EdgeID           string `json:"edge_id,omitempty"`
	CanonicalID      string `json:"canonical_id,omitempty"`
	IncomingID       string `json:"incoming_id,omitempty"`
	Field            string `json:"field,omitempty"`
	AliasField       string `json:"alias_field,omitempty"`
	ExistingSource   string `json:"existing_source,omitempty"`
	ExistingPriority int    `json:"existing_priority,omitempty"`
	IncomingSource   string `json:"incoming_source,omitempty"`
	IncomingPriority int    `json:"incoming_priority,omitempty"`
	ExistingValue    any    `json:"existing_value,omitempty"`
	IncomingValue    any    `json:"incoming_value,omitempty"`
	Message          string `json:"message"`
}

type SourcePolicy struct {
	DefaultPriority int                 `json:"default_priority"`
	Sources         []SourcePolicyItem  `json:"sources,omitempty"`
	FieldAliases    []FieldAliasRule    `json:"field_aliases,omitempty"`
	FieldPriorities []FieldPriorityRule `json:"field_priorities,omitempty"`
}

type SourcePolicyItem struct {
	Name        string `json:"name"`
	Priority    int    `json:"priority"`
	Description string `json:"description,omitempty"`
}

type FieldAliasRule struct {
	Source  string            `json:"source"`
	Kind    string            `json:"kind,omitempty"`
	Aliases map[string]string `json:"aliases,omitempty"`
}

type FieldPriorityRule struct {
	Source string         `json:"source"`
	Kind   string         `json:"kind,omitempty"`
	Fields map[string]int `json:"fields,omitempty"`
}
