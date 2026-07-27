package graph

import "time"

type Fields map[string]any

type CIType struct {
	Name         string               `json:"name"`
	DisplayName  string               `json:"display_name,omitempty"`
	Extends      []string             `json:"extends,omitempty"`
	Fields       map[string]FieldSpec `json:"fields,omitempty"`
	IdentityKeys []IdentityKey        `json:"identity_keys,omitempty"`
}

// EntityType is the domain-neutral name for CIType introduced in GGraphDB 1.1.
// It is an alias so the in-memory and persisted 1.0 data structures stay identical.
type EntityType = CIType

type FieldSpec struct {
	Type          string `json:"type,omitempty"`
	MergeStrategy string `json:"merge_strategy,omitempty"`
	Required      bool   `json:"required,omitempty"`
	Indexed       bool   `json:"indexed,omitempty"`
	Unique        bool   `json:"unique,omitempty"`
	Enum          []any  `json:"enum,omitempty"`
	Default       any    `json:"default,omitempty"`
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

type IdentityKey struct {
	Name                string   `json:"name"`
	Fields              []string `json:"fields"`
	ConfidenceThreshold float64  `json:"confidence_threshold,omitempty"`
	Strategy            string   `json:"strategy,omitempty"`
}

type Entity struct {
	ID              string                 `json:"id"`
	Kind            string                 `json:"kind"`
	Fields          Fields                 `json:"fields,omitempty"`
	FieldWriteModes map[string]string      `json:"field_write_modes,omitempty"`
	FieldSources    map[string]FieldSource `json:"field_sources,omitempty"`
	FieldConflicts  []FieldConflict        `json:"-"`
	ExistenceSource *FieldSource           `json:"existence_source,omitempty"`
	Source          string                 `json:"source,omitempty"`
	ExternalID      string                 `json:"external_id,omitempty"`
	Identity        map[string]any         `json:"identity_keys,omitempty"`
	Confidence      float64                `json:"confidence,omitempty"`
	SourceRank      int                    `json:"source_priority,omitempty"`
	Sources         []EntitySource         `json:"sources,omitempty"`
	MergedFrom      []string               `json:"merged_from,omitempty"`
	SplitFrom       string                 `json:"split_from,omitempty"`
	Version         int64                  `json:"version"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

type EntitySource struct {
	Source     string    `json:"source"`
	ExternalID string    `json:"external_id"`
	Confidence float64   `json:"confidence,omitempty"`
	Priority   int       `json:"priority,omitempty"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
	Stale      bool      `json:"stale,omitempty"`
	StaleAt    time.Time `json:"stale_at,omitempty"`
}

type FieldSource struct {
	Source     string    `json:"source,omitempty"`
	Priority   int       `json:"priority"`
	Confidence float64   `json:"confidence,omitempty"`
	Version    int64     `json:"version,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
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
	ExistingPriority int    `json:"existing_priority"`
	IncomingSource   string `json:"incoming_source,omitempty"`
	IncomingPriority int    `json:"incoming_priority"`
	ExistingValue    any    `json:"existing_value,omitempty"`
	IncomingValue    any    `json:"incoming_value,omitempty"`
	Message          string `json:"message"`
}

type EntityCanonicalization struct {
	CanonicalID string `json:"canonical_id"`
	IncomingID  string `json:"incoming_id,omitempty"`
	Kind        string `json:"kind"`
	Source      string `json:"source,omitempty"`
	ExternalID  string `json:"external_id,omitempty"`
}

type EdgeCanonicalization struct {
	CanonicalID string `json:"canonical_id"`
	IncomingID  string `json:"incoming_id,omitempty"`
	Type        string `json:"type"`
	From        string `json:"from"`
	To          string `json:"to"`
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

const (
	ManyToMany = "many_to_many"
	OneToMany  = "one_to_many"
	ManyToOne  = "many_to_one"
	OneToOne   = "one_to_one"
)

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
	SourceRank      int                    `json:"source_priority,omitempty"`
	Sources         []EdgeSource           `json:"sources,omitempty"`
	ExistenceSource *FieldSource           `json:"existence_source,omitempty"`
	Version         int64                  `json:"version"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

type EdgeSource struct {
	Source     string    `json:"source"`
	ExternalID string    `json:"external_id,omitempty"`
	EdgeID     string    `json:"edge_id,omitempty"`
	Confidence float64   `json:"confidence,omitempty"`
	Priority   int       `json:"priority,omitempty"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
}

type EdgeDeleteRequest struct {
	ID         string  `json:"id,omitempty"`
	Type       string  `json:"type,omitempty"`
	From       string  `json:"from,omitempty"`
	To         string  `json:"to,omitempty"`
	Source     string  `json:"source,omitempty"`
	SourceRank int     `json:"source_priority,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
	Reason     string  `json:"reason,omitempty"`
}

type EntityDeleteRequest struct {
	ID         string  `json:"id,omitempty"`
	Kind       string  `json:"kind,omitempty"`
	Source     string  `json:"source,omitempty"`
	ExternalID string  `json:"external_id,omitempty"`
	SourceRank int     `json:"source_priority,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
	Reason     string  `json:"reason,omitempty"`
}

type SourceStaleRequest struct {
	Source              string   `json:"source"`
	Kind                string   `json:"kind,omitempty"`
	ObservedExternalIDs []string `json:"observed_external_ids,omitempty"`
	Action              string   `json:"action,omitempty"`
	SourceRank          int      `json:"source_priority,omitempty"`
	Confidence          float64  `json:"confidence,omitempty"`
	Reason              string   `json:"reason,omitempty"`
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

type MergeRequest struct {
	TargetID  string   `json:"target_id"`
	SourceIDs []string `json:"source_ids"`
}

type SplitRequest struct {
	SourceID string   `json:"source_id"`
	Entities []Entity `json:"entities"`
}

type Commit struct {
	LayoutVersion int       `json:"layout_version,omitempty"`
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	Version       int64     `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
	Mutations     Mutations `json:"mutations"`
}

type ApplyOptions struct {
	SourcePolicy *SourcePolicy
}

type ApplyReport struct {
	Suppressed         []FieldConflict          `json:"suppressed,omitempty"`
	CanonicalEntities  []EntityCanonicalization `json:"canonical_entities,omitempty"`
	CanonicalEdges     []EdgeCanonicalization   `json:"canonical_edges,omitempty"`
	AffectedEntityIDs  []string                 `json:"-"`
	AffectedEdgeIDs    []string                 `json:"-"`
	Changed            bool                     `json:"-"`
	ContentFingerprint string                   `json:"-"`
}

type Snapshot struct {
	Version       int64          `json:"version"`
	CITypes       []CIType       `json:"ci_types,omitempty"`
	Entities      []Entity       `json:"entities"`
	RelationTypes []RelationType `json:"relation_types"`
	Edges         []Edge         `json:"edges"`
	Index         *IndexSnapshot `json:"index,omitempty"`
}

type IndexSnapshot struct {
	Version  int64                                     `json:"version"`
	Field    map[string]map[string]map[string][]string `json:"field,omitempty"`
	Out      map[string][]string                       `json:"out,omitempty"`
	In       map[string][]string                       `json:"in,omitempty"`
	Identity map[string]map[string]string              `json:"identity,omitempty"`
}

type Neighbor struct {
	Entity    Entity `json:"entity"`
	Edge      Edge   `json:"edge"`
	Direction string `json:"direction"`
}

type Path struct {
	Entities []Entity `json:"entities"`
	Edges    []Edge   `json:"edges"`
}
