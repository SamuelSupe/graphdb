package graphdb

import "context"

type CommitOptions struct {
	ExpectedVersion *int64
	IdempotencyKey  string
}

type CommitRequest struct {
	ExpectedVersion *int64    `json:"expected_version,omitempty"`
	IdempotencyKey  string    `json:"idempotency_key,omitempty"`
	Mutations       Mutations `json:"mutations"`
}

type CommitResult struct {
	TenantID          string                   `json:"tenant_id,omitempty"`
	Version           int64                    `json:"version"`
	HeadCommitID      string                   `json:"head_commit_id,omitempty"`
	SnapshotVersion   int64                    `json:"snapshot_version,omitempty"`
	ReadableVersion   int64                    `json:"readable_version,omitempty"`
	ReadAfterCommitID string                   `json:"read_after_commit_id,omitempty"`
	Skipped           bool                     `json:"skipped,omitempty"`
	IdempotentReplay  bool                     `json:"idempotent_replay,omitempty"`
	DataMD5           string                   `json:"data_md5,omitempty"`
	Suppressed        []FieldConflict          `json:"suppressed,omitempty"`
	CanonicalEntities []EntityCanonicalization `json:"canonical_entities,omitempty"`
	CanonicalEdges    []EdgeCanonicalization   `json:"canonical_edges,omitempty"`
	IndexWarnings     []string                 `json:"index_warnings,omitempty"`
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

func (c *Client) Commit(ctx context.Context, mutations Mutations, options *CommitOptions) (out CommitResult, err error) {
	request := CommitRequest{Mutations: mutations}
	if options != nil {
		request.ExpectedVersion = options.ExpectedVersion
		request.IdempotencyKey = options.IdempotencyKey
	}
	err = c.doJSON(ctx, "POST", "/v1/commits", "", nil, request, &out)
	return out, err
}

type IngestRequest struct {
	Source         string       `json:"source"`
	CollectorID    string       `json:"collector_id"`
	BatchID        string       `json:"batch_id,omitempty"`
	IdempotencyKey string       `json:"idempotency_key,omitempty"`
	Cursor         string       `json:"cursor,omitempty"`
	FullSync       bool         `json:"full_sync,omitempty"`
	StaleAction    string       `json:"stale_action,omitempty"`
	StaleKind      string       `json:"stale_kind,omitempty"`
	Items          []IngestItem `json:"items"`
}

type IngestItem struct {
	ExternalID   string               `json:"external_id"`
	Entity       *Entity              `json:"entity,omitempty"`
	Edge         *Edge                `json:"edge,omitempty"`
	DeleteEntity *EntityDeleteRequest `json:"delete_entity,omitempty"`
	DeleteEdge   *EdgeDeleteRequest   `json:"delete_edge,omitempty"`
	Relation     *RelationType        `json:"relation_type,omitempty"`
	CIType       *CIType              `json:"ci_type,omitempty"`
}

type IngestResult struct {
	BatchID    string           `json:"batch_id"`
	Version    int64            `json:"version"`
	Applied    int              `json:"applied"`
	Failed     int              `json:"failed"`
	Suppressed int              `json:"suppressed,omitempty"`
	Skipped    bool             `json:"skipped"`
	Cursor     string           `json:"cursor,omitempty"`
	Failures   []IngestFailure  `json:"failures,omitempty"`
	Conflicts  []IngestConflict `json:"conflicts,omitempty"`
}

type IngestFailure struct {
	Index      int    `json:"index"`
	ExternalID string `json:"external_id,omitempty"`
	Error      string `json:"error"`
}

type IngestConflict struct {
	ResourceType     string `json:"resource_type,omitempty"`
	Index            int    `json:"index"`
	ExternalID       string `json:"external_id,omitempty"`
	ExistingID       string `json:"existing_id,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	EdgeID           string `json:"edge_id,omitempty"`
	CanonicalID      string `json:"canonical_id,omitempty"`
	IncomingID       string `json:"incoming_id,omitempty"`
	Field            string `json:"field,omitempty"`
	ExistingSource   string `json:"existing_source,omitempty"`
	ExistingPriority int    `json:"existing_priority,omitempty"`
	IncomingSource   string `json:"incoming_source,omitempty"`
	IncomingPriority int    `json:"incoming_priority,omitempty"`
	ExistingValue    any    `json:"existing_value,omitempty"`
	IncomingValue    any    `json:"incoming_value,omitempty"`
	Message          string `json:"message"`
}

func (c *Client) Ingest(ctx context.Context, request IngestRequest) (out IngestResult, err error) {
	err = c.doJSON(ctx, "POST", "/v1/ingest/batches", "", nil, request, &out)
	return out, err
}

type SourcePolicyResponse struct {
	Configured bool         `json:"configured"`
	Policy     SourcePolicy `json:"policy"`
}

func (c *Client) GetSourcePolicy(ctx context.Context) (out SourcePolicyResponse, err error) {
	err = c.doJSON(ctx, "GET", "/v1/source-policy", "", nil, nil, &out)
	return out, err
}

func (c *Client) PutSourcePolicy(ctx context.Context, policy SourcePolicy) (out SourcePolicyResponse, err error) {
	err = c.doJSON(ctx, "PUT", "/v1/source-policy", "", nil, policy, &out)
	return out, err
}

type TenantConfigResponse struct {
	Configured bool           `json:"configured"`
	Config     map[string]any `json:"config"`
}

func (c *Client) GetTenantConfig(ctx context.Context) (out TenantConfigResponse, err error) {
	err = c.doJSON(ctx, "GET", "/v1/tenant-config", "", nil, nil, &out)
	return out, err
}

func (c *Client) PutTenantConfig(ctx context.Context, config map[string]any) (out TenantConfigResponse, err error) {
	err = c.doJSON(ctx, "PUT", "/v1/tenant-config", "", nil, config, &out)
	return out, err
}
