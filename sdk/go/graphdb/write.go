package graphdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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
	Source          string               `json:"source"`
	CollectorID     string               `json:"collector_id"`
	BatchID         string               `json:"batch_id,omitempty"`
	IdempotencyKey  string               `json:"idempotency_key,omitempty"`
	Cursor          string               `json:"cursor,omitempty"`
	FullSync        bool                 `json:"full_sync,omitempty"`
	StaleAction     string               `json:"stale_action,omitempty"`
	StaleKind       string               `json:"stale_kind,omitempty"`
	ExpectedVersion *int64               `json:"expected_version,omitempty"`
	FailureMode     string               `json:"failure_mode,omitempty"`
	Preconditions   []IngestPrecondition `json:"preconditions,omitempty"`
	Items           []IngestItem         `json:"items"`
}

type IngestPrecondition struct {
	ResourceType string `json:"resource_type"`
	ID           string `json:"id"`
	Field        string `json:"field,omitempty"`
	Op           string `json:"op"`
	Value        any    `json:"value,omitempty"`
	ValueFrom    string `json:"value_from,omitempty"`
}

type IngestItem struct {
	ExternalID   string               `json:"external_id"`
	Entity       *Entity              `json:"entity,omitempty"`
	Edge         *Edge                `json:"edge,omitempty"`
	DeleteEntity *EntityDeleteRequest `json:"delete_entity,omitempty"`
	DeleteEdge   *EdgeDeleteRequest   `json:"delete_edge,omitempty"`
	Relation     *RelationType        `json:"relation_type,omitempty"`
	CIType       *CIType              `json:"ci_type,omitempty"`
	EntityType   *EntityType          `json:"entity_type,omitempty"`
}

type IngestResult struct {
	BatchID    string           `json:"batch_id"`
	Version    int64            `json:"version"`
	Applied    int              `json:"applied"`
	Failed     int              `json:"failed"`
	ErrorCode  string           `json:"error_code,omitempty"`
	Suppressed int              `json:"suppressed,omitempty"`
	Skipped    bool             `json:"skipped"`
	SkipReason string           `json:"skip_reason,omitempty"`
	Cursor     string           `json:"cursor,omitempty"`
	Failures   []IngestFailure  `json:"failures,omitempty"`
	Conflicts  []IngestConflict `json:"conflicts,omitempty"`
}

type IngestAcceptance struct {
	WriterID         string    `json:"writer_id,omitempty"`
	BatchID          string    `json:"batch_id"`
	Source           string    `json:"source,omitempty"`
	CollectorID      string    `json:"collector_id,omitempty"`
	State            string    `json:"state"`
	Durability       string    `json:"durability"`
	AcceptedAt       time.Time `json:"accepted_at"`
	EstimatedFlushAt time.Time `json:"estimated_flush_at"`
	StatusURL        string    `json:"status_url"`
}

type IngestSubmission struct {
	StatusCode int
	Accepted   *IngestAcceptance
	Result     *IngestResult
}

type IngestBatchStatus struct {
	WriterID         string        `json:"writer_id,omitempty"`
	TenantID         string        `json:"tenant_id"`
	Source           string        `json:"source"`
	CollectorID      string        `json:"collector_id"`
	BatchID          string        `json:"batch_id"`
	State            string        `json:"state"`
	Durability       string        `json:"durability"`
	AcceptedLSN      uint64        `json:"accepted_lsn,omitempty"`
	AcceptedAt       time.Time     `json:"accepted_at,omitempty"`
	EstimatedFlushAt time.Time     `json:"estimated_flush_at,omitempty"`
	FinishedAt       time.Time     `json:"finished_at,omitempty"`
	Result           *IngestResult `json:"result,omitempty"`
	LastError        string        `json:"last_error,omitempty"`
	RecoveryPending  bool          `json:"recovery_pending,omitempty"`
}

type IngestWaitOptions struct {
	PollInterval time.Duration
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
	AliasField       string `json:"alias_field,omitempty"`
	ExistingSource   string `json:"existing_source,omitempty"`
	ExistingPriority int    `json:"existing_priority,omitempty"`
	IncomingSource   string `json:"incoming_source,omitempty"`
	IncomingPriority int    `json:"incoming_priority,omitempty"`
	ExistingValue    any    `json:"existing_value,omitempty"`
	IncomingValue    any    `json:"incoming_value,omitempty"`
	Message          string `json:"message"`
}

func (c *Client) Ingest(ctx context.Context, request IngestRequest) (out IngestResult, err error) {
	submission, err := c.submitIngest(ctx, request, true)
	if err != nil {
		return out, err
	}
	if submission.Result != nil {
		return *submission.Result, nil
	}
	if submission.Accepted == nil {
		return out, fmt.Errorf("graphdb: ingest response contains neither an acceptance nor a result")
	}
	status, err := c.WaitIngest(ctx, submission.Accepted.StatusURL, nil)
	if err != nil {
		return out, err
	}
	if status.Result == nil {
		return out, fmt.Errorf("graphdb: terminal ingest status %q has no result", status.State)
	}
	return *status.Result, nil
}

func (c *Client) SubmitIngest(ctx context.Context, request IngestRequest) (IngestSubmission, error) {
	return c.submitIngest(ctx, request, false)
}

func (c *Client) submitIngest(ctx context.Context, request IngestRequest, waitCommitted bool) (IngestSubmission, error) {
	var body json.RawMessage
	headers := http.Header{}
	if waitCommitted {
		headers.Set("Prefer", "wait=committed")
	}
	statusCode, responseHeaders, err := c.doJSONWithHeaders(
		ctx, http.MethodPost, "/v1/ingest/batches", "", nil, request, headers, &body,
	)
	if err != nil {
		return IngestSubmission{}, err
	}
	if statusCode == http.StatusAccepted {
		var accepted IngestAcceptance
		if err := json.Unmarshal(body, &accepted); err != nil {
			return IngestSubmission{}, err
		}
		if accepted.StatusURL == "" {
			accepted.StatusURL = responseHeaders.Get("Location")
		}
		if accepted.Source == "" {
			accepted.Source = request.Source
		}
		if accepted.CollectorID == "" {
			accepted.CollectorID = request.CollectorID
		}
		if _, err := validateIngestStatusURL(accepted.StatusURL); err != nil {
			return IngestSubmission{}, err
		}
		return IngestSubmission{StatusCode: statusCode, Accepted: &accepted}, nil
	}
	var result IngestResult
	if err := json.Unmarshal(body, &result); err != nil {
		return IngestSubmission{}, err
	}
	return IngestSubmission{StatusCode: statusCode, Result: &result}, nil
}

func (c *Client) GetIngestStatus(ctx context.Context, statusURL string) (out IngestBatchStatus, err error) {
	path, err := validateIngestStatusURL(statusURL)
	if err != nil {
		return out, err
	}
	err = c.doJSON(ctx, http.MethodGet, path, "", nil, nil, &out)
	return out, err
}

func (c *Client) WaitIngest(ctx context.Context, statusURL string, options *IngestWaitOptions) (IngestBatchStatus, error) {
	interval := 250 * time.Millisecond
	if options != nil {
		if options.PollInterval < 0 {
			return IngestBatchStatus{}, fmt.Errorf("graphdb: ingest poll interval must not be negative")
		}
		if options.PollInterval > 0 {
			interval = options.PollInterval
		}
	}
	for {
		status, err := c.GetIngestStatus(ctx, statusURL)
		if err != nil {
			return IngestBatchStatus{}, err
		}
		switch status.State {
		case "committed", "failed":
			return status, nil
		case "accepted", "prepared", "published", "retrying":
		default:
			return IngestBatchStatus{}, fmt.Errorf("graphdb: unknown ingest state %q", status.State)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return IngestBatchStatus{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func validateIngestStatusURL(statusURL string) (string, error) {
	statusURL = strings.TrimSpace(statusURL)
	parsed, err := url.ParseRequestURI(statusURL)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("graphdb: invalid ingest status URL %q", statusURL)
	}
	path := parsed.EscapedPath()
	prefix := "/v1/ingest/batches/"
	partCount := 3
	if strings.HasPrefix(path, "/v1/ingest/writers/") {
		prefix = "/v1/ingest/writers/"
		partCount = 4
	} else if !strings.HasPrefix(path, prefix) {
		return "", fmt.Errorf("graphdb: invalid ingest status URL %q", statusURL)
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(parts) != partCount {
		return "", fmt.Errorf("graphdb: invalid ingest status URL %q", statusURL)
	}
	for _, part := range parts {
		decoded, decodeErr := url.PathUnescape(part)
		if decodeErr != nil || decoded == "" || decoded == "." || decoded == ".." {
			return "", fmt.Errorf("graphdb: invalid ingest status URL %q", statusURL)
		}
	}
	return path, nil
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
