package graphdb

import (
	"context"
	"net/url"
	"strconv"
)

type TenantInfo struct {
	TenantID         string            `json:"tenant_id"`
	Status           string            `json:"status"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Metadata         map[string]any    `json:"metadata,omitempty"`
	ClonedFrom       string            `json:"cloned_from,omitempty"`
	ManifestVersion  int64             `json:"manifest_version"`
	SnapshotVersion  int64             `json:"snapshot_version"`
	CommitTailLength int               `json:"commit_tail_length"`
	Exists           bool              `json:"exists"`
	CreatedAt        string            `json:"created_at,omitempty"`
	UpdatedAt        string            `json:"updated_at,omitempty"`
	DisabledAt       string            `json:"disabled_at,omitempty"`
	DeletedAt        string            `json:"deleted_at,omitempty"`
}

type TenantCreateRequest struct {
	TenantID     string            `json:"tenant_id,omitempty"`
	Name         string            `json:"name,omitempty"`
	Description  string            `json:"description,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Metadata     map[string]any    `json:"metadata,omitempty"`
	Config       map[string]any    `json:"config,omitempty"`
	SourcePolicy *SourcePolicy     `json:"source_policy,omitempty"`
}

type TenantCloneRequest struct {
	TargetTenantID string            `json:"target_tenant_id"`
	Name           string            `json:"name,omitempty"`
	Description    string            `json:"description,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Metadata       map[string]any    `json:"metadata,omitempty"`
}

type TenantPurgeReport struct {
	TenantID             string   `json:"tenant_id"`
	Deleted              int      `json:"deleted"`
	DeletedKeys          []string `json:"deleted_keys,omitempty"`
	DeletedKeysTruncated bool     `json:"deleted_keys_truncated,omitempty"`
}

func (c *Client) ListTenants(ctx context.Context, includeLegacy bool) ([]TenantInfo, error) {
	values := url.Values{}
	setBool(values, "include_legacy", includeLegacy)
	var out struct {
		Tenants []TenantInfo `json:"tenants"`
	}
	if err := c.doJSON(ctx, "GET", "/v1/tenants", platformTenant, values, nil, &out); err != nil {
		return nil, err
	}
	return out.Tenants, nil
}

type TenantListOptions struct {
	Limit         int
	Query         string
	Cursor        string
	IncludeLegacy bool
}

type TenantListPage struct {
	Tenants    []TenantInfo `json:"tenants"`
	Total      int          `json:"total"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// ListTenantPage searches IDs and reads a live page, using a default limit of 50.
// Reuse Query and NextCursor for the next page; the server accepts limits 1–200.
// IncludeLegacy opts into a potentially expensive storage-prefix scan.
func (c *Client) ListTenantPage(ctx context.Context, options TenantListOptions) (out TenantListPage, err error) {
	limit := options.Limit
	if limit == 0 {
		limit = 50
	}
	values := url.Values{"limit": {strconv.Itoa(limit)}}
	if options.Query != "" {
		values.Set("q", options.Query)
	}
	if options.Cursor != "" {
		values.Set("cursor", options.Cursor)
	}
	setBool(values, "include_legacy", options.IncludeLegacy)
	err = c.doJSON(ctx, "GET", "/v1/tenants", platformTenant, values, nil, &out)
	return out, err
}

func (c *Client) CreateTenant(ctx context.Context, request TenantCreateRequest) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants", platformTenant, nil, request, &out)
	return out, err
}

func (c *Client) GetTenant(ctx context.Context, tenantID string) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "GET", "/v1/tenants/"+pathEscape(tenantID), platformTenant, nil, nil, &out)
	return out, err
}

func (c *Client) UpdateTenant(ctx context.Context, tenantID string, request TenantCreateRequest) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "PUT", "/v1/tenants/"+pathEscape(tenantID), platformTenant, nil, request, &out)
	return out, err
}

func (c *Client) DisableTenant(ctx context.Context, tenantID string) (TenantInfo, error) {
	return c.tenantAction(ctx, tenantID, "disable")
}

func (c *Client) EnableTenant(ctx context.Context, tenantID string) (TenantInfo, error) {
	return c.tenantAction(ctx, tenantID, "enable")
}

func (c *Client) DeleteTenant(ctx context.Context, tenantID string) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "DELETE", "/v1/tenants/"+pathEscape(tenantID), platformTenant, nil, nil, &out)
	return out, err
}

func (c *Client) PurgeTenant(ctx context.Context, tenantID string, force bool) (out TenantPurgeReport, err error) {
	values := url.Values{}
	setBool(values, "force", force)
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/purge", platformTenant, values, nil, &out)
	return out, err
}

func (c *Client) CloneTenant(ctx context.Context, sourceTenantID string, request TenantCloneRequest) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(sourceTenantID)+"/clone", platformTenant, nil, request, &out)
	return out, err
}

func (c *Client) BackupTenant(ctx context.Context, tenantID string) (out Task, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/backup", platformTenant, nil, nil, &out)
	return out, err
}

// BackupTenantToObjectStorage captures a committed snapshot in the server's
// configured S3 repository. The completed task result contains its backup_key.
func (c *Client) BackupTenantToObjectStorage(ctx context.Context, tenantID string) (out Task, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/backup", platformTenant, nil, map[string]any{"destination": "object"}, &out)
	return out, err
}

type ObjectBackup struct {
	Automatic   bool   `json:"automatic,omitempty"`
	Format      string `json:"format"`
	TenantID    string `json:"tenant_id"`
	BackupID    string `json:"backup_id"`
	Version     int64  `json:"version"`
	CreatedAt   string `json:"created_at"`
	SnapshotKey string `json:"snapshot_key"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
	BackupKey   string `json:"backup_key"`
}

type ObjectBackupPage struct {
	Backups    []ObjectBackup `json:"backups"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

func (c *Client) ListObjectBackups(ctx context.Context, tenantID, cursor string, limit int) (out ObjectBackupPage, err error) {
	values := url.Values{}
	if cursor != "" {
		values.Set("cursor", cursor)
	}
	if limit != 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	err = c.doJSON(ctx, "GET", "/v1/tenants/"+pathEscape(tenantID)+"/backups", platformTenant, values, nil, &out)
	return out, err
}

func (c *Client) RestoreTenant(ctx context.Context, tenantID string, backupKey string, overwrite bool, dryRun bool) (out Task, err error) {
	request := map[string]any{"backup_key": backupKey, "overwrite": overwrite, "dry_run": dryRun}
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/restore", platformTenant, nil, request, &out)
	return out, err
}

func (c *Client) RestoreDrillTenant(ctx context.Context, tenantID string, request map[string]any) (out Task, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/restore-drill", platformTenant, nil, request, &out)
	return out, err
}

func (c *Client) tenantAction(ctx context.Context, tenantID string, action string) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/"+action, platformTenant, nil, nil, &out)
	return out, err
}

// BackupAutomationStatus reports the last observed scheduled cycle. Task progress
// remains available through GetTask; success includes verification and retention.
type BackupAutomationStatus struct {
	TaskID              string `json:"task_id,omitempty"`
	NextRun             string `json:"next_run,omitempty"`
	LastSuccess         string `json:"last_success,omitempty"`
	LastBackupKey       string `json:"last_backup_key,omitempty"`
	LastDrill           string `json:"last_drill,omitempty"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	LastError           string `json:"last_error,omitempty"`
}

func (c *Client) GetBackupAutomation(ctx context.Context) (out BackupAutomationStatus, err error) {
	err = c.doJSON(ctx, "GET", "/v1/backup-automation", platformTenant, nil, nil, &out)
	return out, err
}

// ResetBackupAutomation abandons the current retry; an enabled policy starts a
// fresh capture on its next tick. Returns a conflict while a backup is active.
func (c *Client) ResetBackupAutomation(ctx context.Context) (out BackupAutomationStatus, err error) {
	err = c.doJSON(ctx, "POST", "/v1/backup-automation/reset", platformTenant, nil, nil, &out)
	return out, err
}
