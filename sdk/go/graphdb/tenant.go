package graphdb

import (
	"context"
	"net/url"
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
	if err := c.doJSON(ctx, "GET", "/v1/tenants", "", values, nil, &out); err != nil {
		return nil, err
	}
	return out.Tenants, nil
}

func (c *Client) CreateTenant(ctx context.Context, request TenantCreateRequest) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants", "", nil, request, &out)
	return out, err
}

func (c *Client) GetTenant(ctx context.Context, tenantID string) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "GET", "/v1/tenants/"+pathEscape(tenantID), "", nil, nil, &out)
	return out, err
}

func (c *Client) UpdateTenant(ctx context.Context, tenantID string, request TenantCreateRequest) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "PUT", "/v1/tenants/"+pathEscape(tenantID), "", nil, request, &out)
	return out, err
}

func (c *Client) DisableTenant(ctx context.Context, tenantID string) (TenantInfo, error) {
	return c.tenantAction(ctx, tenantID, "disable")
}

func (c *Client) EnableTenant(ctx context.Context, tenantID string) (TenantInfo, error) {
	return c.tenantAction(ctx, tenantID, "enable")
}

func (c *Client) DeleteTenant(ctx context.Context, tenantID string) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "DELETE", "/v1/tenants/"+pathEscape(tenantID), "", nil, nil, &out)
	return out, err
}

func (c *Client) PurgeTenant(ctx context.Context, tenantID string, force bool) (out TenantPurgeReport, err error) {
	values := url.Values{}
	setBool(values, "force", force)
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/purge", "", values, nil, &out)
	return out, err
}

func (c *Client) CloneTenant(ctx context.Context, sourceTenantID string, request TenantCloneRequest) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(sourceTenantID)+"/clone", "", nil, request, &out)
	return out, err
}

func (c *Client) BackupTenant(ctx context.Context, tenantID string) (out Task, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/backup", "", nil, nil, &out)
	return out, err
}

func (c *Client) RestoreTenant(ctx context.Context, tenantID string, backupKey string, overwrite bool, dryRun bool) (out Task, err error) {
	request := map[string]any{"backup_key": backupKey, "overwrite": overwrite, "dry_run": dryRun}
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/restore", "", nil, request, &out)
	return out, err
}

func (c *Client) RestoreDrillTenant(ctx context.Context, tenantID string, request map[string]any) (out Task, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/restore-drill", "", nil, request, &out)
	return out, err
}

func (c *Client) tenantAction(ctx context.Context, tenantID string, action string) (out TenantInfo, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tenants/"+pathEscape(tenantID)+"/"+action, "", nil, nil, &out)
	return out, err
}
