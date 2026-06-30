package graphdb

import (
	"context"
	"net/url"
)

type Task struct {
	ID                string         `json:"id"`
	TenantID          string         `json:"tenant_id"`
	Type              string         `json:"type"`
	Status            string         `json:"status"`
	Phase             string         `json:"phase,omitempty"`
	ProgressCompleted int            `json:"progress_completed,omitempty"`
	ProgressTotal     int            `json:"progress_total,omitempty"`
	OwnerID           string         `json:"owner_id,omitempty"`
	Params            map[string]any `json:"params,omitempty"`
	Checkpoint        map[string]any `json:"checkpoint,omitempty"`
	Result            map[string]any `json:"result,omitempty"`
	ResultKey         string         `json:"result_key,omitempty"`
	Error             string         `json:"error,omitempty"`
	StartedAt         string         `json:"started_at"`
	UpdatedAt         string         `json:"updated_at,omitempty"`
	FinishedAt        string         `json:"finished_at,omitempty"`
}

type TaskListOptions struct {
	Type   string
	Status string
	Limit  int
}

func (c *Client) StartTask(ctx context.Context, taskType string, params map[string]any) (out Task, err error) {
	request := map[string]any{"type": taskType}
	if params != nil {
		request["params"] = params
	}
	err = c.doJSON(ctx, "POST", "/v1/tasks", "", nil, request, &out)
	return out, err
}

func (c *Client) ListTasks(ctx context.Context, options TaskListOptions) ([]Task, error) {
	values := queryValues("type", options.Type, "status", options.Status)
	setInt(values, "limit", options.Limit)
	var out struct {
		Tasks []Task `json:"tasks"`
	}
	if err := c.doJSON(ctx, "GET", "/v1/tasks", "", values, nil, &out); err != nil {
		return nil, err
	}
	return out.Tasks, nil
}

func (c *Client) GetTask(ctx context.Context, id string) (out Task, err error) {
	err = c.doJSON(ctx, "GET", "/v1/tasks/"+pathEscape(id), "", nil, nil, &out)
	return out, err
}

func (c *Client) CancelTask(ctx context.Context, id string) (Task, error) {
	return c.taskAction(ctx, id, "cancel")
}

func (c *Client) RetryTask(ctx context.Context, id string) (Task, error) {
	return c.taskAction(ctx, id, "retry")
}

func (c *Client) taskAction(ctx context.Context, id string, action string) (out Task, err error) {
	err = c.doJSON(ctx, "POST", "/v1/tasks/"+pathEscape(id)+"/"+action, "", nil, nil, &out)
	return out, err
}

type IndexDefinition struct {
	Name      string `json:"name,omitempty"`
	Kind      string `json:"kind"`
	Field     string `json:"field"`
	Unique    bool   `json:"unique,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

func (c *Client) ListIndexDefinitions(ctx context.Context) ([]IndexDefinition, error) {
	var out struct {
		Indexes []IndexDefinition `json:"indexes"`
	}
	if err := c.doJSON(ctx, "GET", "/v1/indexes/definitions", "", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Indexes, nil
}

func (c *Client) CreateIndex(ctx context.Context, definition IndexDefinition) (out map[string]any, err error) {
	err = c.doJSON(ctx, "POST", "/v1/indexes", "", nil, definition, &out)
	return out, err
}

func (c *Client) DropIndex(ctx context.Context, name string) (out map[string]any, err error) {
	err = c.doJSON(ctx, "DELETE", "/v1/indexes/definitions/"+pathEscape(name), "", nil, nil, &out)
	return out, err
}

func (c *Client) IndexCatalog(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, "GET", "/v1/indexes", "", nil, nil, &out)
	return out, err
}

func (c *Client) IndexHealth(ctx context.Context, deep bool) (out map[string]any, err error) {
	values := url.Values{}
	setBool(values, "deep", deep)
	err = c.doJSON(ctx, "GET", "/v1/indexes/health", "", values, nil, &out)
	return out, err
}

func (c *Client) RebuildIndexes(ctx context.Context, async bool) (out map[string]any, err error) {
	values := url.Values{}
	setBool(values, "async", async)
	err = c.doJSON(ctx, "POST", "/v1/indexes/rebuild", "", values, nil, &out)
	return out, err
}

func (c *Client) ReaderFreshness(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, "GET", "/v1/control/reader-freshness", "", nil, nil, &out)
	return out, err
}

func (c *Client) ReaderFleetReadiness(ctx context.Context, query url.Values) (out map[string]any, err error) {
	err = c.doJSON(ctx, "GET", "/v1/control/reader-fleet-readiness", "", query, nil, &out)
	return out, err
}

func (c *Client) ReaderTrafficGate(ctx context.Context, query url.Values) (out map[string]any, err error) {
	err = c.doJSON(ctx, "GET", "/v1/control/reader-traffic-gate", "", query, nil, &out)
	return out, err
}

func (c *Client) WriterLease(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, "GET", "/v1/control/writer-lease", "", nil, nil, &out)
	return out, err
}

func (c *Client) IntegrityAudit(ctx context.Context, deep bool) (out map[string]any, err error) {
	values := url.Values{}
	if !deep {
		values.Set("deep", "false")
	}
	err = c.doJSON(ctx, "GET", "/v1/control/integrity-audit", "", values, nil, &out)
	return out, err
}

func (c *Client) Recover(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, "POST", "/v1/control/recover", "", nil, nil, &out)
	return out, err
}

func (c *Client) Repair(ctx context.Context, apply bool) (out map[string]any, err error) {
	err = c.doJSON(ctx, "POST", "/v1/control/repair", "", nil, map[string]any{"apply": apply}, &out)
	return out, err
}

func (c *Client) CleanupCommits(ctx context.Context) (out map[string]any, err error) {
	err = c.doJSON(ctx, "POST", "/v1/control/cleanup-commits", "", nil, nil, &out)
	return out, err
}

func (c *Client) GC(ctx context.Context, request map[string]any) (out map[string]any, err error) {
	err = c.doJSON(ctx, "POST", "/v1/control/gc", "", nil, request, &out)
	return out, err
}

func (c *Client) Compact(ctx context.Context) (out CommitResult, err error) {
	err = c.doJSON(ctx, "POST", "/v1/compact", "", nil, nil, &out)
	return out, err
}
