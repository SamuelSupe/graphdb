package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

func (c *apiClient) runTaskWithTimeout(ctx context.Context, metrics *registry, metricName string, timeout time.Duration, taskType string, params map[string]any) error {
	start := time.Now()
	status, err := c.runTask(ctx, timeout, taskType, params)
	if metrics != nil {
		metrics.add(metricName, time.Since(start), status, metricErr(ctx, err))
	}
	return err
}

func (c *apiClient) runTask(ctx context.Context, timeout time.Duration, taskType string, params map[string]any) (int, error) {
	if timeout <= 0 {
		timeout = c.timeout
	}
	startTimeout := c.timeout
	if timeout < startTimeout {
		startTimeout = timeout
	}
	body := map[string]any{"type": taskType}
	if len(params) > 0 {
		body["params"] = params
	}
	resp, err := c.doWithTimeout(ctx, nil, startTimeout, "start-task", http.MethodPost, "/v1/tasks", body, http.StatusAccepted)
	if err != nil {
		return resp.status, err
	}
	taskID, _ := resp.json["id"].(string)
	if taskID == "" {
		return resp.status, fmt.Errorf("%s task response missing id", taskType)
	}
	if err := c.waitTask(ctx, timeout, taskType, taskID); err != nil {
		return resp.status, err
	}
	return resp.status, nil
}

func (c *apiClient) waitTask(ctx context.Context, timeout time.Duration, taskType string, taskID string) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("%s task %s did not finish within %s", taskType, taskID, timeout)
		}
		pollTimeout := c.timeout
		if remaining < pollTimeout {
			pollTimeout = remaining
		}
		resp, err := c.doWithTimeout(ctx, nil, pollTimeout, "task-status", http.MethodGet, "/v1/tasks/"+url.PathEscape(taskID), nil, http.StatusOK)
		if err != nil {
			return err
		}
		status, _ := resp.json["status"].(string)
		switch status {
		case "succeeded":
			return nil
		case "failed":
			message, _ := resp.json["error"].(string)
			if message == "" {
				message = "task failed"
			}
			return fmt.Errorf("%s task %s failed: %s", taskType, taskID, message)
		case "queued", "running":
		default:
			return fmt.Errorf("%s task %s returned unknown status %q", taskType, taskID, status)
		}
		if err := sleepUntilNextTaskPoll(ctx, deadline); err != nil {
			return err
		}
	}
}

func sleepUntilNextTaskPoll(ctx context.Context, deadline time.Time) error {
	sleep := 2 * time.Second
	if remaining := time.Until(deadline); remaining < sleep {
		sleep = remaining
	}
	if sleep <= 0 {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(sleep)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
