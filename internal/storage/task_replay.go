package storage

import (
	"context"
	"fmt"
)

type ReplayCheckpoint struct {
	Cursor     string `json:"cursor,omitempty"`
	NextCursor string `json:"next_cursor,omitempty"`
	LastKey    string `json:"last_key,omitempty"`
	Scanned    int    `json:"scanned,omitempty"`
	Total      int    `json:"total,omitempty"`
	Completed  bool   `json:"completed"`
}

func (s *TenantStore) replayDeadLettersTask(ctx context.Context, task Task) (ReplayReport, error) {
	source, err := normalizeDeadLetterScope(task.TenantID, stringTaskParam(task.Params, "source"))
	if err != nil {
		return ReplayReport{}, err
	}
	limit := intTaskParam(task.Params, "limit")
	if limit < 0 {
		return ReplayReport{}, fmt.Errorf("limit must be a non-negative integer")
	}
	cursor := stringTaskParam(task.Params, "cursor")
	candidateTotal, windowEnd, err := s.replayTaskWindow(
		ctx, task.TenantID, source, cursor, limit,
	)
	if err != nil {
		return ReplayReport{}, err
	}
	total := candidateTotal
	if total < 1 {
		total = 1
	}
	report := ReplayReport{}
	checkpoint := ReplayCheckpoint{Cursor: cursor, Total: total}
	if err := s.updateTaskProgress(ctx, task, "replay_deadletters", 0, total, taskResult(checkpoint)); err != nil {
		return ReplayReport{}, err
	}
	if candidateTotal > 0 {
		err = s.walkDeadLettersByKey(
			ctx,
			task.TenantID,
			source,
			cursor,
			func(letter DeadLetter) (bool, error) {
				if limit > 0 && report.Scanned >= limit {
					return false, nil
				}
				key := deadLetterObjectKey(s, task.TenantID, source, letter)
				if key > windowEnd {
					return false, nil
				}
				if letter.Status == "resolved" || letter.Status == "invalid" {
					return true, nil
				}
				if err := s.updateTaskProgress(ctx, task, "replay_deadletters", report.Scanned, total, taskResult(checkpoint)); err != nil {
					return false, err
				}
				report.Scanned++
				result, replayed, err := s.replayDeadLetter(ctx, task.TenantID, source, letter)
				if replayed {
					recordReplayResult(&report, result)
				}
				checkpoint.Scanned = report.Scanned
				if err == nil {
					checkpoint.LastKey = key
					checkpoint.NextCursor = key
				}
				if err := s.updateTaskProgress(ctx, task, "replay_deadletters", report.Scanned, total, taskResult(checkpoint)); err != nil {
					return false, err
				}
				if err != nil {
					return false, err
				}
				return true, nil
			},
		)
	}
	if err != nil {
		report.Checkpoint = checkpoint
		return report, err
	}
	checkpoint.Completed = true
	report.Checkpoint = checkpoint
	_ = s.updateTaskProgress(ctx, task, "replay_done", total, total, taskResult(checkpoint))
	return report, nil
}

func (s *TenantStore) replayTaskWindow(
	ctx context.Context,
	tenantID string,
	source string,
	cursor string,
	limit int,
) (int, string, error) {
	total := 0
	windowEnd := ""
	err := s.walkDeadLettersByKey(
		ctx,
		tenantID,
		source,
		cursor,
		func(letter DeadLetter) (bool, error) {
			if letter.Status == "resolved" || letter.Status == "invalid" {
				return true, nil
			}
			total++
			windowEnd = deadLetterObjectKey(s, tenantID, source, letter)
			return limit <= 0 || total < limit, nil
		},
	)
	return total, windowEnd, err
}
