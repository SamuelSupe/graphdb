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
	letters, err := s.ListDeadLetters(ctx, task.TenantID, source)
	if err != nil {
		return ReplayReport{}, err
	}
	total := replayCandidateCount(s, task.TenantID, source, letters, cursor, limit)
	if total < 1 {
		total = 1
	}
	report := ReplayReport{}
	checkpoint := ReplayCheckpoint{Cursor: cursor, Total: total}
	if err := s.updateTaskProgress(ctx, task, "replay_deadletters", 0, total, taskResult(checkpoint)); err != nil {
		return ReplayReport{}, err
	}
	for _, letter := range letters {
		key := deadLetterObjectKey(s, task.TenantID, source, letter)
		if cursor != "" && key <= cursor {
			continue
		}
		if limit > 0 && report.Scanned >= limit {
			break
		}
		if letter.Status == "resolved" || letter.Status == "invalid" {
			continue
		}
		if err := s.updateTaskProgress(ctx, task, "replay_deadletters", report.Scanned, total, taskResult(checkpoint)); err != nil {
			return report, err
		}
		report.Scanned++
		result, replayed, err := s.replayDeadLetter(ctx, task.TenantID, source, letter)
		if replayed {
			recordReplayResult(&report, result)
		}
		checkpoint.LastKey = key
		checkpoint.NextCursor = key
		checkpoint.Scanned = report.Scanned
		if err := s.updateTaskProgress(ctx, task, "replay_deadletters", report.Scanned, total, taskResult(checkpoint)); err != nil {
			return report, err
		}
		if err != nil {
			report.Checkpoint = checkpoint
			return report, err
		}
	}
	checkpoint.Completed = true
	report.Checkpoint = checkpoint
	_ = s.updateTaskProgress(ctx, task, "replay_done", total, total, taskResult(checkpoint))
	return report, nil
}

func replayCandidateCount(s *TenantStore, tenantID string, source string, letters []DeadLetter, cursor string, limit int) int {
	total := 0
	for _, letter := range letters {
		key := deadLetterObjectKey(s, tenantID, source, letter)
		if cursor != "" && key <= cursor {
			continue
		}
		if letter.Status == "resolved" || letter.Status == "invalid" {
			continue
		}
		total++
		if limit > 0 && total >= limit {
			return total
		}
	}
	return total
}
