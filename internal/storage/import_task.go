package storage

import (
	"context"
	"encoding/json"
	"fmt"
)

const maxImportIssues = 100

type ImportIssue struct {
	Row        int    `json:"row"`
	ExternalID string `json:"external_id,omitempty"`
	Error      string `json:"error"`
}

type ImportReport struct {
	ImportID    string        `json:"import_id"`
	Format      string        `json:"format"`
	Source      string        `json:"source"`
	CollectorID string        `json:"collector_id"`
	Records     int           `json:"records"`
	Applied     int           `json:"applied"`
	Failed      int           `json:"failed"`
	Suppressed  int           `json:"suppressed,omitempty"`
	Batches     int           `json:"batches"`
	Version     int64         `json:"version,omitempty"`
	Issues      []ImportIssue `json:"issues,omitempty"`
}

type importCheckpoint struct {
	RecordsCompleted int           `json:"records_completed"`
	BatchesCompleted int           `json:"batches_completed"`
	Applied          int           `json:"applied"`
	Failed           int           `json:"failed"`
	Suppressed       int           `json:"suppressed,omitempty"`
	Version          int64         `json:"version,omitempty"`
	Total            int           `json:"total"`
	Issues           []ImportIssue `json:"issues,omitempty"`
	Completed        bool          `json:"completed"`
}

func (s *TenantStore) bulkImportTask(ctx context.Context, task Task) (ImportReport, error) {
	options, err := importOptionsFromTask(task)
	if err != nil {
		return ImportReport{}, err
	}
	importID := stringTaskParam(task.Params, "import_id")
	key := stringTaskParam(task.Params, "source_key")
	if err := s.validateImportSourceKey(task.TenantID, key); err != nil {
		return ImportReport{}, err
	}
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return ImportReport{}, fmt.Errorf("read import source: %w", err)
	}
	reader, err := newImportRecordReader(options.Format, data)
	if err != nil {
		return ImportReport{}, err
	}
	checkpoint := importCheckpointFromTask(task)
	total := max(checkpoint.Total, estimatedImportRecords(options.Format, data))
	if err := skipImportedRecords(reader, checkpoint.RecordsCompleted); err != nil {
		return ImportReport{}, err
	}
	report := ImportReport{
		ImportID: importID, Format: options.Format, Source: options.Source, CollectorID: options.CollectorID,
		Records: checkpoint.RecordsCompleted, Applied: checkpoint.Applied, Failed: checkpoint.Failed,
		Suppressed: checkpoint.Suppressed, Batches: checkpoint.BatchesCompleted, Version: checkpoint.Version,
		Issues: append([]ImportIssue(nil), checkpoint.Issues...),
	}
	if err := s.updateTaskProgress(ctx, task, "importing", report.Records, total, taskResult(checkpoint)); err != nil {
		return report, err
	}

	items := make([]IngestItem, 0, options.BatchSize)
	rows := make([]int, 0, options.BatchSize)
	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		batchNumber := report.Batches + 1
		batchID := fmt.Sprintf("%s-%06d", importID, batchNumber)
		result, err := s.Ingest(ctx, task.TenantID, IngestRequest{
			Source: options.Source, CollectorID: options.CollectorID,
			BatchID: batchID, IdempotencyKey: batchID, Cursor: fmt.Sprintf("%d", report.Records), Items: items,
		})
		if err != nil {
			return err
		}
		recordImportResult(&report, result, rows)
		if result.Failed > 0 && options.OnError == "abort" {
			return fmt.Errorf("import batch %d contains %d failed records", batchNumber, result.Failed)
		}
		report.Batches = batchNumber
		checkpoint = importCheckpointFromReport(report, total, false)
		if err := s.updateTaskProgress(ctx, task, "importing", report.Records, total, taskResult(checkpoint)); err != nil {
			return err
		}
		items = items[:0]
		rows = rows[:0]
		return nil
	}

	for {
		item, row, ok, readErr := reader.Next()
		if !ok {
			if readErr != nil {
				return report, readErr
			}
			break
		}
		report.Records++
		if readErr != nil {
			report.Failed++
			appendImportIssue(&report, ImportIssue{Row: row, Error: readErr.Error()})
			if options.OnError == "abort" {
				return report, readErr
			}
			continue
		}
		items = append(items, item)
		rows = append(rows, row)
		if len(items) >= options.BatchSize {
			if err := flush(); err != nil {
				return report, err
			}
		}
	}
	if err := flush(); err != nil {
		return report, err
	}
	total = max(total, report.Records)
	checkpoint = importCheckpointFromReport(report, total, true)
	if err := s.updateTaskProgress(ctx, task, "import_done", total, total, taskResult(checkpoint)); err != nil {
		return report, err
	}
	return report, nil
}

func importOptionsFromTask(task Task) (ImportOptions, error) {
	return normalizeImportOptions(ImportOptions{
		Format: stringTaskParam(task.Params, "format"), Source: stringTaskParam(task.Params, "source"),
		CollectorID: stringTaskParam(task.Params, "collector_id"), BatchSize: intTaskParam(task.Params, "batch_size"),
		OnError: stringTaskParam(task.Params, "on_error"),
	})
}

func skipImportedRecords(reader importRecordReader, count int) error {
	for i := 0; i < count; i++ {
		_, _, ok, err := reader.Next()
		if !ok {
			if err != nil {
				return err
			}
			return fmt.Errorf("import checkpoint exceeds the source record count")
		}
	}
	return nil
}

func recordImportResult(report *ImportReport, result IngestResult, rows []int) {
	report.Applied += result.Applied
	report.Failed += result.Failed
	report.Suppressed += result.Suppressed
	if result.Version > report.Version {
		report.Version = result.Version
	}
	for _, failure := range result.Failures {
		row := 0
		if failure.Index >= 0 && failure.Index < len(rows) {
			row = rows[failure.Index]
		}
		appendImportIssue(report, ImportIssue{Row: row, ExternalID: failure.ExternalID, Error: failure.Error})
	}
}

func appendImportIssue(report *ImportReport, issue ImportIssue) {
	if len(report.Issues) < maxImportIssues {
		report.Issues = append(report.Issues, issue)
	}
}

func importCheckpointFromReport(report ImportReport, total int, completed bool) importCheckpoint {
	return importCheckpoint{
		RecordsCompleted: report.Records, BatchesCompleted: report.Batches,
		Applied: report.Applied, Failed: report.Failed, Suppressed: report.Suppressed,
		Version: report.Version, Total: total, Issues: append([]ImportIssue(nil), report.Issues...), Completed: completed,
	}
}

func importCheckpointFromTask(task Task) importCheckpoint {
	if len(task.Checkpoint) == 0 {
		return importCheckpoint{}
	}
	data, err := json.Marshal(task.Checkpoint)
	if err != nil {
		return importCheckpoint{}
	}
	var checkpoint importCheckpoint
	_ = json.Unmarshal(data, &checkpoint)
	return checkpoint
}
