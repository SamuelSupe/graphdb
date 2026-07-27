package storage

import (
	"fmt"
	"strings"
)

func restoreIntegrityError(report RestoreIntegrityReport) error {
	if report.Status == "ok" {
		return nil
	}
	detail := strings.Join(report.Issues, "; ")
	if detail == "" {
		detail = report.Status
	}
	return fmt.Errorf("restore integrity failed: %s", detail)
}
