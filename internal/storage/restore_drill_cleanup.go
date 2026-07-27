package storage

import (
	"context"
	"fmt"
)

func applyRestoreDrillCleanup(
	ctx context.Context,
	cleanup func(context.Context) (TenantPurgeReport, error),
	report *TenantRestoreDrillReport,
) error {
	purge, err := cleanup(ctx)
	if err != nil {
		report.CleanupError = err.Error()
		report.Proof.Checks = append(report.Proof.Checks, TenantRestoreProofCheck{
			Name: "cleanup", Status: "error", Required: true, Message: err.Error(),
		})
	} else {
		report.CleanupDeleted = purge.Deleted
		report.Proof.Checks = append(report.Proof.Checks, TenantRestoreProofCheck{
			Name: "cleanup", Status: "ok", Required: true,
			Message: fmt.Sprintf("deleted %d drill objects", purge.Deleted),
		})
	}
	report.Proof.finish()
	return err
}
