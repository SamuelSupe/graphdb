package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func runReceiptCheck(cfg config.Config, args []string) error {
	const worker = "--check-worker"
	if len(args) == 2 && args[0] == worker {
		count, err := storage.CheckLocalReceiptDatabase(context.Background(), cfg.DataDir, cfg.Prefix, args[1])
		if err != nil {
			return err
		}
		return printJSON(map[string]any{"tenant_id": args[1], "receipts_checked": count})
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: graphdb receipt-check <tenant-id>")
	}
	if err := storage.ValidateTenantID(args[0]); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "receipt-check", worker, args[0])
	cmd.Env = append(os.Environ(), "GOTRACEBACK=none")
	output, err := receiptCheckSubprocess(ctx, cmd)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(output)
	return err
}

// A bbolt assertion or mapped-file fault can kill the worker goroutine's
// process before it returns an error. Keep that failure local to the checker.
func receiptCheckSubprocess(ctx context.Context, cmd *exec.Cmd) ([]byte, error) {
	output, err := cmd.Output()
	if err == nil {
		return output, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return nil, err
	}
	if exit.ExitCode() != 1 {
		return nil, fmt.Errorf("%w: receipt checker terminated unexpectedly (%v)", storage.ErrReceiptStoreUnavailable, exit)
	}
	// main exits with status 1 for reported errors, including lock conflicts
	// and missing tenants. Preserve those diagnostics without relabeling them.
	if message := strings.TrimPrefix(strings.TrimSpace(string(exit.Stderr)), "error: "); message != "" {
		return nil, errors.New(message)
	}
	return nil, err
}
