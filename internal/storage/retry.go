package storage

import (
	"context"
	"time"
)

func retryDelay(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt+1) * 20 * time.Millisecond
	if ctx == nil {
		time.Sleep(delay)
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
