package storage

import (
	"context"
	"math/rand/v2"
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

func coordinatorRetryDelay(ctx context.Context, attempt int) error {
	delay := coordinatorRetryBackoff(attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	if ctx == nil {
		<-timer.C
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func coordinatorRetryBackoff(attempt int) time.Duration {
	ceiling := 5 * time.Millisecond
	for i := 0; i < attempt && ceiling < 200*time.Millisecond; i++ {
		ceiling *= 2
		if ceiling > 200*time.Millisecond {
			ceiling = 200 * time.Millisecond
		}
	}
	floor := ceiling / 2
	if floor < 5*time.Millisecond {
		floor = 5 * time.Millisecond
	}
	if ceiling <= floor {
		return floor
	}
	return floor + time.Duration(rand.Int64N(int64(ceiling-floor)+1))
}
