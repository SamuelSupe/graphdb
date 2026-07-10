package storage

import (
	"context"
	"errors"
	"testing"
)

func TestParquetDecodeAdmissionHonorsContext(t *testing.T) {
	ConfigureParquetDecodeMaxConcurrent(1)
	t.Cleanup(func() { ConfigureParquetDecodeMaxConcurrent(0) })

	release, err := acquireParquetDecode(context.Background())
	if err != nil {
		t.Fatalf("acquire first slot: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := acquireParquetDecode(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked acquire error = %v, want context canceled", err)
	}
}
