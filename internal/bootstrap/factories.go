package bootstrap

import (
	"context"
	"fmt"
	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func newCoordinator(ctx context.Context, cfg config.Config) (storage.WriteCoordinator, error) {
	if cfg.CoordinationMode() != storage.CoordinationLocal {
		return nil, fmt.Errorf("PostgreSQL coordination is unsupported in the local disk edition")
	}
	return nil, nil
}
