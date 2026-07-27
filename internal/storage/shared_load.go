package storage

import (
	"context"
	"errors"
)

func loadCanceledByContext(ctx context.Context, err error) bool {
	if ctx == nil || err == nil {
		return false
	}
	ctxErr := ctx.Err()
	return ctxErr != nil && errors.Is(err, ctxErr)
}
