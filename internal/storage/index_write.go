package storage

import (
	"context"
	"fmt"
	"sync"
)

const indexWriteConcurrency = 4

func runIndexWriteJobs(ctx context.Context, count int, fn func(context.Context, int) error) error {
	if count == 0 {
		return nil
	}
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := min(indexWriteConcurrency, count)
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for {
				select {
				case <-writeCtx.Done():
					return
				case index, ok := <-jobs:
					if !ok {
						return
					}
					if err := runIndexWriteJob(writeCtx, index, fn); err != nil {
						select {
						case errCh <- err:
						default:
						}
						cancel()
						return
					}
				}
			}
		}()
	}

dispatch:
	for index := 0; index < count; index++ {
		select {
		case jobs <- index:
		case <-writeCtx.Done():
			break dispatch
		}
	}
	close(jobs)
	group.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func runIndexWriteJob(ctx context.Context, index int, fn func(context.Context, int) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return fn(ctx, index)
}

func (s *TenantStore) putBytesWithMeta(ctx context.Context, key string, data []byte, meta ObjectMeta) error {
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	_, err := s.Objects.PutConditional(ctx, key, data, condition)
	return err
}

func (s *TenantStore) putBytesWithMetaResult(ctx context.Context, key string, data []byte, meta ObjectMeta) (ObjectMeta, error) {
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	return s.Objects.PutConditional(ctx, key, data, condition)
}
