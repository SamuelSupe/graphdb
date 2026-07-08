package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type entityRecordWriteJob struct {
	Key    string
	Record EntityRecord
}

func (s *TenantStore) loadEntityRecord(ctx context.Context, tenantID string, id string) (EntityRecord, error) {
	key := s.entityRecordKey(tenantID, id)
	return s.loadEntityRecordKey(ctx, tenantID, key)
}

func (s *TenantStore) loadEntityRecordKey(ctx context.Context, tenantID string, key string) (EntityRecord, error) {
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return EntityRecord{}, err
	}
	id, _, parseErr := s.entityIDFromRecordKey(tenantID, key)
	if parseErr != nil {
		return EntityRecord{}, parseErr
	}
	return decodeEntityRecordObject(ctx, data, key, tenantID, id)
}

func decodeEntityRecordObject(ctx context.Context, data []byte, key string, tenantID string, id string) (EntityRecord, error) {
	if !isParquetBytes(data) {
		return EntityRecord{}, fmt.Errorf("unsupported entity record: only parquet entity records are readable")
	}
	return decodeParquetEntityRecord(ctx, data, tenantID, id)
}

func (s *TenantStore) putEntityRecordBatch(ctx context.Context, jobs []entityRecordWriteJob) error {
	if len(jobs) == 0 {
		return nil
	}
	workers := indexWriteConcurrency
	if len(jobs) < workers {
		workers = len(jobs)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobCh := make(chan entityRecordWriteJob)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobCh:
					if !ok {
						return
					}
					if err := s.putEntityRecordIfChanged(ctx, job); err != nil {
						select {
						case errCh <- err:
							cancel()
						default:
						}
						return
					}
				}
			}
		}()
	}
	var sendErr error
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			sendErr = ctx.Err()
			goto wait
		case jobCh <- job:
		}
	}
wait:
	close(jobCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return sendErr
	}
}

func (s *TenantStore) putEntityRecordIfChanged(ctx context.Context, job entityRecordWriteJob) error {
	data, err := marshalParquetEntityRecord(ctx, job.Record)
	if err != nil {
		return err
	}
	mayExist, err := s.objectKeyMayExist(ctx, job.Key)
	if err != nil {
		return err
	}
	if !mayExist {
		if _, err := s.Objects.PutConditional(ctx, job.Key, data, PutCondition{IfNoneMatch: true}); err == nil {
			s.markObjectKeyCached(job.Key)
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
	}
	existing, meta, err := s.Objects.GetWithMeta(ctx, job.Key)
	if errors.Is(err, ErrNotFound) {
		if _, err := s.Objects.PutConditional(ctx, job.Key, data, PutCondition{IfNoneMatch: true}); err == nil {
			s.markObjectKeyCached(job.Key)
			return nil
		} else {
			return err
		}
	}
	if err != nil {
		return err
	}
	got, err := decodeEntityRecordObject(ctx, existing, job.Key, job.Record.TenantID, job.Record.ID)
	if err == nil {
		if entityRecordContentHash(got) == entityRecordContentHash(job.Record) && got.Version <= job.Record.Version {
			return nil
		}
		if got.Version > job.Record.Version {
			return fmt.Errorf("%w: entity record %q is newer than rebuild target", ErrConflict, job.Key)
		}
	}
	if err := s.putBytesWithMeta(ctx, job.Key, data, meta); err != nil {
		return err
	}
	s.markObjectKeyCached(job.Key)
	return nil
}

func (s *TenantStore) putEntityRecordWithMeta(ctx context.Context, key string, record EntityRecord, meta ObjectMeta) error {
	data, err := marshalParquetEntityRecord(ctx, record)
	if err != nil {
		return err
	}
	return s.putBytesWithMeta(ctx, key, data, meta)
}
