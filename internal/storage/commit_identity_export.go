package storage

import (
	"context"
	"fmt"
	"path"
	"strings"
)

// NormalizeDirectCommitRequest preserves the canonical request comparison
// used by direct-commit idempotency across storage layouts and time zones.
func NormalizeDirectCommitRequest(request DirectCommitRequest) DirectCommitRequest {
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Mutations = mutationTimesUTC(request.Mutations)
	return request
}

// ListDirectCommitRecords exports bounded pages from the frozen tenant. A
// prepared record is exported only after its publication can be established.
func (s *TenantStore) ListDirectCommitRecords(ctx context.Context, tenant, cursor string, limit int) ([]DirectCommitRecord, string, error) {
	if ValidateTenantID(tenant) != nil || limit < 1 || limit > 128 {
		return nil, "", fmt.Errorf("%w: invalid identity export", ErrInvalidArgument)
	}
	prefix := path.Join(s.tenantObjectPrefix(tenant), "idempotency/commits") + "/"
	if cursor != "" && !strings.HasPrefix(cursor, prefix) {
		return nil, "", fmt.Errorf("%w: invalid identity export cursor", ErrInvalidArgument)
	}
	objects, next, err := listObjectPage(ctx, s.Objects, prefix, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	var records []DirectCommitRecord
	for _, object := range objects {
		if !strings.HasSuffix(object.Key, ".parquet") {
			continue
		}
		record, _, err := s.loadDirectCommitRecordWithMeta(ctx, object.Key)
		if err != nil {
			return nil, "", err
		}
		if err := validateDirectCommitRecord(tenant, record.Request, record); err != nil {
			return nil, "", err
		}
		if object.Key != s.commitIdempotencyKey(tenant, record.Request.IdempotencyKey) {
			return nil, "", fmt.Errorf("idempotency export path mismatch")
		}
		status := directCommitRecordStatus(record)
		if status == directCommitStatusPrepared {
			published, decisive, err := s.directCommitPreparedPublished(ctx, tenant, record)
			if err != nil {
				return nil, "", err
			}
			if !decisive {
				return nil, "", fmt.Errorf("%w: source commit outcome must be resolved before conversion", ErrConflict)
			}
			if published {
				status = directCommitStatusCommitted
			} else {
				status = directCommitStatusPending
			}
		}
		if status != directCommitStatusCommitted && status != directCommitStatusPending {
			return nil, "", fmt.Errorf("unknown source direct-commit status")
		}
		record.Status = status
		record.TenantID = tenant
		record.Request = NormalizeDirectCommitRequest(record.Request)
		records = append(records, record)
	}
	return records, next, nil
}
