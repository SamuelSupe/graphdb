package storage

import (
	"context"
	"errors"
	"fmt"
)

func (s *TenantStore) scanDeadLetters(
	ctx context.Context,
	tenantID string,
	source string,
	visit func(DeadLetter) error,
) error {
	return s.walkDeadLettersByKey(
		ctx,
		tenantID,
		source,
		"",
		func(item DeadLetter) (bool, error) {
			return true, visit(item)
		},
	)
}

func (s *TenantStore) walkDeadLettersByKey(
	ctx context.Context,
	tenantID string,
	source string,
	after string,
	visit func(DeadLetter) (bool, error),
) error {
	prefix := s.deadLetterPrefix(tenantID, source)
	cursor := after
	for {
		objects, next, err := listObjectPage(
			ctx,
			s.Objects,
			prefix,
			cursor,
			objectPrefixScanPageSize,
		)
		if err != nil {
			return err
		}
		for _, object := range objects {
			s.clearCoordinatedWriterObjectKey(object.Key)
			item, ok, err := s.loadListedDeadLetter(
				ctx, tenantID, source, object,
			)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			keepGoing, err := visit(item)
			if err != nil {
				return err
			}
			if !keepGoing {
				return nil
			}
		}
		if next == "" {
			return nil
		}
		if next <= cursor {
			return fmt.Errorf(
				"object list cursor did not advance for prefix %q", prefix,
			)
		}
		cursor = next
	}
}

func (s *TenantStore) loadListedDeadLetter(
	ctx context.Context,
	tenantID string,
	source string,
	object ObjectInfo,
) (DeadLetter, bool, error) {
	data, meta, err := s.Objects.GetWithMeta(ctx, object.Key)
	if errors.Is(err, ErrNotFound) {
		return DeadLetter{}, false, nil
	}
	if err != nil {
		return DeadLetter{}, false, err
	}
	if !isParquetBytes(data) {
		return invalidDeadLetter(
			tenantID,
			source,
			object.Key,
			fmt.Errorf(
				"unsupported deadletter object: only parquet deadletters are readable",
			),
		), true, nil
	}
	item, err := decodeParquetDeadLetter(ctx, data)
	if err != nil {
		return invalidDeadLetter(
			tenantID, source, object.Key, err,
		), true, nil
	}
	item.objectKey = object.Key
	item.objectMeta = meta
	if err := validateDeadLetterRecord(
		tenantID, source, item,
	); err != nil {
		return invalidDeadLetter(
			tenantID, source, object.Key, err,
		), true, nil
	}
	return item, true, nil
}

func deadLetterBefore(left DeadLetter, right DeadLetter) bool {
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.Before(right.CreatedAt)
	}
	if left.objectKey != right.objectKey {
		return left.objectKey < right.objectKey
	}
	return left.ID < right.ID
}
