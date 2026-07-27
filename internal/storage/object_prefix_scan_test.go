package storage

import (
	"context"
	"fmt"
	"testing"
)

type listOnlyCountingStore struct {
	ObjectStore
	items []ObjectInfo
	calls int
}

func (s *listOnlyCountingStore) List(
	context.Context,
	string,
) ([]ObjectInfo, error) {
	s.calls++
	return append([]ObjectInfo(nil), s.items...), nil
}

func TestScanObjectPrefixFreshListsNonPagedStoreOnce(t *testing.T) {
	const itemCount = objectPrefixScanPageSize*2 + 1
	items := make([]ObjectInfo, 0, itemCount)
	for index := 0; index < itemCount; index++ {
		items = append(items, ObjectInfo{
			Key: fmt.Sprintf("objects/%05d", index),
		})
	}
	objects := &listOnlyCountingStore{
		ObjectStore: NewMemoryStore(),
		items:       items,
	}
	visited := 0
	err := scanObjectPrefixFresh(
		context.Background(),
		objects,
		"objects/",
		func(page []ObjectInfo) error {
			visited += len(page)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("scan fresh prefix: %v", err)
	}
	if visited != itemCount {
		t.Fatalf("visited = %d, want %d", visited, itemCount)
	}
	if objects.calls != 1 {
		t.Fatalf(
			"non-paged store listed %d times, want one complete listing",
			objects.calls,
		)
	}
}
