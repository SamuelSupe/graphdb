package partition

import (
	"context"
	"fmt"
)

// A plan and its dependency rounds read the same immutable root vector.
// A cached nil owner list also records a directory miss in that view.
type directoryReads struct {
	ctx    context.Context
	view   View
	owners map[string][]string
}

func (d *directoryReads) get(key string) ([]string, error) {
	if owners, ok := d.owners[key]; ok {
		return owners, nil
	}
	var owners []string
	if _, err := d.view.Get(d.ctx, Bucket(key), key, &owners); err != nil {
		return nil, err
	}
	d.owners[key] = owners
	return owners, nil
}

func (d *directoryReads) prefetch(keys []string) error {
	var requests []ReadRequest
	seen := make(map[string]bool)
	for _, key := range keys {
		if _, ok := d.owners[key]; ok || seen[key] {
			continue
		}
		seen[key] = true
		// Each started batch request has enough response budget for one value.
		requests = append(requests, ReadRequest{Partition: Bucket(key), Keys: []string{key}})
	}
	if len(requests) == 0 {
		return nil
	}
	if len(requests) == 1 {
		_, err := d.get(requests[0].Keys[0])
		return err
	}
	results, err := d.view.readBatch(d.ctx, requests)
	if err != nil {
		return err
	}
	for i, result := range results {
		key := requests[i].Keys[0]
		if len(result.Remaining) > 0 || len(result.Rows) > 1 || len(result.Rows) == 1 && result.Rows[0].Key != key {
			return fmt.Errorf("%w: unexpected directory batch result", ErrPartitionCorrupt)
		}
		var owners []string
		if len(result.Rows) == 1 {
			if err := decodeRecord(result.Rows[0], &owners); err != nil {
				return err
			}
		}
		d.owners[key] = owners
	}
	return nil
}
