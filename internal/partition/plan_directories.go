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
	byPartition := make(map[uint8][]string)
	missing := make(map[string]bool)
	count := 0
	var single string
	for _, key := range keys {
		if _, ok := d.owners[key]; ok || missing[key] {
			continue
		}
		missing[key] = true
		single = key
		count++
		bucket := Bucket(key)
		byPartition[bucket] = append(byPartition[bucket], key)
	}
	if count == 0 {
		return nil
	}
	if count == 1 {
		_, err := d.get(single)
		return err
	}
	if err := d.view.visitKeys(d.ctx, byPartition, func(bucket uint8, row Row) error {
		if !missing[row.Key] || Bucket(row.Key) != bucket {
			return fmt.Errorf("%w: unexpected directory batch result", ErrPartitionCorrupt)
		}
		var owners []string
		if err := decodeRecord(row, &owners); err != nil {
			return err
		}
		d.owners[row.Key] = owners
		delete(missing, row.Key)
		return nil
	}); err != nil {
		return err
	}
	for key := range missing {
		d.owners[key] = nil
	}
	return nil
}
