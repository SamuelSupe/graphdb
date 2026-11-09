package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// ValidateSnapshot reads a captured local replica, including uncommitted roots.
// Remote partitions are validated by their own replica snapshots.
func (s Store) ValidateSnapshot(ctx context.Context, group string) error {
	prefix := path.Join(s.Prefix, "control/graphs") + "/"
	return s.walkObjects(ctx, prefix, func(object storage.ObjectInfo) error {
		key := object.Key
		if !strings.HasSuffix(key, ".json") {
			return nil
		}
		var raw json.RawMessage
		if err := s.load(ctx, key, &raw); err != nil {
			return err
		}
		validateRoot := func(tenant string, generation int64, id uint8, root Root) error {
			return s.Tree(tenant, generation, id).Scan(ctx, root, "", "", func(Row) (bool, error) { return true, nil })
		}
		if strings.HasSuffix(key, "/state.json") {
			var state DataState
			if err := json.Unmarshal(raw, &state); err != nil {
				return err
			}
			if key != s.dataKey(state.Tenant, state.Generation, state.Partition) {
				return fmt.Errorf("partition snapshot scope mismatch")
			}
			if _, err := s.Data(ctx, state.Tenant, state.Generation, state.Partition); err != nil {
				return err
			}
			if err := validateRoot(state.Tenant, state.Generation, state.Partition, state.Root); err != nil {
				return err
			}
			if state.GC != nil && state.GC.Phase != "cleanup" {
				if !validTokenID(state.GC.ID) {
					return fmt.Errorf("invalid partition GC identity")
				}
				for _, root := range state.GC.Stack {
					if err := validateRoot(state.Tenant, state.Generation, state.Partition, root); err != nil {
						return err
					}
				}
				if err := s.gcTree(state, "seen").Scan(ctx, state.GC.Seen, "", "", func(Row) (bool, error) { return true, nil }); err != nil {
					return err
				}
				if err := s.gcTree(state, "marks").Scan(ctx, state.GC.Marked, "", "", func(row Row) (bool, error) {
					if !validDigest(row.Key) {
						return false, fmt.Errorf("invalid marked page digest")
					}
					data, err := s.Objects.Get(ctx, s.Tree(state.Tenant, state.Generation, state.Partition).pageKey(row.Key))
					if err != nil {
						return false, err
					}
					return true, verifyObject(row.Key, data)
				}); err != nil {
					return err
				}
			}
			if state.Capture != "" {
				if !validTokenID(state.Capture) {
					return fmt.Errorf("invalid partition delta capture identity")
				}
				if err := s.deltaTree(state.Tenant, state.Generation, state.Partition, state.Capture).Scan(ctx, state.CaptureRoot, "", "", func(row Row) (bool, error) {
					if !validDigest(row.Key) {
						return false, fmt.Errorf("invalid captured page digest")
					}
					data, err := s.Objects.Get(ctx, s.Tree(state.Tenant, state.Generation, state.Partition).pageKey(row.Key))
					if err != nil {
						return false, err
					}
					return true, verifyObject(row.Key, data)
				}); err != nil {
					return err
				}
			}
			if state.Prepared != nil {
				if err := validateRoot(state.Tenant, state.Generation, state.Partition, state.Prepared.Base); err != nil {
					return err
				}
				if err := validateRoot(state.Tenant, state.Generation, state.Partition, state.Prepared.Root); err != nil {
					return err
				}
			}
		} else if strings.HasSuffix(key, "/graph.json") || strings.Contains(key, "/views/") {
			var g Graph
			if err := json.Unmarshal(raw, &g); err != nil {
				return err
			}
			if g.Format != FormatVersion || storage.ValidateTenantID(g.Tenant) != nil || g.Generation <= 0 {
				return fmt.Errorf("invalid graph snapshot scope")
			}
			placements := g.Partitions
			if current, err := s.GetGraph(ctx, g.Tenant); err == nil && current.Generation == g.Generation {
				placements = current.Partitions
			} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			for i, root := range g.Roots {
				if placements[i].Group != group {
					continue
				}
				if _, err := s.Data(ctx, g.Tenant, g.Generation, uint8(i)); err != nil {
					return fmt.Errorf("required graph partition missing: %w", err)
				}
				if err := validateRoot(g.Tenant, g.Generation, uint8(i), root); err != nil {
					return err
				}
			}
			if g.Active != nil {
				for _, input := range g.Active.Inputs {
					if _, err := s.Input(ctx, g.Tenant, g.Generation, input); err != nil {
						return err
					}
				}
			}
		} else if strings.Contains(key, "/transactions/") {
			var txn Transaction
			if err := json.Unmarshal(raw, &txn); err != nil {
				return err
			}
			if key != s.txnKey(txn.Tenant, txn.Generation, txn.Identity) {
				return fmt.Errorf("transaction snapshot scope mismatch")
			}
			for _, input := range txn.Inputs {
				if _, err := s.Input(ctx, txn.Tenant, txn.Generation, input); err != nil {
					return err
				}
			}
		} else if strings.Contains(key, "/pins/") {
			var pin Pin
			if err := json.Unmarshal(raw, &pin); err != nil {
				return err
			}
			relative := strings.TrimPrefix(key, prefix)
			tenant := strings.SplitN(relative, "/", 2)[0]
			if !validTokenID(pin.ID) || key != s.pinKey(tenant, pin.Generation, pin.ID) {
				return fmt.Errorf("read pin snapshot scope mismatch")
			}
			if _, err := s.Snapshot(ctx, tenant, pin.Generation, pin.Version); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s Store) walkObjects(ctx context.Context, prefix string, visit func(storage.ObjectInfo) error) error {
	if paged, ok := s.Objects.(interface {
		ListPage(context.Context, string, string, int) ([]storage.ObjectInfo, string, error)
	}); ok {
		cursor := ""
		for {
			objects, next, err := paged.ListPage(ctx, prefix, cursor, 128)
			if err != nil {
				return err
			}
			for _, object := range objects {
				if err := visit(object); err != nil {
					return err
				}
			}
			if next == "" {
				return nil
			}
			if next <= cursor {
				return fmt.Errorf("object listing made no progress")
			}
			cursor = next
		}
	}
	objects, err := s.Objects.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := visit(object); err != nil {
			return err
		}
	}
	return nil
}

// verifyObject is shared by replica transfer and snapshot checks.
func verifyObject(key string, data []byte) error {
	if !validDigest(path.Base(key)) || fmt.Sprintf("%x", sha256.Sum256(data)) != path.Base(key) {
		return fmt.Errorf("partition object checksum mismatch")
	}
	if len(data) == 0 {
		return errors.New("empty partition object")
	}
	return nil
}
