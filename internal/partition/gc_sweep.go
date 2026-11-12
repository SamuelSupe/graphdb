package partition

import (
	"context"
	"fmt"
	"path"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const gcSweepLimit = 128

type GCSweepPage struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

type GCSweepPlan struct {
	Pages []GCSweepPage `json:"pages,omitempty"`
	More  bool          `json:"more,omitempty"`
}

// PlanGCSweep enumerates immutable object names. A concurrent prepare may add
// pages to the live mark set, so only application can decide what to delete.
func (s Store) PlanGCSweep(ctx context.Context, a *Action, state DataState) error {
	if state.GC == nil || state.GC.ID != a.Txn || state.GC.Phase != "sweep" ||
		state.Epoch != a.Epoch || state.State != "active" || state.Capture != "" {
		return nil
	}
	prefix := s.Tree(a.Tenant, a.Generation, a.Partition).Prefix + "/"
	objects, next, err := s.objectPage(ctx, prefix, state.GC.Cursor, gcSweepLimit)
	if err != nil {
		return err
	}
	plan := &GCSweepPlan{Pages: make([]GCSweepPage, 0, len(objects)), More: next != ""}
	for _, object := range objects {
		hash := path.Base(object.Key)
		if !validDigest(hash) || object.Key != prefix+hash[:2]+"/"+hash {
			return fmt.Errorf("%w: invalid partition page in GC", ErrPartitionCorrupt)
		}
		plan.Pages = append(plan.Pages, GCSweepPage{Hash: hash, Size: object.Size})
	}
	a.ExpectedCursor = state.GC.Cursor
	a.GCSweep, a.Revision = plan, PlannedGCRevision
	return nil
}

func (s Store) gcSweepPage(ctx context.Context, a Action, prefix, cursor string, limit int) ([]storage.ObjectInfo, string, error) {
	if a.GCSweep == nil {
		return s.objectPage(ctx, prefix, cursor, limit)
	}
	plan := a.GCSweep
	if len(plan.Pages) > gcSweepLimit || plan.More && len(plan.Pages) == 0 {
		return nil, "", invalid("invalid GC sweep candidate count")
	}
	objects := make([]storage.ObjectInfo, 0, len(plan.Pages))
	for _, page := range plan.Pages {
		if !validDigest(page.Hash) || page.Size <= 0 || page.Size > MaxChangeBytes {
			return nil, "", invalid("invalid GC sweep candidate")
		}
		key := prefix + page.Hash[:2] + "/" + page.Hash
		if key <= cursor {
			return nil, "", invalid("GC sweep candidates out of order")
		}
		objects = append(objects, storage.ObjectInfo{Key: key, Size: page.Size})
		cursor = key
	}
	if plan.More {
		return objects, cursor, nil
	}
	return objects, "", nil
}
