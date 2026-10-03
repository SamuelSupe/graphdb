package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ValidateTenantMigrationSource cold-loads the published graph and relation
// schemas, including required objects and the current graph digest. It also
// validates tenant controls and encodings that copy will rewrite. The caller
// must pin or freeze the input until it is copied. Invalid imported data is a
// rejected input; local object IO failures still poison a replicated application.
// declared, when non-nil, is the complete set of objects decoded from a transfer:
// losing an object in that set is local corruption, not an omitted input object.
func ValidateTenantMigrationSource(ctx context.Context, source *TenantStore, tenantID string, declared map[string]bool) error {
	if source == nil {
		return fmt.Errorf("source store is required")
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	objects := &tenantMigrationValidationStore{
		ObjectStore: unwrapTenantMigrationStore(source.Objects),
		journal:     ctx.Value(replicationJournalKey{}),
		declared:    declared,
	}
	// Decoding an unpublished input must not be treated as corruption of this
	// replica's live graph. Object reads restore the journal for actual IO errors.
	ctx = context.WithValue(ctx, replicationJournalKey{}, struct{}{})
	cold := NewTenantStore(objects, source.Prefix)
	manifest, meta, err := cold.readManifest(ctx, tenantID, true)
	if err != nil {
		return err
	}
	if !meta.Exists {
		return ErrNotFound
	}
	loaded, err := cold.loadManifestGraph(ctx, tenantID, manifest, meta)
	if err != nil {
		return err
	}
	schemas, err := cold.GetRelationSchemas(ctx, tenantID)
	if err != nil {
		return err
	}
	if err := validateRelationSchemaGraph(loaded.Graph, schemas); err != nil {
		return err
	}
	if _, _, _, err := cold.getTenantMetadataWithMeta(ctx, tenantID); err != nil {
		return err
	}
	if _, _, err := cold.GetTenantConfig(ctx, tenantID); err != nil {
		return err
	}
	if _, _, err := cold.GetSourcePolicy(ctx, tenantID); err != nil {
		return err
	}
	catalog, err := cold.GetIndexCatalog(ctx, tenantID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if _, err := cold.GetReverseIndexCatalog(ctx, tenantID, catalog.Version); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return objects.validateEncodings(ctx, cold, tenantID)
}

type tenantMigrationValidationStore struct {
	ObjectStore
	journal  any
	declared map[string]bool
}

func (s *tenantMigrationValidationStore) readContext(ctx context.Context) context.Context {
	if s.journal != nil {
		return context.WithValue(ctx, replicationJournalKey{}, s.journal)
	}
	return ctx
}

func (s *tenantMigrationValidationStore) Get(ctx context.Context, key string) ([]byte, error) {
	ctx = s.readContext(ctx)
	data, err := s.ObjectStore.Get(ctx, key)
	s.recordReadFailure(ctx, key, err)
	return data, err
}

func (s *tenantMigrationValidationStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	ctx = s.readContext(ctx)
	data, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	s.recordReadFailure(ctx, key, err)
	return data, meta, err
}

func (s *tenantMigrationValidationStore) recordReadFailure(ctx context.Context, key string, err error) {
	if s.declared[key] && errors.Is(err, ErrNotFound) {
		// The payload already supplied this file. Preserve the replica-failure
		// marker even though missing undeclared dependencies are business errors.
		recordReplicationFailure(ctx, fmt.Errorf("declared migration object %q was lost locally: %v", key, err))
		return
	}
	recordReplicationFailure(ctx, err)
}

func (s *tenantMigrationValidationStore) validateEncodings(ctx context.Context, cold *TenantStore, tenantID string) error {
	prefix := cold.tenantObjectPrefix(tenantID)
	var keys []string
	appendKey := func(key string) {
		if strings.HasPrefix(key, prefix) && key != cold.manifestKey(tenantID) && tenantMigrationObjectNeedsRewrite(strings.TrimPrefix(key, prefix)) {
			keys = append(keys, key)
		}
	}
	if s.declared != nil {
		for key, present := range s.declared {
			if present {
				appendKey(key)
			}
		}
	} else {
		if err := scanObjectPrefix(s.readContext(ctx), s.ObjectStore, prefix, func(objects []ObjectInfo) error {
			for _, object := range objects {
				appendKey(object.Key)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		data, err := s.Get(ctx, key)
		if err != nil {
			return err
		}
		// The same decoder/encoder used by copy must succeed before an install
		// changes live incarnation controls. Orphan and optional catalogs can
		// otherwise turn a deterministic bad payload into a group-wide apply failure.
		if _, _, err := cold.rewriteTenantMigrationObject(ctx, data, key, tenantID, tenantID, prefix, prefix, nil, writerFenceRef{}); err != nil {
			return fmt.Errorf("migration object %q: %w", key, err)
		}
	}
	return nil
}
