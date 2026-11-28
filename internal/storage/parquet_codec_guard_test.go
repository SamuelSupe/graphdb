package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

// TestParquetCodecReflectiveFullFieldRoundTrip guards every manually column-mapped
// Parquet codec against silently omitting newly added exported struct fields.
// Any exported field added to SavedQuery (including query.Request),
// IngestBatchRecord, DeadLetter, IndexCatalog, TenantMetadata, or TenantConfig
// is automatically populated with non-zero values via reflection and must survive
// Parquet marshal -> decode with lossless equality.
func TestParquetCodecReflectiveFullFieldRoundTrip(t *testing.T) {
	ctx := context.Background()

	t.Run("SavedQuery", func(t *testing.T) {
		var saved SavedQuery
		pop := newReflectPopulator(map[string]bool{})
		pop.fillValue(reflect.ValueOf(&saved).Elem(), "SavedQuery", 0)
		assertAllExportedNonZero(t, reflect.ValueOf(saved), "SavedQuery", nil)

		data, err := marshalParquetSavedQuery(ctx, saved)
		if err != nil {
			t.Fatalf("marshalParquetSavedQuery: %v", err)
		}
		got, err := decodeParquetSavedQuery(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetSavedQuery: %v", err)
		}
		wantNorm, wantHash, err := normalizeSavedQueryForParquet(saved)
		if err != nil {
			t.Fatalf("normalizeSavedQueryForParquet(want): %v", err)
		}
		gotNorm, gotHash, err := normalizeSavedQueryForParquet(got)
		if err != nil {
			t.Fatalf("normalizeSavedQueryForParquet(got): %v", err)
		}
		if wantHash != gotHash || !reflect.DeepEqual(wantNorm, gotNorm) {
			wantJSON, _ := json.MarshalIndent(wantNorm, "", "  ")
			gotJSON, _ := json.MarshalIndent(gotNorm, "", "  ")
			t.Fatalf("SavedQuery Parquet round-trip lost or altered fields\nhash want=%s got=%s\nwant=%s\ngot=%s", wantHash, gotHash, wantJSON, gotJSON)
		}
	})

	for _, tc := range []struct {
		name  string
		epoch uint64
	}{
		{name: "IngestBatchRecord_Full", epoch: 7},
		{name: "IngestBatchRecord_Sparse", epoch: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var record IngestBatchRecord
			skip := map[string]bool{
				// Intentionally cleared by ingestRecordPayloadJSON because SkipReason is per-attempt response metadata.
				"IngestBatchRecord.Result.SkipReason": true,
			}
			if tc.epoch == 0 {
				skip["IngestBatchRecord.ReplicatedReceipt.Epoch"] = true
			}
			pop := newReflectPopulator(skip)
			pop.fillValue(reflect.ValueOf(&record).Elem(), "IngestBatchRecord", 0)
			record.ReplicatedReceipt.Epoch = tc.epoch
			record.ReplicatedReceipt.Digest = strings.Repeat("a", 64)
			alignEntityFieldWriteModes(record.Request.Items)
			assertAllExportedNonZero(t, reflect.ValueOf(record), "IngestBatchRecord", skip)

			data, err := marshalParquetIngestRecord(ctx, record)
			if err != nil {
				t.Fatalf("marshalParquetIngestRecord: %v", err)
			}
			got, err := decodeParquetIngestRecord(ctx, data)
			if err != nil {
				t.Fatalf("decodeParquetIngestRecord: %v", err)
			}
			wantNorm, wantHash, err := normalizeIngestRecordForParquet(record)
			if err != nil {
				t.Fatalf("normalizeIngestRecordForParquet(want): %v", err)
			}
			gotNorm, gotHash, err := normalizeIngestRecordForParquet(got)
			if err != nil {
				t.Fatalf("normalizeIngestRecordForParquet(got): %v", err)
			}
			if wantHash != gotHash || !reflect.DeepEqual(wantNorm, gotNorm) {
				wantJSON, _ := json.MarshalIndent(wantNorm, "", "  ")
				gotJSON, _ := json.MarshalIndent(gotNorm, "", "  ")
				t.Fatalf("%s Parquet round-trip lost or altered fields\nhash want=%s got=%s\nwant=%s\ngot=%s", tc.name, wantHash, gotHash, wantJSON, gotJSON)
			}
		})
	}

	t.Run("DeadLetter", func(t *testing.T) {
		var letter DeadLetter
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&letter).Elem(), "DeadLetter", 0)
		alignEntityFieldWriteModes(letter.Request.Items)
		assertAllExportedNonZero(t, reflect.ValueOf(letter), "DeadLetter", nil)

		data, err := marshalParquetDeadLetter(ctx, letter)
		if err != nil {
			t.Fatalf("marshalParquetDeadLetter: %v", err)
		}
		got, err := decodeParquetDeadLetter(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetDeadLetter: %v", err)
		}
		wantNorm, wantHash, err := normalizeDeadLetterForParquet(letter)
		if err != nil {
			t.Fatalf("normalizeDeadLetterForParquet(want): %v", err)
		}
		gotNorm, gotHash, err := normalizeDeadLetterForParquet(got)
		if err != nil {
			t.Fatalf("normalizeDeadLetterForParquet(got): %v", err)
		}
		if wantHash != gotHash || !reflect.DeepEqual(wantNorm, gotNorm) {
			wantJSON, _ := json.MarshalIndent(wantNorm, "", "  ")
			gotJSON, _ := json.MarshalIndent(gotNorm, "", "  ")
			t.Fatalf("DeadLetter Parquet round-trip lost or altered fields\nhash want=%s got=%s\nwant=%s\ngot=%s", wantHash, gotHash, wantJSON, gotJSON)
		}
	})

	t.Run("IndexCatalog", func(t *testing.T) {
		var catalog IndexCatalog
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&catalog).Elem(), "IndexCatalog", 0)
		catalog.LayoutVersion = CurrentObjectLayoutVersion
		for i := range catalog.EdgeShards {
			if catalog.EdgeShards[i].AdjacencyRoot != nil {
				catalog.EdgeShards[i].AdjacencyRoot.Count = catalog.EdgeShards[i].EdgeCount
				catalog.EdgeShards[i].AdjacencyRoot.Height = 3
				catalog.EdgeShards[i].AdjacencyRoot.Hash = strings.Repeat("b", 64)
			}
		}
		assertAllExportedNonZero(t, reflect.ValueOf(catalog), "IndexCatalog", nil)

		data, err := marshalParquetIndexCatalog(ctx, catalog)
		if err != nil {
			t.Fatalf("marshalParquetIndexCatalog: %v", err)
		}
		got, err := decodeParquetIndexCatalog(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetIndexCatalog: %v", err)
		}
		wantNorm := normalizeIndexCatalogForParquet(catalog)
		gotNorm := normalizeIndexCatalogForParquet(got)
		gotNorm.contentHash = ""
		wantJSON, _ := json.MarshalIndent(wantNorm, "", "  ")
		gotJSON, _ := json.MarshalIndent(gotNorm, "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("IndexCatalog Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("TenantMetadata", func(t *testing.T) {
		var meta TenantMetadata
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&meta).Elem(), "TenantMetadata", 0)
		meta.Status = TenantStatusDisabled
		assertAllExportedNonZero(t, reflect.ValueOf(meta), "TenantMetadata", nil)

		data, err := marshalParquetTenantMetadata(ctx, meta)
		if err != nil {
			t.Fatalf("marshalParquetTenantMetadata: %v", err)
		}
		got, err := decodeParquetTenantMetadata(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetTenantMetadata: %v", err)
		}
		wantNorm, err := normalizeTenantMetadataForParquet(meta)
		if err != nil {
			t.Fatalf("normalizeTenantMetadataForParquet(want): %v", err)
		}
		gotNorm, err := normalizeTenantMetadataForParquet(got)
		if err != nil {
			t.Fatalf("normalizeTenantMetadataForParquet(got): %v", err)
		}
		if !reflect.DeepEqual(wantNorm, gotNorm) {
			wantJSON, _ := json.MarshalIndent(wantNorm, "", "  ")
			gotJSON, _ := json.MarshalIndent(gotNorm, "", "  ")
			t.Fatalf("TenantMetadata Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("TenantConfig", func(t *testing.T) {
		var rec tenantConfigRecord
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&rec).Elem(), "tenantConfigRecord", 0)
		// Ensure validation invariants: backup interval >= 60s, merge < split, retry_initial <= retry_max.
		intervalVal := int64(300)
		rec.Config.Backup.IntervalSeconds = &intervalVal
		if rec.Config.Maintenance.EntityPageMergeThreshold != nil && rec.Config.Maintenance.EntityPageSplitThreshold != nil {
			mergeVal := 10
			splitVal := 100
			rec.Config.Maintenance.EntityPageMergeThreshold = &mergeVal
			rec.Config.Maintenance.EntityPageSplitThreshold = &splitVal
		}
		if rec.Config.Maintenance.EdgeShardMergeThreshold != nil && rec.Config.Maintenance.EdgeShardSplitThreshold != nil {
			mergeVal := 20
			splitVal := 200
			rec.Config.Maintenance.EdgeShardMergeThreshold = &mergeVal
			rec.Config.Maintenance.EdgeShardSplitThreshold = &splitVal
		}
		if rec.Config.Backup.RetryInitialSeconds != nil && rec.Config.Backup.RetryMaxSeconds != nil {
			initVal := int64(5)
			maxVal := int64(60)
			rec.Config.Backup.RetryInitialSeconds = &initVal
			rec.Config.Backup.RetryMaxSeconds = &maxVal
		}
		assertAllExportedNonZero(t, reflect.ValueOf(rec), "tenantConfigRecord", nil)

		data, err := marshalParquetTenantConfig(ctx, rec)
		if err != nil {
			t.Fatalf("marshalParquetTenantConfig: %v", err)
		}
		got, err := decodeParquetTenantConfig(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetTenantConfig: %v", err)
		}
		if !reflect.DeepEqual(rec, got) {
			wantJSON, _ := json.MarshalIndent(rec, "", "  ")
			gotJSON, _ := json.MarshalIndent(got, "", "  ")
			t.Fatalf("TenantConfig Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("Manifest", func(t *testing.T) {
		var manifest Manifest
		pop := newReflectPopulator(nil)
		pop.includeNonJSON = true
		pop.fillValue(reflect.ValueOf(&manifest).Elem(), "Manifest", 0)
		manifest.LayoutVersion = CurrentObjectLayoutVersion
		assertAllExportedNonZeroOpts(t, reflect.ValueOf(manifest), "Manifest", nil, true)

		data, err := marshalParquetManifest(ctx, manifest)
		if err != nil {
			t.Fatalf("marshalParquetManifest: %v", err)
		}
		got, err := decodeParquetManifest(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetManifest: %v", err)
		}
		wantNorm := normalizeManifestForParquet(manifest)
		gotNorm := normalizeManifestForParquet(got)
		if !reflect.DeepEqual(wantNorm, gotNorm) {
			t.Fatalf("Manifest Parquet round-trip lost or altered fields\nwant=%+v\ngot=%+v", wantNorm, gotNorm)
		}
	})

	t.Run("WriterLease", func(t *testing.T) {
		var lease WriterLease
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&lease).Elem(), "WriterLease", 0)
		assertAllExportedNonZero(t, reflect.ValueOf(lease), "WriterLease", nil)

		data, err := marshalParquetWriterLease(ctx, lease)
		if err != nil {
			t.Fatalf("marshalParquetWriterLease: %v", err)
		}
		got, err := decodeParquetWriterLease(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetWriterLease: %v", err)
		}
		if !reflect.DeepEqual(lease, got) {
			t.Fatalf("WriterLease Parquet round-trip lost or altered fields\nwant=%+v\ngot=%+v", lease, got)
		}
	})

	t.Run("ShardedSnapshotCatalog", func(t *testing.T) {
		var catalog ShardedSnapshotCatalog
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&catalog).Elem(), "ShardedSnapshotCatalog", 0)
		catalog.LayoutVersion = CurrentObjectLayoutVersion
		assertAllExportedNonZero(t, reflect.ValueOf(catalog), "ShardedSnapshotCatalog", nil)

		data, err := marshalParquetShardedSnapshotCatalog(ctx, catalog)
		if err != nil {
			t.Fatalf("marshalParquetShardedSnapshotCatalog: %v", err)
		}
		got, err := decodeParquetShardedSnapshotCatalog(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetShardedSnapshotCatalog: %v", err)
		}
		wantJSON, _ := json.MarshalIndent(catalog, "", "  ")
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("ShardedSnapshotCatalog Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("EntityRecord", func(t *testing.T) {
		var rec EntityRecord
		skip := map[string]bool{
			"EntityRecord.Deleted":                true,
			"EntityRecord.Entity.FieldWriteModes": true,
		}
		pop := newReflectPopulator(skip)
		pop.fillValue(reflect.ValueOf(&rec).Elem(), "EntityRecord", 0)
		rec.LayoutVersion = CurrentObjectLayoutVersion
		rec.Deleted = false
		rec.Entity.ID = rec.ID
		assertAllExportedNonZero(t, reflect.ValueOf(rec), "EntityRecord", skip)

		data, err := marshalParquetEntityRecord(ctx, rec)
		if err != nil {
			t.Fatalf("marshalParquetEntityRecord: %v", err)
		}
		got, err := decodeParquetEntityRecord(ctx, data, rec.TenantID, rec.ID)
		if err != nil {
			t.Fatalf("decodeParquetEntityRecord: %v", err)
		}
		wantJSON, _ := json.MarshalIndent(rec, "", "  ")
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("EntityRecord Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("Task", func(t *testing.T) {
		var task Task
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&task).Elem(), "Task", 0)
		assertAllExportedNonZero(t, reflect.ValueOf(task), "Task", nil)

		data, err := marshalParquetTask(ctx, task)
		if err != nil {
			t.Fatalf("marshalParquetTask: %v", err)
		}
		got, err := decodeParquetTask(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetTask: %v", err)
		}
		wantJSON, _ := json.MarshalIndent(task, "", "  ")
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("Task Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("DirectCommitRecord", func(t *testing.T) {
		var rec DirectCommitRecord
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&rec).Elem(), "DirectCommitRecord", 0)
		rec.Result.TenantID = rec.TenantID
		alignGraphMutationsFieldWriteModes(&rec.Request.Mutations)
		assertAllExportedNonZero(t, reflect.ValueOf(rec), "DirectCommitRecord", nil)

		data, err := marshalParquetDirectCommitRecord(ctx, rec)
		if err != nil {
			t.Fatalf("marshalParquetDirectCommitRecord: %v", err)
		}
		got, err := decodeParquetDirectCommitRecord(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetDirectCommitRecord: %v", err)
		}
		wantJSON, _ := json.MarshalIndent(rec, "", "  ")
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("DirectCommitRecord Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("SourcePolicy", func(t *testing.T) {
		var rec sourcePolicyRecord
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&rec).Elem(), "sourcePolicyRecord", 0)
		assertAllExportedNonZero(t, reflect.ValueOf(rec), "sourcePolicyRecord", nil)

		data, err := marshalParquetSourcePolicy(ctx, rec)
		if err != nil {
			t.Fatalf("marshalParquetSourcePolicy: %v", err)
		}
		got, err := decodeParquetSourcePolicy(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetSourcePolicy: %v", err)
		}
		wantJSON, _ := json.MarshalIndent(rec, "", "  ")
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("SourcePolicy Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("IndexDefinitionRecord", func(t *testing.T) {
		var rec IndexDefinitionRecord
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&rec).Elem(), "IndexDefinitionRecord", 0)
		rec.LayoutVersion = CurrentObjectLayoutVersion
		assertAllExportedNonZero(t, reflect.ValueOf(rec), "IndexDefinitionRecord", nil)

		data, err := marshalParquetIndexDefinitions(ctx, rec)
		if err != nil {
			t.Fatalf("marshalParquetIndexDefinitions: %v", err)
		}
		got, err := decodeParquetIndexDefinitions(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetIndexDefinitions: %v", err)
		}
		wantJSON, _ := json.MarshalIndent(normalizeIndexDefinitionRecord(rec), "", "  ")
		gotJSON, _ := json.MarshalIndent(normalizeIndexDefinitionRecord(got), "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("IndexDefinitionRecord Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("CollectorStatus", func(t *testing.T) {
		var status CollectorStatus
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&status).Elem(), "CollectorStatus", 0)
		assertAllExportedNonZero(t, reflect.ValueOf(status), "CollectorStatus", nil)

		data, err := marshalParquetCollectorStatus(ctx, status)
		if err != nil {
			t.Fatalf("marshalParquetCollectorStatus: %v", err)
		}
		got, err := decodeParquetCollectorStatus(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetCollectorStatus: %v", err)
		}
		wantJSON, _ := json.MarshalIndent(status, "", "  ")
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("CollectorStatus Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})

	t.Run("ReaderHeartbeat", func(t *testing.T) {
		var hb ReaderHeartbeat
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&hb).Elem(), "ReaderHeartbeat", 0)
		assertAllExportedNonZero(t, reflect.ValueOf(hb), "ReaderHeartbeat", nil)

		data, err := marshalParquetReaderHeartbeat(ctx, hb)
		if err != nil {
			t.Fatalf("marshalParquetReaderHeartbeat: %v", err)
		}
		got, err := decodeParquetReaderHeartbeat(ctx, data)
		if err != nil {
			t.Fatalf("decodeParquetReaderHeartbeat: %v", err)
		}
		if !reflect.DeepEqual(hb, got) {
			t.Fatalf("ReaderHeartbeat Parquet round-trip lost or altered fields\nwant=%+v\ngot=%+v", hb, got)
		}
	})

	t.Run("CommitSegment", func(t *testing.T) {
		var item commitSegmentItem
		pop := newReflectPopulator(nil)
		pop.fillValue(reflect.ValueOf(&item).Elem(), "commitSegmentItem", 0)
		item.Commit.TenantID = "tenant-a"
		item.Commit.Version = 2
		item.Commit.LayoutVersion = CurrentObjectLayoutVersion
		item.Commit.IdentityEncoding = graph.IdentityEncodingTuple
		alignGraphMutationsFieldWriteModes(&item.Commit.Mutations)
		assertAllExportedNonZero(t, reflect.ValueOf(item), "commitSegmentItem", nil)
		data, err := marshalParquetCommitSegment(ctx, "tenant-a", []commitSegmentItem{item})
		if err != nil {
			t.Fatalf("marshalParquetCommitSegment: %v", err)
		}
		_, gotItems, err := decodeParquetCommitSegmentObject(ctx, data, "tenant-a", CommitSegmentRef{})
		if err != nil {
			t.Fatalf("decodeParquetCommitSegmentObject: %v", err)
		}
		if len(gotItems) != 1 {
			t.Fatalf("got %d items, want 1", len(gotItems))
		}
		wantJSON, _ := json.MarshalIndent(item, "", "  ")
		gotJSON, _ := json.MarshalIndent(gotItems[0], "", "  ")
		if string(wantJSON) != string(gotJSON) {
			t.Fatalf("CommitSegment Parquet round-trip lost or altered fields\nwant=%s\ngot=%s", wantJSON, gotJSON)
		}
	})
}

type reflectPopulator struct {
	seq            int
	skip           map[string]bool
	base           time.Time
	includeNonJSON bool
}

func newReflectPopulator(skip map[string]bool) *reflectPopulator {
	return &reflectPopulator{
		seq:  1,
		skip: skip,
		base: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
	}
}

func (p *reflectPopulator) next() int {
	p.seq++
	return p.seq
}

var (
	timeType       = reflect.TypeOf(time.Time{})
	filterExprType = reflect.TypeOf(query.FilterExpr{})
	graphFieldsTyp = reflect.TypeOf(graph.Fields{})
)

func (p *reflectPopulator) fillValue(v reflect.Value, path string, depth int) {
	if p.skip[path] || !v.CanSet() {
		return
	}
	t := v.Type()
	if t == timeType {
		v.Set(reflect.ValueOf(p.base.Add(time.Duration(p.next()) * time.Second).UTC()))
		return
	}
	if t == filterExprType {
		if strings.Count(path, "Children") >= 1 {
			v.Set(reflect.ValueOf(query.FilterExpr{
				Op:    "eq",
				Field: fmt.Sprintf("f_%d", p.next()),
				Value: fmt.Sprintf("v_%d", p.next()),
			}))
			return
		}
		v.Set(reflect.ValueOf(query.FilterExpr{
			Op:    "and",
			Field: fmt.Sprintf("expr_%d", p.next()),
			Value: fmt.Sprintf("ev_%d", p.next()),
			Children: []query.FilterExpr{
				{Op: "eq", Field: fmt.Sprintf("cf_%d", p.next()), Value: float64(p.next())},
			},
		}))
		return
	}
	if t == graphFieldsTyp {
		m := graph.Fields{
			fmt.Sprintf("k_%d", p.next()): fmt.Sprintf("val_%d", p.next()),
			fmt.Sprintf("n_%d", p.next()): float64(p.next()),
			fmt.Sprintf("b_%d", p.next()): true,
		}
		v.Set(reflect.ValueOf(m))
		return
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(fmt.Sprintf("s_%d", p.next()))
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(p.next()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(uint64(p.next()))
	case reflect.Float32, reflect.Float64:
		v.SetFloat(float64(p.next()) + 0.25)
	case reflect.Interface:
		// Populate `any` with a JSON/Parquet-safe scalar string.
		v.Set(reflect.ValueOf(fmt.Sprintf("any_%d", p.next())))
	case reflect.Ptr:
		if depth > 12 {
			return
		}
		elem := reflect.New(t.Elem())
		p.fillValue(elem.Elem(), path, depth+1)
		v.Set(elem)
	case reflect.Slice:
		if depth > 12 {
			return
		}
		slice := reflect.MakeSlice(t, 1, 1)
		p.fillValue(slice.Index(0), path+"[]", depth+1)
		v.Set(slice)
	case reflect.Map:
		if depth > 12 {
			return
		}
		m := reflect.MakeMapWithSize(t, 1)
		k := reflect.New(t.Key()).Elem()
		p.fillValue(k, path+".key", depth+1)
		val := reflect.New(t.Elem()).Elem()
		p.fillValue(val, path+".val", depth+1)
		m.SetMapIndex(k, val)
		v.Set(m)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			if !sf.IsExported() {
				continue
			}
			// Skip json:"-" fields unless includeNonJSON is explicitly enabled.
			if !p.includeNonJSON && sf.Tag.Get("json") == "-" {
				continue
			}
			p.fillValue(v.Field(i), path+"."+sf.Name, depth+1)
		}
	}
}

func alignEntityFieldWriteModes(items []IngestItem) {
	for i := range items {
		if items[i].Entity != nil {
			for k := range items[i].Entity.Fields {
				items[i].Entity.FieldWriteModes = map[string]string{k: "replace"}
				break
			}
		}
	}
}

func alignGraphMutationsFieldWriteModes(mutations *graph.Mutations) {
	alignGraphEntityFieldWriteModes(mutations.UpsertEntities)
	for i := range mutations.SplitEntities {
		alignGraphEntityFieldWriteModes(mutations.SplitEntities[i].Entities)
	}
}

func alignGraphEntityFieldWriteModes(entities []graph.Entity) {
	for i := range entities {
		for k := range entities[i].Fields {
			entities[i].FieldWriteModes = map[string]string{k: "replace"}
			break
		}
	}
}

func assertAllExportedNonZero(t *testing.T, v reflect.Value, path string, skip map[string]bool) {
	t.Helper()
	assertAllExportedNonZeroOpts(t, v, path, skip, false)
}

func assertAllExportedNonZeroOpts(t *testing.T, v reflect.Value, path string, skip map[string]bool, includeNonJSON bool) {
	t.Helper()
	if skip[path] {
		return
	}
	if !v.IsValid() {
		t.Fatalf("%s is invalid", path)
	}
	if v.Type() == timeType {
		if v.Interface().(time.Time).IsZero() {
			t.Fatalf("%s time.Time is zero", path)
		}
		return
	}
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			t.Fatalf("%s pointer is nil", path)
		}
		assertAllExportedNonZeroOpts(t, v.Elem(), path, skip, includeNonJSON)
	case reflect.Slice:
		if v.Len() == 0 {
			t.Fatalf("%s slice is empty", path)
		}
		for i := 0; i < v.Len(); i++ {
			assertAllExportedNonZeroOpts(t, v.Index(i), path+"[]", skip, includeNonJSON)
		}
	case reflect.Map:
		if v.Len() == 0 {
			t.Fatalf("%s map is empty", path)
		}
	case reflect.Struct:
		rt := v.Type()
		for i := 0; i < rt.NumField(); i++ {
			sf := rt.Field(i)
			if !sf.IsExported() || (!includeNonJSON && sf.Tag.Get("json") == "-") {
				continue
			}
			fieldPath := path + "." + sf.Name
			if skip[fieldPath] {
				continue
			}
			// Leaf FilterExpr has no Children; checked at root level.
			if rt == filterExprType && sf.Name == "Children" && strings.Count(path, "Children") >= 1 {
				continue
			}
			assertAllExportedNonZeroOpts(t, v.Field(i), fieldPath, skip, includeNonJSON)
		}
	default:
		if v.IsZero() {
			t.Fatalf("%s is zero", path)
		}
	}
}
