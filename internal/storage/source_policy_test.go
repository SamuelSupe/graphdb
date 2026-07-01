package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestPutSourcePolicyRequiresWriterLease(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	owner := NewTenantStore(objects, "test")
	owner.LeaseTTL = time.Hour
	if _, err := owner.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	other := NewTenantStore(objects, "test")
	_, err := other.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		Sources: []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
	})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("put policy error = %v, want ErrLeaseHeld", err)
	}
	_, configured, err := owner.GetSourcePolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if configured {
		t.Fatal("policy was written despite held writer lease")
	}
}

func TestSourcePolicyRecordIncludesTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	policy, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 1,
		Sources:         []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		FieldAliases:    []graph.FieldAliasRule{{Source: "aws", Kind: "host", Aliases: map[string]string{"privateIpAddress": "private_ip"}}},
		FieldPriorities: []graph.FieldPriorityRule{{Source: "aws", Kind: "host", Fields: map[string]int{"private_ip": 900}}},
	})
	if err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if policy.DefaultPriority != 1 || len(policy.Sources) != 1 || len(policy.FieldAliases) != 1 || len(policy.FieldPriorities) != 1 {
		t.Fatalf("policy = %#v", policy)
	}
	data, err := store.Objects.Get(ctx, store.sourcePolicyKey("tenant-a"))
	if err != nil {
		t.Fatalf("get raw policy: %v", err)
	}
	if !isParquetBytes(data) {
		t.Fatal("source policy object is not parquet")
	}
	record, err := decodeParquetSourcePolicy(ctx, data)
	if err != nil {
		t.Fatalf("decode raw policy: %v", err)
	}
	if record.TenantID != "tenant-a" || record.DefaultPriority != 1 || record.Sources[0].Name != "manual" ||
		record.FieldAliases[0].Aliases["privateIpAddress"] != "private_ip" || record.FieldPriorities[0].Fields["private_ip"] != 900 {
		t.Fatalf("record = %#v", record)
	}
}

func TestGetSourcePolicyReadsV1ParquetWithoutAliases(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	data, err := marshalParquetSourcePolicyV1ForTest(ctx, sourcePolicyRecord{
		TenantID: "tenant-a",
		SourcePolicy: graph.SourcePolicy{
			DefaultPriority: 1,
			Sources:         []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		},
	})
	if err != nil {
		t.Fatalf("marshal v1 policy: %v", err)
	}
	if err := store.Objects.Put(ctx, store.sourcePolicyKey("tenant-a"), data); err != nil {
		t.Fatalf("put v1 policy: %v", err)
	}
	policy, configured, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get v1 policy: %v", err)
	}
	if !configured || policy.DefaultPriority != 1 || len(policy.Sources) != 1 || len(policy.FieldAliases) != 0 || len(policy.FieldPriorities) != 0 {
		t.Fatalf("policy configured=%v policy=%#v", configured, policy)
	}
}

func TestPutSourcePolicyDoesNotOverwriteAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	objects := &takeoverDuringSourcePolicyPutStore{
		ObjectStore: base,
		base:        base,
		tenantID:    "tenant-a",
		triggerKey:  store.sourcePolicyKey("tenant-a"),
	}
	stale := NewTenantStore(objects, "test")
	stale.LeaseTTL = time.Nanosecond
	_, err := stale.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 1,
		Sources:         []graph.SourcePolicyItem{{Name: "agent", Priority: 100}},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale put err = %v, want ErrConflict", err)
	}
	if !objects.Triggered() {
		t.Fatal("test store did not trigger takeover")
	}

	policy, configured, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if !configured || policy.DefaultPriority != 9 || len(policy.Sources) != 1 || policy.Sources[0].Name != "manual" {
		t.Fatalf("policy after takeover = configured=%v %#v", configured, policy)
	}
}

func TestGetSourcePolicyRejectsMismatchedTenant(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putSourcePolicyFixture(ctx, store, "tenant-a", sourcePolicyRecord{
		TenantID: "tenant-b",
		SourcePolicy: graph.SourcePolicy{
			Sources: []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	_, _, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err == nil || !strings.Contains(err.Error(), "source policy tenant mismatch") {
		t.Fatalf("get policy err = %v, want tenant mismatch", err)
	}
}

func TestGetSourcePolicyAcceptsRecordWithoutTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putSourcePolicyFixture(ctx, store, "tenant-a", sourcePolicyRecord{
		SourcePolicy: graph.SourcePolicy{
			DefaultPriority: 1,
			Sources:         []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	policy, configured, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if !configured || policy.DefaultPriority != 1 || len(policy.Sources) != 1 || policy.Sources[0].Name != "manual" {
		t.Fatalf("policy configured=%v policy=%#v", configured, policy)
	}
}

func putSourcePolicyFixture(ctx context.Context, store *TenantStore, tenantID string, record sourcePolicyRecord) error {
	data, err := marshalParquetSourcePolicy(ctx, record)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, store.sourcePolicyKey(tenantID), data)
}

func TestIngestUsesSourcePolicyAndSuppressionIsNotFailure(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"owner": "platform"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("manual commit: %v", err)
	}
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "ec2"}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "aws")
	if err != nil {
		t.Fatalf("deadletters: %v", err)
	}
	if len(letters) != 0 {
		t.Fatalf("deadletters = %#v", letters)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed.Skipped || replayed.Suppressed != 1 {
		t.Fatalf("replayed = %#v", replayed)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "platform" {
		t.Fatalf("owner = %#v", entity.Fields["owner"])
	}
}

func TestCommitAndIngestApplySourcePolicyFieldAliases(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources:         []graph.SourcePolicyItem{{Name: "aws", Priority: 50}},
		FieldAliases:    []graph.FieldAliasRule{{Source: "aws", Kind: "host", Aliases: map[string]string{"host_name": "hostname"}}},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "aws", Fields: graph.Fields{"host_name": "web-1"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit alias: %v", err)
	}
	if result.Skipped {
		t.Fatal("initial alias commit was skipped")
	}
	skipped, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "aws", Fields: graph.Fields{"host_name": "web-1"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("repeat alias commit: %v", err)
	}
	if !skipped.Skipped {
		t.Fatalf("repeat alias write did not MD5 skip: %#v", skipped)
	}
	ingested, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "alias-ingest",
		Items: []IngestItem{{
			ExternalID: "host-2",
			Entity:     &graph.Entity{ID: "host:2", Kind: "host", Fields: graph.Fields{"host_name": "web-2"}},
		}},
	})
	if err != nil {
		t.Fatalf("ingest alias: %v", err)
	}
	if ingested.Failed != 0 || ingested.Suppressed != 0 {
		t.Fatalf("ingest result = %#v", ingested)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	host1, _ := g.GetEntity("host:1")
	host2, _ := g.GetEntity("host:2")
	if host1.Fields["hostname"] != "web-1" || host2.Fields["hostname"] != "web-2" {
		t.Fatalf("host1=%#v host2=%#v", host1.Fields, host2.Fields)
	}
	if _, ok := host2.Fields["host_name"]; ok {
		t.Fatalf("ingest alias field persisted: %#v", host2.Fields)
	}
}

func TestCommitIngestAndCompactApplySourcePolicyFieldPriorities(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
		FieldAliases: []graph.FieldAliasRule{{Source: "aws", Kind: "host", Aliases: map[string]string{"privateIpAddress": "private_ip"}}},
		FieldPriorities: []graph.FieldPriorityRule{{Source: "aws", Kind: "host", Fields: map[string]int{
			"hostname":   1200,
			"private_ip": 1200,
		}}},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual",
		Fields: graph.Fields{"hostname": "manual-host", "owner": "platform", "private_ip": "10.0.0.1"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("manual commit: %v", err)
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "aws",
		Fields: graph.Fields{"hostname": "aws-host", "owner": "collector"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("aws commit: %v", err)
	}
	if len(result.Suppressed) != 1 || result.Suppressed[0].Field != "owner" {
		t.Fatalf("commit result = %#v", result)
	}
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "field-priority-ingest",
		IdempotencyKey: "field-priority-ingest",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"privateIpAddress": "10.0.0.2"}},
		}},
	}
	ingested, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if ingested.Failed != 0 || ingested.Suppressed != 0 {
		t.Fatalf("ingest result = %#v", ingested)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed.Skipped || replayed.Version != ingested.Version {
		t.Fatalf("replayed = %#v want version %d skipped", replayed, ingested.Version)
	}
	store.deleteWriteCache("tenant-a")
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entity, _ := loaded.GetEntity("host:1")
	if entity.Fields["hostname"] != "aws-host" || entity.Fields["owner"] != "platform" || entity.Fields["private_ip"] != "10.0.0.2" {
		t.Fatalf("loaded fields = %#v", entity.Fields)
	}
	if entity.FieldSources["hostname"].Priority != 1200 || entity.FieldSources["private_ip"].Priority != 1200 || entity.FieldSources["owner"].Priority != 1000 {
		t.Fatalf("loaded field sources = %#v", entity.FieldSources)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	compacted, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load compacted: %v", err)
	}
	entity, _ = compacted.GetEntity("host:1")
	if entity.Fields["private_ip"] != "10.0.0.2" || entity.FieldSources["private_ip"].Priority != 1200 {
		t.Fatalf("compacted entity = %#v", entity)
	}
}

func TestIngestFieldAliasConflictIsSuppressedNotFailure(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		Sources:      []graph.SourcePolicyItem{{Name: "aws", Priority: 50}},
		FieldAliases: []graph.FieldAliasRule{{Source: "aws", Aliases: map[string]string{"host_name": "hostname"}}},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	request := IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "alias-conflict",
		IdempotencyKey: "alias-conflict-idem",
		Items: []IngestItem{{
			ExternalID: "host-1",
			Entity:     &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"hostname": "canonical", "host_name": "alias"}},
		}},
	}
	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest conflict: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.Conflicts[0].Field != "hostname" || result.Conflicts[0].AliasField != "host_name" {
		t.Fatalf("conflict = %#v", result.Conflicts[0])
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed.Skipped || replayed.Suppressed != 1 || len(replayed.Conflicts) != 1 || replayed.Conflicts[0].AliasField != "host_name" {
		t.Fatalf("replayed = %#v", replayed)
	}
}

func TestIngestEntitySuppressionReportsItemLocation(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}); err != nil {
		t.Fatalf("put policy: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"owner": "platform"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("seed manual entity: %v", err)
	}
	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "entity-conflict-location",
		Items: []IngestItem{
			{ExternalID: "host-2", Entity: &graph.Entity{ID: "host:2", Kind: "host", Fields: graph.Fields{"owner": "ec2"}}},
			{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "ec2"}}},
		},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Failed != 0 || result.Suppressed != 1 || len(result.Conflicts) != 1 {
		t.Fatalf("result = %#v", result)
	}
	conflict := result.Conflicts[0]
	if conflict.Index != 1 || conflict.ExternalID != "host-1" || conflict.IncomingID != "host-1" {
		t.Fatalf("conflict location = %#v", conflict)
	}
}

type takeoverDuringSourcePolicyPutStore struct {
	ObjectStore
	base       *MemoryStore
	tenantID   string
	triggerKey string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverDuringSourcePolicyPutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		time.Sleep(time.Millisecond)
		takeover := NewTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.PutSourcePolicy(ctx, s.tenantID, graph.SourcePolicy{
			DefaultPriority: 9,
			Sources:         []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}},
		}); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *takeoverDuringSourcePolicyPutStore) shouldTrigger(key string) bool {
	if key != s.triggerKey {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *takeoverDuringSourcePolicyPutStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

func marshalParquetSourcePolicyV1ForTest(ctx context.Context, record sourcePolicyRecord) ([]byte, error) {
	normalized, err := normalizeSourcePolicyRecord(record)
	if err != nil {
		return nil, err
	}
	hash, err := sourcePolicyContentHashV1(normalized)
	if err != nil {
		return nil, err
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "default_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "source_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "source_description", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	appendRow := func(source graph.SourcePolicyItem) {
		builder.Field(parquetSourcePolicyColumnTenantID).(*array.StringBuilder).Append(normalized.TenantID)
		builder.Field(parquetSourcePolicyColumnDefaultPriority).(*array.Int64Builder).Append(int64(normalized.DefaultPriority))
		builder.Field(parquetSourcePolicyColumnSourceCount).(*array.Int64Builder).Append(int64(len(normalized.Sources)))
		builder.Field(parquetSourcePolicyColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetSourcePolicyColumnSourceName).(*array.StringBuilder).Append(source.Name)
		builder.Field(parquetSourcePolicyColumnSourcePriority).(*array.Int64Builder).Append(int64(source.Priority))
		builder.Field(parquetSourcePolicyColumnSourceDescription).(*array.StringBuilder).Append(source.Description)
	}
	if len(normalized.Sources) == 0 {
		appendRow(graph.SourcePolicyItem{})
	} else {
		for _, source := range normalized.Sources {
			appendRow(source)
		}
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()
	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, 1, writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}
