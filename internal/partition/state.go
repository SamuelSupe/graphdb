package partition

import (
	"context"
	"crypto/sha256"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const FormatVersion = 1

type Placement struct {
	Group string `json:"group"`
	Epoch uint64 `json:"epoch"`
}

type Group struct {
	ID        string            `json:"id"`
	ClusterID string            `json:"cluster_id"`
	Peers     map[uint64]string `json:"peers"`
}

type Graph struct {
	HistoryCutoff          time.Time                     `json:"history_cutoff,omitempty"`
	SourceCleanupRevision  int                           `json:"source_cleanup_revision,omitempty"`
	ReadLease              *Pin                          `json:"read_lease,omitempty"`
	LastGCAt               time.Time                     `json:"last_gc_at,omitempty"`
	GC                     *GraphGC                      `json:"gc,omitempty"`
	Move                   *Move                         `json:"move,omitempty"`
	SourceRetired          bool                          `json:"source_retired,omitempty"`
	Format                 int                           `json:"format"`
	Tenant                 string                        `json:"tenant_id"`
	Generation             int64                         `json:"generation"`
	Version                int64                         `json:"version"`
	EntityCount            int64                         `json:"entity_count"`
	EdgeTypeCounts         map[string]int64              `json:"edge_type_counts,omitempty"`
	EdgeCount              int64                         `json:"edge_count"`
	Sequence               uint64                        `json:"sequence"`
	State                  string                        `json:"state"`
	LayoutRevision         uint64                        `json:"layout_revision"`
	Partitions             [256]Placement                `json:"partitions"`
	Groups                 map[string]Group              `json:"groups"`
	Roots                  [256]Root                     `json:"roots"`
	EntityCounts           [256]int64                    `json:"entity_counts"`
	CITypes                []graph.CIType                `json:"ci_types,omitempty"`
	RelationTypes          []graph.RelationType          `json:"relation_types,omitempty"`
	SourcePolicyConfigured bool                          `json:"source_policy_configured"`
	SourcePolicy           graph.SourcePolicy            `json:"source_policy"`
	Copy                   *CopyProgress                 `json:"copy,omitempty"`
	Config                 storage.TenantConfig          `json:"config"`
	RelationSchemas        storage.RelationSchemaCatalog `json:"relation_schemas"`
	HashBuckets            [1024]string                  `json:"hash_buckets"`
	DataHash               string                        `json:"data_hash"`
	Active                 *Transaction                  `json:"active,omitempty"`
}

type Input struct {
	Stage      string `json:"stage"`
	Digest     string `json:"digest"`
	Bytes      int    `json:"bytes"`
	Acceptance string `json:"acceptance,omitempty"`
}

type Transaction struct {
	IngestResult *storage.IngestResult `json:"ingest_result,omitempty"`
	ErrorCode    string                `json:"error_code,omitempty"`
	Purpose      string                `json:"purpose,omitempty"`
	Tenant       string                `json:"tenant_id"`
	Generation   int64                 `json:"generation"`
	ID           string                `json:"id"`
	Sequence     uint64                `json:"sequence"`
	Identity     string                `json:"identity"`
	Digest       string                `json:"digest"`
	Inputs       []Input               `json:"inputs"`
	At           time.Time             `json:"at"`
	BaseVersion  int64                 `json:"base_version"`
	State        string                `json:"state"`
	Prepared     map[uint8]Prepared    `json:"prepared,omitempty"`
	Participants []uint8               `json:"participants,omitempty"`
	Result       storage.CommitResult  `json:"result"`
	Error        string                `json:"error,omitempty"`
}

type Prepared struct {
	EdgeTypeDelta map[string]int64 `json:"edge_type_delta,omitempty"`
	EntityDelta   int64            `json:"entity_delta,omitempty"`
	EdgeDelta     int64            `json:"edge_delta,omitempty"`
	Txn           string           `json:"txn"`
	Sequence      uint64           `json:"sequence"`
	Base          Root             `json:"base"`
	Root          Root             `json:"root"`
	Digest        string           `json:"digest"`
	HashBuckets   map[int]string   `json:"hash_buckets,omitempty"`
}

type DataState struct {
	CaptureViews []Root            `json:"capture_views,omitempty"`
	MoveVerify   *MoveVerification `json:"move_verify,omitempty"`
	GC           *DataGC           `json:"gc,omitempty"`
	LastGC       string            `json:"last_gc,omitempty"`
	CaptureRoot  Root              `json:"capture_root"`
	Capture      string            `json:"capture,omitempty"`
	Tenant       string            `json:"tenant_id"`
	Generation   int64             `json:"generation"`
	Partition    uint8             `json:"partition"`
	Epoch        uint64            `json:"epoch"`
	State        string            `json:"state"`
	Sequence     uint64            `json:"sequence"`
	LastTxn      string            `json:"last_txn,omitempty"`
	Root         Root              `json:"root"`
	Prepared     *Prepared         `json:"prepared,omitempty"`
}

// Action is applied inside the existing replication journal. The coordinator
// performs remote calls before proposing one of these deterministic steps.
const ActionRevision = 2

// InputGCRevision allows unclaimed staging chunks to expire before begin.
const InputGCRevision = 3

const BatchProtocol = 8
const BatchGCRevision = 4

const BufferedGCProtocol = 9
const BufferedGCRevision = 5

const PlannedGCProtocol = 10
const PlannedGCRevision = 6

const BalancedLeafProtocol = 11
const BalancedLeafRevision = 7

type Action struct {
	GCSweep         *GCSweepPlan          `json:"gc_sweep,omitempty"`
	PreparedBatch   map[uint8]Prepared    `json:"prepared_batch,omitempty"`
	LayoutRevision  uint64                `json:"layout_revision,omitempty"`
	ReceiptRef      string                `json:"receipt_ref,omitempty"`
	IngestResult    *storage.IngestResult `json:"ingest_result,omitempty"`
	Verify          *MoveVerification     `json:"verify,omitempty"`
	Revision        int                   `json:"revision,omitempty"`
	Pages           []PageObject          `json:"pages,omitempty"`
	ErrorCode       string                `json:"error_code,omitempty"`
	Roots           []Root                `json:"roots,omitempty"`
	Target          *Group                `json:"target,omitempty"`
	DataState       *DataState            `json:"data_state,omitempty"`
	Transactions    []Transaction         `json:"transactions,omitempty"`
	Purpose         string                `json:"purpose,omitempty"`
	Phase           string                `json:"phase,omitempty"`
	Cursor          string                `json:"cursor,omitempty"`
	ExpectedCursor  string                `json:"expected_cursor,omitempty"`
	ExpectedVersion *int64                `json:"expected_version,omitempty"`
	Operation       string                `json:"operation"`
	Tenant          string                `json:"tenant_id"`
	Generation      int64                 `json:"generation"`
	Partition       uint8                 `json:"partition,omitempty"`
	Epoch           uint64                `json:"epoch,omitempty"`
	Txn             string                `json:"txn,omitempty"`
	Sequence        uint64                `json:"sequence,omitempty"`
	BaseVersion     int64                 `json:"base_version,omitempty"`
	Base            Root                  `json:"base"`
	Digest          string                `json:"digest,omitempty"`
	Identity        string                `json:"identity,omitempty"`
	At              time.Time             `json:"at,omitempty"`
	Inputs          []Input               `json:"inputs,omitempty"`
	Data            []byte                `json:"data,omitempty"`
	// Empty and nil changes have different prepare digests; preserve [] on the wire.
	Changes       []Row                 `json:"changes"`
	Graph         *Graph                `json:"graph,omitempty"`
	Prepared      *Prepared             `json:"prepared,omitempty"`
	Participants  []uint8               `json:"participants,omitempty"`
	Result        *storage.CommitResult `json:"result,omitempty"`
	CITypes       []graph.CIType        `json:"ci_types,omitempty"`
	RelationTypes []graph.RelationType  `json:"relation_types,omitempty"`
	Error         string                `json:"error,omitempty"`
}

type Store struct {
	Objects         storage.ObjectStore
	Prefix          string
	ReadIngestInput func(context.Context, string, int64, string) ([]byte, error)
}

func Bucket(key string) uint8 { return sha256.Sum256([]byte(key))[0] }

func Key(family string, parts ...string) string {
	var b strings.Builder
	b.WriteString(family)
	b.WriteByte(0)
	for _, part := range parts {
		b.WriteString(strings.ReplaceAll(part, "\x00", "\x00\xff"))
		b.WriteString("\x00\x00")
	}
	return b.String()
}
