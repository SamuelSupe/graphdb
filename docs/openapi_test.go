package docs

import (
	"os"
	"strings"
	"testing"
)

func TestOpenAPIContractIncludesProductAPIs(t *testing.T) {
	data, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatalf("read openapi.yaml: %v", err)
	}
	spec := string(data)
	required := []string{
		"/openapi.yaml:",
		"/v1/health:",
		"/v1/readiness:",
		"/metrics:",
		"/v1/commits:",
		"/v1/ingest/batches:",
		"/v1/imports:",
		"/v1/ingest/collectors/{source}/{collector_id}:",
		"/v1/ingest/deadletters/{source}:",
		"/v1/ingest/deadletters/{source}/replay:",
		"/v1/entity-types:",
		"/v1/relation-schemas:",
		"/v1/tenants:",
		"/v1/query:",
		"/v1/query/graphql:",
		"/v1/query/gql:",
		"/v1/query/gql/stream:",
		"/v1/query/templates:",
		"/v1/query/templates/{name}/run:",
		"/v1/entities:",
		"/v1/edges:",
		"/v1/export/snapshot:",
		"/v1/source-policy:",
		"/v1/tenant-config:",
		"/v1/tenant-usage:",
		"/v1/tenants/{tenant_id}/restore-drill:",
		"/v1/queries/running:",
		"/v1/control/reader-fleet-readiness:",
		"/v1/control/writer-lease:",
		"/v1/control/cleanup-commits:",
		"/v1/control/gc:",
		"/v1/compact:",
		"/v1/indexes/definitions:",
		"/v1/indexes/definitions/{name}:",
		"/v1/tasks:",
		"TenantUsage:",
		"CoordinatorStatus:",
		"ObjectStoreStatus:",
		"BuildInfo:",
		"CollectorStatus:",
		"SavedQuery:",
		"IndexDefinition:",
		"WriterLease:",
		"GCReport:",
		"ReaderFleetReadiness:",
		"RunningQuery:",
		"GraphQLRequest:",
		"GraphQLResponse:",
		"GraphQLError:",
		"GQLQueryRequest:",
		"FilterExpr:",
		"PathStep:",
		"RelationSchemaCatalog:",
		"ErrorResponse:",
		"WriteBackpressureError:",
		"index_stale",
		"reader_not_fresh",
		"object_store_unavailable",
		"lease_held",
		"task_conflict",
		"repair_required",
		"tenant_deleted",
		"tenant_restore_drill",
		"idempotency_key:",
	}
	for _, item := range required {
		if !strings.Contains(spec, item) {
			t.Fatalf("openapi.yaml missing %q", item)
		}
	}
}
