package docs

import (
	"os"
	"strings"
	"testing"
)

func TestOpenAPIContractIncludesProductAPIs(t *testing.T) {
	spec := readOpenAPISpec(t)
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

func TestOpenAPICommitAndIngestContractsAreTyped(t *testing.T) {
	spec := readOpenAPISpec(t)
	commitPath := openAPIBlock(t, spec, "  /v1/commits:")
	if !strings.Contains(commitPath, "#/components/schemas/CommitRequest") {
		t.Fatalf("/v1/commits does not reference CommitRequest: %s", commitPath)
	}
	commitRequest := openAPIBlock(t, spec, "    CommitRequest:")
	for _, field := range []string{"expected_version:", "idempotency_key:", "mutations:"} {
		if !strings.Contains(commitRequest, field) {
			t.Errorf("CommitRequest missing %q", field)
		}
	}

	mutations := openAPIBlock(t, spec, "    Mutations:")
	for _, field := range []string{
		"upsert_ci_types:", "delete_ci_types:", "upsert_entity_types:", "delete_entity_types:",
		"upsert_relation_types:", "delete_relation_types:", "upsert_entities:", "delete_entities:",
		"delete_entity_requests:", "mark_source_stale:", "upsert_edges:", "delete_edges:",
		"delete_edge_requests:", "merge_entities:", "split_entities:",
	} {
		if !strings.Contains(mutations, field) {
			t.Errorf("Mutations missing %q", field)
		}
	}

	ingestPath := openAPIBlock(t, spec, "  /v1/ingest/batches:")
	for _, response := range []string{"'200':", "'207':", "'202':", "Location:", "IngestAccepted"} {
		if !strings.Contains(ingestPath, response) {
			t.Errorf("ingest path missing %q", response)
		}
	}
	ingestRequest := openAPIBlock(t, spec, "    IngestRequest:")
	for _, field := range []string{"expected_version:", "failure_mode:", "preconditions:", "items:"} {
		if !strings.Contains(ingestRequest, field) {
			t.Errorf("IngestRequest missing %q", field)
		}
	}
	if strings.Contains(ingestRequest, "additionalProperties: true") {
		t.Fatal("IngestRequest must describe typed items instead of arbitrary item objects")
	}
	ingestItem := openAPIBlock(t, spec, "    IngestItem:")
	for _, field := range []string{
		"external_id:", "entity:", "edge:", "delete_entity:", "delete_edge:",
		"relation_type:", "ci_type:", "entity_type:",
	} {
		if !strings.Contains(ingestItem, field) {
			t.Errorf("IngestItem missing %q", field)
		}
	}
	result := openAPIBlock(t, spec, "    IngestResult:")
	if !strings.Contains(result, "error_code:") {
		t.Fatal("IngestResult missing error_code")
	}
	accepted := openAPIBlock(t, spec, "    IngestAccepted:")
	if !strings.Contains(accepted, "required: [writer_id, source, collector_id, batch_id, state, durability, accepted_at, estimated_flush_at, status_url]") {
		t.Fatalf("IngestAccepted required fields do not include owner/source identity: %s", accepted)
	}
}

func TestOpenAPIPublicContractDoesNotExposeEvidenceSearch(t *testing.T) {
	spec := readOpenAPISpec(t)
	if strings.Contains(strings.ToLower(spec), "evidence") {
		t.Fatal("OpenAPI public contract still exposes evidence search")
	}
}

func readOpenAPISpec(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatalf("read openapi.yaml: %v", err)
	}
	return string(data)
}

// openAPIBlock returns a top-level path or component block without requiring a
// YAML dependency in the contract test. The marker must include its indentation
// so nested properties cannot terminate the block early.
func openAPIBlock(t *testing.T, spec string, marker string) string {
	t.Helper()
	start := strings.Index(spec, "\n"+marker)
	if start < 0 {
		t.Fatalf("OpenAPI marker %q not found", marker)
	}
	start++
	indent := marker[:len(marker)-len(strings.TrimLeft(marker, " "))]
	rest := spec[start+len(marker):]
	offset := 0
	for _, line := range strings.SplitAfter(rest, "\n") {
		content := strings.TrimSuffix(line, "\n")
		leadingSpaces := len(content) - len(strings.TrimLeft(content, " "))
		if strings.TrimSpace(content) != "" && leadingSpaces == len(indent) {
			return spec[start : start+len(marker)+offset]
		}
		offset += len(line)
	}
	return spec[start:]
}
