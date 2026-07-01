package main

import (
	"context"
	"fmt"
	"net/http"

	"graphdb/internal/graph"
	"graphdb/internal/storage"
)

func (r *runner) checkSourceFieldGovernance(ctx context.Context) error {
	hostID := "host:" + r.cfg.tenant + ":source-governance"
	manualName := "manual-" + r.cfg.tenant
	awsName := "aws-" + r.cfg.tenant
	if err := r.commit(ctx, "source-field-manual", sourceFieldManual(hostID, manualName)); err != nil {
		return err
	}
	resp, err := r.commitResponse(ctx, "source-field-aws-alias-priority", sourceFieldAWS(hostID, awsName, r.cfg.tenant))
	if err != nil {
		return err
	}
	if !hasSuppressedField(resp.json, "hostname") || !hasSuppressedField(resp.json, "private_ip") || !hasSuppressedField(resp.json, "region") {
		return fmt.Errorf("expected alias/private_ip/region suppressions: %s", string(resp.body))
	}
	if err := r.checkSourceFieldEntity(ctx, hostID, awsName); err != nil {
		return err
	}
	if err := r.checkSourceFieldArrayOverride(ctx, hostID, awsName); err != nil {
		return err
	}
	if err := r.checkSourceFieldIngest(ctx); err != nil {
		return err
	}
	if err := r.waitReaderEntity(ctx, hostID); err != nil {
		return err
	}
	pass("source field alias array override and field priority")
	return nil
}

func sourceFieldManual(hostID string, hostname string) graph.Mutations {
	return graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: hostID, Kind: "host", Source: "manual", Confidence: 0.9,
		Fields: graph.Fields{
			"hostname":   hostname,
			"private_ip": "10.0.0.1",
			"region":     "manual-region",
			"tags":       []any{"manual", "shared"},
		},
	}}}
}

func sourceFieldAWS(hostID string, hostname string, tenant string) graph.Mutations {
	return graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: hostID, Kind: "host", Source: "aws", Confidence: 0.9,
		Fields: graph.Fields{
			"hostname":         hostname,
			"instanceName":     "ignored-alias-" + tenant,
			"privateIpAddress": "10.0.0.2",
			"region":           "aws-region",
			"tags":             []any{"shared", "aws"},
		},
	}}}
}

func (r *runner) checkSourceFieldEntity(ctx context.Context, hostID string, awsName string) error {
	entity, err := r.writer.do(ctx, http.MethodGet, entityPath(hostID), nil, http.StatusOK)
	if err != nil {
		return err
	}
	fields := mapValue(mapValue(entity.json["entity"])["fields"])
	if stringValue(fields["hostname"]) != awsName {
		return fmt.Errorf("field priority did not override hostname: %s", string(entity.body))
	}
	if stringValue(fields["private_ip"]) != "10.0.0.1" || stringValue(fields["region"]) != "manual-region" {
		return fmt.Errorf("low priority fields were not suppressed: %s", string(entity.body))
	}
	if _, ok := fields["privateIpAddress"]; ok {
		return fmt.Errorf("alias field was stored instead of canonical field: %s", string(entity.body))
	}
	if got := stringArray(fields["tags"]); !sameStrings(got, []string{"manual", "shared", "aws"}) {
		return fmt.Errorf("array append_unique tags = %v", got)
	}
	sources := mapValue(mapValue(entity.json["entity"])["field_sources"])
	if intValue(mapValue(sources["hostname"])["priority"]) != 1200 {
		return fmt.Errorf("hostname field source did not use field priority: %s", string(entity.body))
	}
	if intValue(mapValue(sources["private_ip"])["priority"]) != 1000 {
		return fmt.Errorf("private_ip owner was downgraded: %s", string(entity.body))
	}
	return nil
}

func (r *runner) checkSourceFieldArrayOverride(ctx context.Context, hostID string, awsName string) error {
	lowReplace := graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: hostID, Kind: "host", Source: "aws", Confidence: 0.9,
		Fields: graph.Fields{"hostname": awsName, "tags!": []any{"low-replace"}},
	}}}
	replaceResp, err := r.commitResponse(ctx, "source-field-low-replace", lowReplace)
	if err != nil {
		return err
	}
	if !hasSuppressedField(replaceResp.json, "tags") {
		return fmt.Errorf("expected low priority tags! suppression: %s", string(replaceResp.body))
	}
	entity, err := r.writer.do(ctx, http.MethodGet, entityPath(hostID), nil, http.StatusOK)
	if err != nil {
		return err
	}
	fields := mapValue(mapValue(entity.json["entity"])["fields"])
	if got := stringArray(fields["tags"]); !sameStrings(got, []string{"manual", "shared", "aws"}) {
		return fmt.Errorf("low priority tags! changed tags: %v", got)
	}
	manualReplace := graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: hostID, Kind: "host", Source: "manual", Confidence: 0.9,
		Fields: graph.Fields{"hostname": awsName, "tags!": []any{"manual-only"}},
	}}}
	if err := r.commit(ctx, "source-field-manual-replace", manualReplace); err != nil {
		return err
	}
	entity, err = r.writer.do(ctx, http.MethodGet, entityPath(hostID), nil, http.StatusOK)
	if err != nil {
		return err
	}
	fields = mapValue(mapValue(entity.json["entity"])["fields"])
	if got := stringArray(fields["tags"]); !sameStrings(got, []string{"manual-only"}) {
		return fmt.Errorf("manual tags! replace = %v", got)
	}
	return nil
}

func (r *runner) checkSourceFieldIngest(ctx context.Context) error {
	ingestExternalID := "aws-ingest-" + r.cfg.tenant
	ingestName := "aws-ingest-" + r.cfg.tenant
	ingest := storage.IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-source-fields",
		BatchID:        "source-fields-ingest",
		IdempotencyKey: "source-fields-ingest",
		Items: []storage.IngestItem{{
			ExternalID: ingestExternalID,
			Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{
				"instanceName":     ingestName,
				"privateIpAddress": "10.0.1.7",
				"region":           "aws-ingest-region",
				"tags":             []any{"ingest"},
			}},
		}},
	}
	ingestResp, err := r.writer.do(ctx, http.MethodPost, "/v1/ingest/batches", ingest, http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(ingestResp.json["failed"]) != 0 || intValue(ingestResp.json["applied"]) != 1 {
		return fmt.Errorf("source field ingest result = %s", string(ingestResp.body))
	}
	ingestID := graph.CanonicalEntityIDParts("host", "aws", ingestExternalID)
	ingested, err := r.writer.do(ctx, http.MethodGet, entityPath(ingestID), nil, http.StatusOK)
	if err != nil {
		return err
	}
	fields := mapValue(mapValue(ingested.json["entity"])["fields"])
	if stringValue(fields["hostname"]) != ingestName || stringValue(fields["private_ip"]) != "10.0.1.7" {
		return fmt.Errorf("ingest alias fields not canonicalized: %s", string(ingested.body))
	}
	sources := mapValue(mapValue(ingested.json["entity"])["field_sources"])
	if intValue(mapValue(sources["hostname"])["priority"]) != 1200 ||
		intValue(mapValue(sources["private_ip"])["priority"]) != 900 {
		return fmt.Errorf("ingest field priorities not applied: %s", string(ingested.body))
	}
	return nil
}
