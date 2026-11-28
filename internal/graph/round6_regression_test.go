package graph

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestRound6ApplyPolicyToEntityNormalizesSourcesBeforePromotion(t *testing.T) {
	policy := SourcePolicy{
		Sources: []SourcePolicyItem{
			{Name: "cmdb", Priority: 90},
		},
	}

	entity := Entity{
		ID:   "host-1",
		Kind: "host",
		Sources: []EntitySource{
			{Source: "   ", ExternalID: "ignored-empty-source", Confidence: 0.9},
			{Source: "cmdb", ExternalID: "   ", Confidence: 0.9},
			{Source: " cmdb ", ExternalID: " ext-1 ", Confidence: math.NaN(), Priority: 10},
			{Source: "cmdb", ExternalID: "ext-1", Confidence: 0.85, Priority: 5},
		},
	}
	applyPolicyToEntity(&entity, policy)

	if len(entity.Sources) != 1 {
		t.Fatalf("expected 1 normalized deduplicated source, got %+v", entity.Sources)
	}
	if entity.Source != "cmdb" || entity.ExternalID != "ext-1" {
		t.Fatalf("expected single normalized source to be promoted, got source=%q external_id=%q", entity.Source, entity.ExternalID)
	}
	if entity.SourceRank != 90 {
		t.Fatalf("expected promoted SourceRank=90, got %d", entity.SourceRank)
	}
	if entity.Confidence != 0.85 {
		t.Fatalf("expected promoted Confidence=0.85 (clamping NaN to 0), got %v", entity.Confidence)
	}

	// Invalid confidence on a single source is clamped to 0.
	invalidConf := Entity{
		ID:   "host-2",
		Kind: "host",
		Sources: []EntitySource{
			{Source: "cmdb", ExternalID: "ext-2", Confidence: 1.5},
		},
	}
	applyPolicyToEntity(&invalidConf, policy)
	if invalidConf.Confidence != 0 {
		t.Fatalf("expected out-of-range confidence to be clamped to 0, got %v", invalidConf.Confidence)
	}
}

func TestRound6EntityAndEdgeMergeSuppressedOrderDeterministic(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	existingEntity := Entity{
		ID:         "e1",
		Kind:       "host",
		Source:     "authoritative",
		SourceRank: 100,
		Version:    1,
		UpdatedAt:  now,
		Fields: Fields{
			"z_field": "v1",
			"a_field": "v2",
			"m_field": "v3",
			"b_field": "v4",
		},
	}
	incomingEntity := Entity{
		ID:         "e1",
		Kind:       "host",
		Source:     "secondary",
		SourceRank: 10,
		Fields: Fields{
			"z_field": "x1",
			"a_field": "x2",
			"m_field": "",
			"b_field": "x4",
		},
	}

	wantEntityFields := []string{"a_field", "b_field", "m_field", "z_field"}
	for i := 0; i < 20; i++ {
		_, report := mergeEntityForUpsert(existingEntity, incomingEntity, "e1", nil, 2, now.Add(time.Second))
		got := make([]string, 0, len(report.Suppressed))
		for _, c := range report.Suppressed {
			got = append(got, c.Field)
		}
		if !reflect.DeepEqual(got, wantEntityFields) {
			t.Fatalf("iteration %d: entity suppressed fields = %v, want %v", i, got, wantEntityFields)
		}
	}

	existingEdge := Edge{
		ID:         "edge1",
		Type:       "depends_on",
		From:       "e1",
		To:         "e2",
		Source:     "authoritative",
		SourceRank: 100,
		Version:    1,
		UpdatedAt:  now,
		Fields: Fields{
			"z_field": "v1",
			"a_field": "v2",
			"m_field": "v3",
			"b_field": "v4",
		},
	}
	incomingEdge := Edge{
		ID:         "edge1",
		Type:       "depends_on",
		From:       "e1",
		To:         "e2",
		Source:     "secondary",
		SourceRank: 10,
		Fields: Fields{
			"z_field": "x1",
			"a_field": "x2",
			"m_field": "",
			"b_field": "x4",
		},
	}
	for i := 0; i < 20; i++ {
		_, upsertReport := mergeEdgeForUpsert(existingEdge, incomingEdge, "edge1", "edge1", 2, now.Add(time.Second))
		got := make([]string, 0, len(upsertReport.Suppressed))
		for _, c := range upsertReport.Suppressed {
			got = append(got, c.Field)
		}
		if !reflect.DeepEqual(got, wantEntityFields) {
			t.Fatalf("iteration %d: edge upsert suppressed fields = %v, want %v", i, got, wantEntityFields)
		}
		_, storedReport := mergeStoredEdge(existingEdge, incomingEdge, "edge1", "edge1", 2, now.Add(time.Second))
		gotStored := make([]string, 0, len(storedReport.Suppressed))
		for _, c := range storedReport.Suppressed {
			gotStored = append(gotStored, c.Field)
		}
		if !reflect.DeepEqual(gotStored, wantEntityFields) {
			t.Fatalf("iteration %d: stored edge suppressed fields = %v, want %v", i, gotStored, wantEntityFields)
		}
	}
}
