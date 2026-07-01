package graph

import "testing"

func TestSourcePolicyFieldOwnershipSuppressesLowerPriority(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"owner": "collector"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("aws write: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"owner": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws-again",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"owner": "collector-2"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("aws rewrite: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "platform" {
		t.Fatalf("owner = %#v", entity.Fields["owner"])
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "owner" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
	if entity.FieldSources["owner"].Source != "manual" || entity.FieldSources["owner"].Priority != 1000 {
		t.Fatalf("field source = %#v", entity.FieldSources["owner"])
	}
}

func TestSourcePolicyFieldAliasesMapBeforeMerge(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "aws", Priority: 50},
		},
		FieldAliases: []FieldAliasRule{
			{Source: "aws", Aliases: map[string]string{"host_name": "display_name"}},
			{Source: "aws", Kind: "host", Aliases: map[string]string{
				"host_name":        "hostname",
				"privateIpAddress": "private_ip",
			}},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws",
			Fields: Fields{"host_name": "web-1", "privateIpAddress": "10.0.0.1"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("apply alias write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["hostname"] != "web-1" || entity.Fields["private_ip"] != "10.0.0.1" {
		t.Fatalf("fields = %#v", entity.Fields)
	}
	if _, ok := entity.Fields["host_name"]; ok {
		t.Fatalf("alias field was persisted: %#v", entity.Fields)
	}
	if _, ok := entity.Fields["display_name"]; ok {
		t.Fatalf("kind-specific alias did not override global rule: %#v", entity.Fields)
	}
	if owner := entity.FieldSources["hostname"]; owner.Source != "aws" || owner.Priority != 50 {
		t.Fatalf("canonical field source = %#v", owner)
	}
}

func TestSourcePolicyFieldPrioritiesOverrideSourcePriority(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
		FieldPriorities: []FieldPriorityRule{{
			Source: "aws",
			Kind:   "host",
			Fields: map[string]int{"hostname": 1200},
		}},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual",
			Fields: Fields{"hostname": "manual-host", "owner": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws",
			Fields: Fields{"hostname": "aws-host", "owner": "collector"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("aws write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["hostname"] != "aws-host" || entity.Fields["owner"] != "platform" {
		t.Fatalf("fields = %#v", entity.Fields)
	}
	if owner := entity.FieldSources["hostname"]; owner.Source != "aws" || owner.Priority != 1200 {
		t.Fatalf("hostname field source = %#v", owner)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "owner" || report.Suppressed[0].IncomingPriority != 50 {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestSourcePolicyKindFieldPriorityOverridesGlobal(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
		FieldPriorities: []FieldPriorityRule{
			{Source: "aws", Fields: map[string]int{"hostname": 40}},
			{Source: "aws", Kind: "host", Fields: map[string]int{"hostname": 1200}},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"hostname": "manual-host"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"hostname": "aws-host"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("aws write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["hostname"] != "aws-host" || entity.FieldSources["hostname"].Priority != 1200 {
		t.Fatalf("entity = %#v", entity)
	}
}

func TestSourcePolicyFieldPriorityUsesCanonicalAliasField(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
		FieldAliases: []FieldAliasRule{{Source: "aws", Kind: "host", Aliases: map[string]string{"privateIpAddress": "private_ip"}}},
		FieldPriorities: []FieldPriorityRule{{
			Source: "aws",
			Kind:   "host",
			Fields: map[string]int{"private_ip": 1200},
		}},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"private_ip": "10.0.0.1"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"privateIpAddress": "10.0.0.2"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("aws write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["private_ip"] != "10.0.0.2" || entity.FieldSources["private_ip"].Priority != 1200 {
		t.Fatalf("entity = %#v", entity)
	}
	if _, ok := entity.Fields["privateIpAddress"]; ok {
		t.Fatalf("alias field persisted: %#v", entity.Fields)
	}
}

func TestSourcePolicyFieldPriorityDoesNotLetOverrideMarkerBypassPriority(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
		FieldPriorities: []FieldPriorityRule{{Source: "aws", Kind: "host", Fields: map[string]int{"tags": 50}}},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "schema",
		Version: 1,
		Mutations: Mutations{UpsertCITypes: []CIType{{Name: "host", Fields: map[string]FieldSpec{
			"tags": {Type: "array", MergeStrategy: FieldMergeAppendUnique},
		}}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"tags": []any{"manual"}},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws-replace",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"tags!": []any{"aws"}},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("aws replace: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	assertFieldSlice(t, entity.Fields["tags"], []any{"manual"})
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "tags" || report.Suppressed[0].IncomingPriority != 50 {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestSourcePolicyFieldAliasCanonicalWins(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		Sources:      []SourcePolicyItem{{Name: "aws", Priority: 50}},
		FieldAliases: []FieldAliasRule{{Source: "aws", Aliases: map[string]string{"host_name": "hostname"}}},
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws",
			Fields: Fields{"hostname": "canonical", "host_name": "alias"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("apply alias write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["hostname"] != "canonical" {
		t.Fatalf("hostname = %#v", entity.Fields["hostname"])
	}
	if _, ok := entity.Fields["host_name"]; ok {
		t.Fatalf("alias field was persisted: %#v", entity.Fields)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "hostname" || report.Suppressed[0].AliasField != "host_name" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestSourcePolicyMultipleAliasesChooseDeterministically(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		Sources: []SourcePolicyItem{{Name: "aws", Priority: 50}},
		FieldAliases: []FieldAliasRule{{Source: "aws", Aliases: map[string]string{
			"zName": "hostname",
			"aName": "hostname",
		}}},
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws",
			Fields: Fields{"zName": "z-value", "aName": "a-value"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("apply alias write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["hostname"] != "a-value" {
		t.Fatalf("hostname = %#v fields=%#v", entity.Fields["hostname"], entity.Fields)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].AliasField != "zName" || report.Suppressed[0].ExistingValue != "a-value" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestSourcePolicyFieldAliasesRequireMatchingSource(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		FieldAliases:    []FieldAliasRule{{Source: "aws", Aliases: map[string]string{"host_name": "hostname"}}},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "unknown",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Fields: Fields{"host_name": "web-1"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("apply unknown source write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["host_name"] != "web-1" {
		t.Fatalf("fields = %#v", entity.Fields)
	}
	if _, ok := entity.Fields["hostname"]; ok {
		t.Fatalf("alias applied without source: %#v", entity.Fields)
	}
}

func TestIncomingEntitySourcesCannotElevateWriteOwner(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"owner": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "agent-spoof",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent", Fields: Fields{"owner": "collector"},
			Sources: []EntitySource{{Source: "manual", ExternalID: "spoofed-manual"}},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("agent spoof write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "platform" {
		t.Fatalf("spoofed source elevated write owner: %#v", entity)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].IncomingSource != "agent" || report.Suppressed[0].IncomingPriority != 100 {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestIncomingEntitySourcesCannotSpoofIdentityAlias(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "manual",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:manual", Kind: "host", Source: "manual", ExternalID: "manual-raw",
			SourceRank: 1000, Fields: Fields{"owner": "platform"},
		}}},
	}); err != nil {
		t.Fatalf("manual write: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "agent-spoof",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:agent", Kind: "host", Source: "agent", ExternalID: "agent-raw",
			Sources: []EntitySource{{Source: "manual", ExternalID: "manual-raw", Priority: 1000}},
			Fields:  Fields{"owner": "collector"},
		}}},
	}); err != nil {
		t.Fatalf("agent spoof write: %v", err)
	}
	manual, ok := g.GetEntity("host:manual")
	if !ok || manual.Fields["owner"] != "platform" {
		t.Fatalf("manual entity = %#v ok=%v", manual, ok)
	}
	agent, ok := g.GetEntity("host:agent")
	if !ok || agent.Fields["owner"] != "collector" {
		t.Fatalf("agent entity = %#v ok=%v", agent, ok)
	}
	if len(g.Entities) != 2 {
		t.Fatalf("entities = %#v, spoofed source alias merged into manual entity", g.Entities)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "manual-alias",
		Version: 3,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:manual-rename", Kind: "host", Source: "manual", ExternalID: "manual-raw",
			Fields: Fields{"rack": "r1"},
		}}},
	}); err != nil {
		t.Fatalf("top-level alias merge: %v", err)
	}
	manual, _ = g.GetEntity("host:manual")
	if len(g.Entities) != 2 || manual.Fields["rack"] != "r1" {
		t.Fatalf("top-level source alias did not merge: entities=%#v manual=%#v", g.Entities, manual)
	}
}

func TestEqualPriorityHigherConfidenceWins(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "low",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent-a", SourceRank: 10, Confidence: 0.4, Fields: Fields{"region": "old"},
		}}},
	}); err != nil {
		t.Fatalf("low confidence: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "high",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "agent-b", SourceRank: 10, Confidence: 0.9, Fields: Fields{"region": "new"},
		}}},
	}); err != nil {
		t.Fatalf("high confidence: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["region"] != "new" {
		t.Fatalf("region = %#v", entity.Fields["region"])
	}
}

func TestEntitySourceMergeKeepsHigherPriorityAliasRecord(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "high-priority",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "collector", ExternalID: "raw-host",
			SourceRank: 1000, Confidence: 0.1, Fields: Fields{"region": "manual"},
		}}},
	}); err != nil {
		t.Fatalf("high priority: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "lower-priority-confidence",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "collector", ExternalID: "raw-host",
			SourceRank: 100, Confidence: 1, Fields: Fields{"zone": "collector-zone"},
		}}},
	}); err != nil {
		t.Fatalf("lower priority: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if len(entity.Sources) != 1 {
		t.Fatalf("sources = %#v", entity.Sources)
	}
	if entity.Sources[0].Priority != 1000 || entity.Sources[0].Confidence != 0.1 {
		t.Fatalf("source alias was downgraded by lower priority write: %#v", entity.Sources[0])
	}
}

func TestEmptyIncomingFieldDoesNotClearExistingValue(t *testing.T) {
	g := New()
	policy := SourcePolicy{
		DefaultPriority: 0,
		Sources: []SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "aws", Priority: 50},
		},
	}
	if _, err := g.ApplyCommitWithOptions(Commit{
		ID:      "aws",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", Fields: Fields{"owner": "collector", "team": ""},
		}}},
	}, ApplyOptions{SourcePolicy: &policy}); err != nil {
		t.Fatalf("aws write: %v", err)
	}
	report, err := g.ApplyCommitWithOptions(Commit{
		ID:      "manual-empty",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", Fields: Fields{"owner": "", "team": "platform"},
		}}},
	}, ApplyOptions{SourcePolicy: &policy})
	if err != nil {
		t.Fatalf("manual write: %v", err)
	}
	entity, _ := g.GetEntity("host:1")
	if entity.Fields["owner"] != "collector" {
		t.Fatalf("owner was cleared: %#v", entity.Fields)
	}
	if entity.Fields["team"] != "platform" {
		t.Fatalf("empty field was not filled: %#v", entity.Fields)
	}
	if len(report.Suppressed) != 1 || report.Suppressed[0].Field != "owner" {
		t.Fatalf("suppressed = %#v", report.Suppressed)
	}
}

func TestNoPolicyKeepsSourcePriorityBehaviorAndSnapshotFieldSources(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "low",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "aws", SourceRank: 10, Fields: Fields{"owner": "collector"},
		}}},
	}); err != nil {
		t.Fatalf("low priority: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "high",
		Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{{
			ID: "host:1", Kind: "host", Source: "manual", SourceRank: 100, Fields: Fields{"owner": "manual"},
		}}},
	}); err != nil {
		t.Fatalf("high priority: %v", err)
	}
	loaded, err := FromSnapshot(g.Snapshot())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	entity, _ := loaded.GetEntity("host:1")
	if entity.Fields["owner"] != "manual" || entity.FieldSources["owner"].Source != "manual" {
		t.Fatalf("entity after snapshot = %#v", entity)
	}
}
