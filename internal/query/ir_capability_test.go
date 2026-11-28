package query

import "testing"

func TestCompileRequestNormalizesIRAcrossOps(t *testing.T) {
	t.Run("impact_defaults", func(t *testing.T) {
		ir, err := CompileRequest(Request{Op: "impact", ID: "n:1"})
		if err != nil {
			t.Fatalf("CompileRequest impact: %v", err)
		}
		if ir.EffectiveOp != "impact" || ir.Normalized.DirectionStrategy != "impact" || ir.Normalized.Depth != 4 {
			t.Fatalf("unexpected impact IR: %+v", ir)
		}
	})

	t.Run("stepped_shortest_path_clamps_depth", func(t *testing.T) {
		ir, err := CompileRequest(Request{
			Op:       "shortest_path",
			ID:       "n:1",
			TargetID: "n:3",
			Depth:    8,
			Path: PathFilter{Steps: []PathStep{
				{Direction: "out"},
				{Direction: "out"},
			}},
		})
		if err != nil {
			t.Fatalf("CompileRequest shortest_path: %v", err)
		}
		if ir.Normalized.Depth != 2 {
			t.Fatalf("expected stepped shortest_path depth clamped to 2, got %d", ir.Normalized.Depth)
		}
	})

	t.Run("wrapper_profile_and_explain", func(t *testing.T) {
		ir, err := CompileRequest(Request{
			Op:       "profile",
			TargetOp: "pattern",
			Kind:     "service",
			Path:     PathFilter{Steps: []PathStep{{RelationTypes: []string{"calls"}}}},
		})
		if err != nil {
			t.Fatalf("CompileRequest profile pattern: %v", err)
		}
		if ir.WrapperOp != "profile" || ir.EffectiveOp != "pattern" || !ir.Normalized.Profile || ir.Normalized.Direction != "out" || ir.Normalized.Depth != 1 {
			t.Fatalf("unexpected wrapped pattern IR: %+v", ir)
		}
	})
}

func TestLazyOperatorCapabilityTableCoversAllSupportedOps(t *testing.T) {
	wantOps := []string{"match", "pattern", "traverse", "shortest_path", "neighbors", "impact"}
	if len(lazyOperatorCapabilities) != len(wantOps) {
		t.Fatalf("lazyOperatorCapabilities len = %d, want %d", len(lazyOperatorCapabilities), len(wantOps))
	}
	for _, op := range wantOps {
		cap, ok := lazyOperatorCapabilities[op]
		if !ok || cap.CanExecute == nil || cap.RequiresReverse == nil {
			t.Fatalf("operator %q missing complete capability descriptor", op)
		}
	}

	// Verify neighbors with DirectionStrategy=impact checks reverse edge shards consistently.
	statsNoReverse := PlannerStats{
		Version:                   1,
		EntityPages:               []PlannerEntityPageStat{{Shard: "00", EntityCount: 2}},
		ForwardEdgeIndexAvailable: true,
		EdgeShards:                []PlannerEdgeStat{{RelationType: "feeds", ImpactDirection: "reverse", Shard: "00", EdgeCount: 1}},
	}
	req := Request{Op: "neighbors", ID: "n:1", DirectionStrategy: "impact", RelationTypes: []string{"feeds"}}
	if SupportsLazyRead(req, statsNoReverse) {
		t.Fatalf("neighbors(impact) should not admit lazy read when reverse shard for reverse-impact relation is absent")
	}
	if !RequiresReverseIndex(req) {
		t.Fatalf("neighbors(impact) must report RequiresReverseIndex=true")
	}
}
