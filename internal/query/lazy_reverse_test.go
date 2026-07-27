package query

import "testing"

func TestRequiresReverseIndex(t *testing.T) {
	tests := []struct {
		name    string
		request Request
		want    bool
	}{
		{name: "match", request: Request{Op: "match", Kind: "host"}},
		{name: "neighbors out", request: Request{Op: "neighbors", Direction: "out"}},
		{name: "neighbors in", request: Request{Op: "neighbors", Direction: "in"}, want: true},
		{name: "traverse out", request: Request{Op: "traverse", Direction: "out", Depth: 2}},
		{
			name: "traverse incoming step",
			request: Request{
				Op:        "traverse",
				Direction: "out",
				Depth:     2,
				Path: PathFilter{Steps: []PathStep{
					{Direction: "out"},
					{Direction: "in"},
				}},
			},
			want: true,
		},
		{
			name: "pattern default out",
			request: Request{
				Op:   "pattern",
				Path: PathFilter{Steps: []PathStep{{}, {}}},
			},
		},
		{
			name: "pattern incoming step",
			request: Request{
				Op:   "pattern",
				Path: PathFilter{Steps: []PathStep{{Direction: "in"}}},
			},
			want: true,
		},
		{name: "impact", request: Request{Op: "impact"}, want: true},
		{
			name: "profile impact",
			request: Request{
				Op:       "profile",
				TargetOp: "impact",
			},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := RequiresReverseIndex(test.request); got != test.want {
				t.Fatalf("RequiresReverseIndex() = %t, want %t", got, test.want)
			}
		})
	}
}
