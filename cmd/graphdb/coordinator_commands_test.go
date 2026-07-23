package main

import "testing"

func TestParseCoordinatorRollbackArgs(t *testing.T) {
	if dryRun, err := parseCoordinatorRollbackArgs([]string{"--dry-run"}); err != nil || !dryRun {
		t.Fatalf("dry-run = %v err=%v", dryRun, err)
	}
	if dryRun, err := parseCoordinatorRollbackArgs(
		[]string{"--apply", "--writers-stopped"},
	); err != nil || dryRun {
		t.Fatalf("apply = dry-run %v err=%v", dryRun, err)
	}
	for _, args := range [][]string{
		nil,
		{"--apply"},
		{"--writers-stopped", "--apply"},
	} {
		if _, err := parseCoordinatorRollbackArgs(args); err == nil {
			t.Fatalf("args %#v were accepted", args)
		}
	}
}
