package storage

import (
	"context"
	"testing"
)

func TestCommitTailAfterVersionKeepsOnlyUncompactedHistory(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	manifest := Manifest{
		CommitSegments: []CommitSegmentRef{
			{Key: "segment-1", FirstVersion: 1, LastVersion: 2, Count: 2},
			{Key: "segment-2", FirstVersion: 3, LastVersion: 5, Count: 3},
			{Key: "segment-3", FirstVersion: 6, LastVersion: 8, Count: 3},
		},
		CommitKeys: []string{
			store.commitKey("tenant-a", 4, "commit-4"),
			store.commitKey("tenant-a", 6, "commit-6"),
		},
	}

	segments, keys, err := store.commitTailAfterVersion(
		context.Background(), "tenant-a", manifest, 5,
	)
	if err != nil {
		t.Fatalf("trim commit tail: %v", err)
	}
	if len(segments) != 1 || segments[0].Key != "segment-3" {
		t.Fatalf("segments = %#v, want only segment-3", segments)
	}
	if len(keys) != 1 || keys[0] != manifest.CommitKeys[1] {
		t.Fatalf("keys = %#v, want only version 6", keys)
	}
}

func TestCommitTailAfterVersionKeepsCrossingSegment(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	manifest := Manifest{CommitSegments: []CommitSegmentRef{{
		Key: "segment-crossing", FirstVersion: 4, LastVersion: 7, Count: 4,
	}}}

	segments, keys, err := store.commitTailAfterVersion(
		context.Background(), "tenant-a", manifest, 5,
	)
	if err != nil {
		t.Fatalf("trim commit tail: %v", err)
	}
	if len(segments) != 1 || segments[0].Key != "segment-crossing" {
		t.Fatalf("segments = %#v, want crossing segment retained", segments)
	}
	if len(keys) != 0 {
		t.Fatalf("keys = %#v, want none", keys)
	}
}
