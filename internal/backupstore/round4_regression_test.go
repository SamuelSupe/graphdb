package backupstore

import (
	"testing"
	"time"
)

func TestRound4ManifestsEqualIgnoresMonotonicClock(t *testing.T) {
	nowWithMono := time.Now().UTC()
	nowNoMono := nowWithMono.Round(0)
	a := Manifest{
		Format:      Format,
		Automatic:   true,
		TenantID:    "tenant-a",
		BackupID:    "b1",
		Version:     3,
		SnapshotKey: "prefix/tenant-a/b1/abc.parquet",
		Bytes:       128,
		SHA256:      "abc",
		CreatedAt:   nowWithMono,
	}
	b := a
	b.CreatedAt = nowNoMono
	if !manifestsEqual(a, b) {
		t.Fatalf("manifestsEqual failed for equal timestamps differing only in monotonic reading: a=%v b=%v", a.CreatedAt, b.CreatedAt)
	}
}
