package graphdb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRound3BackupAutomationSendsTenantHeader(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path+" tenant="+r.Header.Get("X-Tenant-ID"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(BackupAutomationStatus{TaskID: "task-1"})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithTenant("tenant-alpha"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetBackupAutomation(context.Background()); err != nil {
		t.Fatalf("GetBackupAutomation failed: %v", err)
	}
	if _, err := client.ResetBackupAutomation(context.Background()); err != nil {
		t.Fatalf("ResetBackupAutomation failed: %v", err)
	}
	want := []string{
		"GET /v1/backup-automation tenant=tenant-alpha",
		"POST /v1/backup-automation/reset tenant=tenant-alpha",
	}
	if len(seen) != len(want) || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("backup automation requests=%v want=%v", seen, want)
	}
}
