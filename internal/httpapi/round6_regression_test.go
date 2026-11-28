package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound6ReaderModeControlRepairProtectedAgainstCSRF(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "r6")
	srv := &Server{
		Store: store,
		Mode:  "reader",
	}
	handler := srv.Handler()

	// Cross-site browser POST to /v1/control/repair must be blocked by ProtectBrowserWrite even in reader mode.
	req := httptest.NewRequest(http.MethodPost, "/v1/control/repair", strings.NewReader(`{"mode":"inspect"}`))
	req.Header.Set("Origin", "https://evil.example")
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden && rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected CSRF/media-type rejection on reader-mode /v1/control/repair, got %d: %s", rec.Code, rec.Body.String())
	}

	// Non-browser POST /v1/commits in reader mode must still return 405 Method Not Allowed.
	commitReq := httptest.NewRequest(http.MethodPost, "/v1/commits", strings.NewReader(`{}`))
	commitRec := httptest.NewRecorder()
	handler.ServeHTTP(commitRec, commitReq)
	if commitRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on reader-mode /v1/commits, got %d", commitRec.Code)
	}
}

func TestRound6TenantUsageCachePanicAllowsWaiterRetry(t *testing.T) {
	cache := newTenantUsageCache(time.Minute)
	started := make(chan struct{})
	releasePanic := make(chan struct{})

	var calls int
	var mu sync.Mutex
	loader := func(ctx context.Context, tenantID string) (storage.TenantUsageReport, error) {
		mu.Lock()
		calls++
		callNum := calls
		mu.Unlock()
		if callNum == 1 {
			close(started)
			<-releasePanic
			panic("boom")
		}
		return storage.TenantUsageReport{TenantID: tenantID, ObjectCount: 42}, nil
	}

	var leaderPanicked bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if recover() != nil {
				leaderPanicked = true
			}
		}()
		_, _ = cache.getOrLoad(context.Background(), "t1", time.Now().UTC(), loader)
	}()

	<-started
	waiterDone := make(chan struct{})
	var waiterReport storage.TenantUsageReport
	var waiterErr error
	go func() {
		defer close(waiterDone)
		waiterReport, waiterErr = cache.getOrLoad(context.Background(), "t1", time.Now().UTC(), loader)
	}()

	// Wait briefly until waiter is queued on active.done, then trigger panic.
	time.Sleep(25 * time.Millisecond)
	close(releasePanic)
	wg.Wait()
	<-waiterDone

	if !leaderPanicked {
		t.Fatalf("expected leader call to panic")
	}
	if waiterErr != nil {
		t.Fatalf("expected coalesced waiter to retry cleanly, got err=%v", waiterErr)
	}
	if waiterReport.TenantID != "t1" || waiterReport.ObjectCount != 42 {
		t.Fatalf("expected waiter to receive retried report (ObjectCount=42), got %+v", waiterReport)
	}
}
