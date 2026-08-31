package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPIngestAcceptanceCarriesStableOwnerAndStatusRecovery(t *testing.T) {
	store := storage.NewTenantStore(&lifecycleLookupUnavailableObjectStore{
		ObjectStore: storage.NewMemoryStore(),
	}, "test")
	store.InstanceID = "writer-a"
	service := &ownerRoutingIngestService{
		acceptance: storage.IngestAcceptance{
			WriterID: "writer-a",
			Source:   "agent", CollectorID: "collector-a", BatchID: "batch-a",
			State: storage.IngestStateAccepted, Durability: "durable",
			AcceptedAt: time.Unix(100, 0).UTC(), EstimatedFlush: time.Unix(110, 0).UTC(),
		},
		status: storage.IngestBatchStatus{
			WriterID: "writer-a",
			TenantID: "tenant-a", Source: "agent", CollectorID: "collector-a", BatchID: "batch-a",
			State: storage.IngestStatePrepared, Durability: "durable", RecoveryPending: true,
		},
	}
	handler := (&Server{Store: store, Mode: "all", IngestService: service}).Handler()

	request := storage.IngestRequest{Source: "agent", CollectorID: "collector-a", BatchID: "batch-a"}
	body := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", request)
	if body.Code != http.StatusAccepted {
		t.Fatalf("accept status = %d body=%s", body.Code, body.Body.String())
	}
	var accepted struct {
		StatusURL string `json:"status_url"`
		WriterID  string `json:"writer_id"`
	}
	if err := json.Unmarshal(body.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.WriterID != "writer-a" {
		t.Fatalf("accepted writer_id = %q, want stable owner writer-a", accepted.WriterID)
	}
	if accepted.StatusURL != "/v1/ingest/writers/writer-a/agent/collector-a/batch-a" {
		t.Fatalf("accepted status_url = %q", accepted.StatusURL)
	}
	if body.Header().Get("Location") != accepted.StatusURL {
		t.Fatalf("Location = %q, status_url = %q", body.Header().Get("Location"), accepted.StatusURL)
	}

	status := serveJSON(handler, http.MethodGet, accepted.StatusURL, "tenant-a", nil)
	if status.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", status.Code, status.Body.String())
	}
	var statusBody storage.IngestBatchStatus
	if err := json.Unmarshal(status.Body.Bytes(), &statusBody); err != nil {
		t.Fatal(err)
	}
	if statusBody.State != storage.IngestStatePrepared || !statusBody.RecoveryPending {
		t.Fatalf("status = %#v, want prepared recovery_pending", statusBody)
	}
	wrongOwner := serveJSON(handler, http.MethodGet, "/v1/ingest/writers/writer-b/agent/collector-a/batch-a", "tenant-a", nil)
	if wrongOwner.Code != http.StatusConflict {
		t.Fatalf("wrong owner status code = %d body=%s, want conflict", wrongOwner.Code, wrongOwner.Body.String())
	}
}

type ownerRoutingIngestService struct {
	acceptance storage.IngestAcceptance
	status     storage.IngestBatchStatus
}

func (s *ownerRoutingIngestService) WriterID() string {
	return s.acceptance.WriterID
}

func (s *ownerRoutingIngestService) Accept(context.Context, string, storage.IngestRequest) (storage.IngestAcceptance, error) {
	return s.acceptance, nil
}

func (s *ownerRoutingIngestService) Wait(context.Context, storage.IngestAcceptance) (storage.IngestResult, error) {
	return storage.IngestResult{}, nil
}

func (s *ownerRoutingIngestService) Status(context.Context, string, string, string, string) (storage.IngestBatchStatus, error) {
	return s.status, nil
}

func (s *ownerRoutingIngestService) Readiness() storage.IngestServiceReadiness {
	return storage.IngestServiceReadiness{Ready: true, Writable: true, Recovered: false, Pending: 1}
}

func (s *ownerRoutingIngestService) ObserveMetrics() {}

var _ IngestService = (*ownerRoutingIngestService)(nil)

type lifecycleLookupUnavailableObjectStore struct {
	storage.ObjectStore
}

func (s *lifecycleLookupUnavailableObjectStore) GetWithMeta(
	context.Context,
	string,
) ([]byte, storage.ObjectMeta, error) {
	return nil, storage.ObjectMeta{}, storage.ErrObjectStoreUnavailable
}
