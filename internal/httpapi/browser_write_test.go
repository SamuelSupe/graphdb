package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestBrowserWriteProtectionPreventsPurge(t *testing.T) {
	for _, test := range []struct {
		name, origin, contentType, fetchSite string
		want                                 int
	}{
		{name: "cross origin", origin: "https://attacker.example", contentType: "application/json", want: http.StatusForbidden},
		{name: "null origin", origin: "null", want: http.StatusForbidden},
		{name: "cross site without origin", fetchSite: "cross-site", want: http.StatusForbidden},
		{name: "plain text", contentType: "text/plain", want: http.StatusUnsupportedMediaType},
		{name: "form", contentType: "application/x-www-form-urlencoded", want: http.StatusUnsupportedMediaType},
		{name: "same origin through proxy", origin: "https://graphdb.example", contentType: "application/json", want: http.StatusOK},
		{name: "non browser client", contentType: "application/json", want: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
			if _, err := store.CreateTenant(context.Background(), "tenant-a", storage.TenantCreateOptions{}); err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "http://graphdb.example/v1/tenants/tenant-a/purge?force=true", strings.NewReader("{}"))
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Content-Type", test.contentType)
			request.Header.Set("Sec-Fetch-Site", test.fetchSite)
			response := httptest.NewRecorder()
			(&Server{Store: store, Mode: "all"}).Handler().ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body)
			}
			if test.want != http.StatusOK {
				if test.want == http.StatusForbidden {
					var failure ErrorResponse
					if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil || failure.Code != ErrorCodeOperationDisabled {
						t.Fatalf("browser denial error contract: %+v %v", failure, err)
					}
				}
				info, err := store.GetTenantInfo(context.Background(), "tenant-a")
				if err != nil || info.Status != storage.TenantStatusActive {
					t.Fatalf("rejected request changed tenant: %+v %v", info, err)
				}
			}
		})
	}
}
