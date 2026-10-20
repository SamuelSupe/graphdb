package httpapi

import (
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func protectBrowserWrite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions || storage.IsReplicatedContext(r.Context()) {
			next.ServeHTTP(w, r)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			// Compare the public authority, which reverse proxies preserve in
			// Host. Forwarded headers are not a trusted origin allowlist.
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || !strings.EqualFold(u.Host, r.Host) {
				writeErrorDetail(w, http.StatusForbidden, ErrorCodeOperationDisabled, "cross-origin writes are not allowed", false, nil)
				return
			}
		}
		if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			writeErrorDetail(w, http.StatusForbidden, ErrorCodeOperationDisabled, "cross-site writes are not allowed", false, nil)
			return
		}
		contentType := r.Header.Get("Content-Type")
		if contentType != "" || r.ContentLength != 0 {
			mediaType, _, err := mime.ParseMediaType(contentType)
			allowed := mediaType == "application/json"
			if r.URL.Path == "/v1/imports" {
				allowed = allowed || mediaType == "application/x-ndjson" || mediaType == "application/ndjson" || mediaType == "application/jsonl" || mediaType == "text/csv" || mediaType == "application/csv" || mediaType == "application/octet-stream"
			}
			if err != nil || !allowed {
				writeErrorDetail(w, http.StatusUnsupportedMediaType, ErrorCodeBadRequest, "unsupported write Content-Type", false, nil)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
