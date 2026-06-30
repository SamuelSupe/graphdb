package httpapi

import (
	"net/http"
	"os"
)

func (s *Server) openAPI(w http.ResponseWriter, _ *http.Request) {
	data, err := readOpenAPIFile()
	if err != nil {
		writeError(w, http.StatusNotFound, "openapi contract not found")
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func readOpenAPIFile() ([]byte, error) {
	for _, path := range openAPIPaths() {
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err == nil {
			return data, nil
		}
	}
	return nil, os.ErrNotExist
}

func openAPIPaths() []string {
	return []string{
		os.Getenv("GRAPHDB_OPENAPI_PATH"),
		"docs/openapi.yaml",
		"/usr/local/share/graphdb/openapi.yaml",
		"../../docs/openapi.yaml",
	}
}
