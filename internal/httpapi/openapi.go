package httpapi

import (
	"net/http"
	"os"
	"sync"
	"time"
)

var openAPICache struct {
	mu      sync.RWMutex
	path    string
	modTime time.Time
	size    int64
	data    []byte
}

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
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		openAPICache.mu.RLock()
		if openAPICache.path == path && openAPICache.size == info.Size() && openAPICache.modTime.Equal(info.ModTime()) && openAPICache.data != nil {
			data := openAPICache.data
			openAPICache.mu.RUnlock()
			return data, nil
		}
		openAPICache.mu.RUnlock()
		data, err := os.ReadFile(path)
		if err == nil {
			openAPICache.mu.Lock()
			openAPICache.path = path
			openAPICache.size = info.Size()
			openAPICache.modTime = info.ModTime()
			openAPICache.data = data
			openAPICache.mu.Unlock()
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
