// Package adminui serves the embedded management console without a build step.
package adminui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static/*
var assets embed.FS

// Register adds the console to a management or combined listener. API requests
// still use the existing listeners and their gateway authorization contracts.
func Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Path+"/", http.StatusTemporaryRedirect)
	})
	mux.Handle("GET /ui/", Handler())
}

// Handler serves only embedded files beneath /ui/. It also supports routers,
// which use a single handler instead of a ServeMux.
func Handler() http.Handler {
	files, _ := fs.Sub(assets, "static")
	server := http.StripPrefix("/ui/", http.FileServer(http.FS(files)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/ui" {
			http.Redirect(w, r, r.URL.Path+"/", http.StatusTemporaryRedirect)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/ui/")
		if name == "" {
			name = "index.html"
		}
		info, err := fs.Stat(files, name)
		if err != nil || info.IsDir() {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self' http: https:; object-src 'none'; base-uri 'none'; frame-ancestors 'self'")
		server.ServeHTTP(w, r)
	})
}
