package adminui

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

var (
	blockCommentRE = regexp.MustCompile(`(?s)/\*.*?\*/`)
	lineCommentRE  = regexp.MustCompile(`(?m)(^|[^:\\])//.*$`)
)

// TestAdminUIStaticAssetsAvoidUnguardedSecureContextAPIs prevents regressions
// where plain-HTTP intranet deployments fail at runtime due to bare calls to
// browser APIs restricted to Secure Contexts (e.g. crypto.randomUUID).
func TestAdminUIStaticAssetsAvoidUnguardedSecureContextAPIs(t *testing.T) {
	forbidden := []struct {
		name             string
		pattern          *regexp.Regexp
		allowedInHelpers map[string]bool
	}{
		{
			name:             "bare crypto.randomUUID()",
			pattern:          regexp.MustCompile(`\bcrypto\.randomUUID\s*\(`),
			allowedInHelpers: map[string]bool{"static/api.js": true},
		},
		{
			name:    "unguarded crypto.subtle",
			pattern: regexp.MustCompile(`\bcrypto\.subtle\b`),
		},
		{
			name:    "unguarded navigator.clipboard",
			pattern: regexp.MustCompile(`\bnavigator\.clipboard\b`),
		},
		{
			name:    "unguarded navigator.serviceWorker",
			pattern: regexp.MustCompile(`\bnavigator\.serviceWorker\b`),
		},
		{
			name:    "unguarded showOpenFilePicker/showSaveFilePicker",
			pattern: regexp.MustCompile(`\bshow(Open|Save)FilePicker\b`),
		},
	}

	err := fs.WalkDir(assets, "static", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".js") {
			return nil
		}
		raw, err := fs.ReadFile(assets, path)
		if err != nil {
			return err
		}
		code := stripJSComments(string(raw))
		for _, rule := range forbidden {
			if rule.pattern.MatchString(code) {
				if rule.allowedInHelpers[path] {
					// api.js must guard crypto.randomUUID with a typeof check and getRandomValues fallback.
					if !strings.Contains(code, "typeof crypto.randomUUID === 'function'") || !strings.Contains(code, "crypto.getRandomValues") {
						t.Errorf("%s calls %s without feature detection and fallback", path, rule.name)
					}
					continue
				}
				t.Errorf("%s uses %s; route through a guarded helper in api.js so plain HTTP works", path, rule.name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk static assets: %v", err)
	}
}

func stripJSComments(src string) string {
	noBlock := blockCommentRE.ReplaceAllString(src, "")
	return lineCommentRE.ReplaceAllString(noBlock, "$1")
}
