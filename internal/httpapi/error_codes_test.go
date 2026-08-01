package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPErrorCodeContractMapsProductErrors(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		err       error
		wantCode  ErrorCode
		retryable bool
	}{
		{
			name:      "lease held",
			status:    http.StatusBadRequest,
			err:       fmt.Errorf("%w: tenant lease", storage.ErrLeaseHeld),
			wantCode:  ErrorCodeLeaseHeld,
			retryable: true,
		},
		{
			name:      "object store unavailable",
			status:    http.StatusBadRequest,
			err:       fmt.Errorf("%w: bucket unavailable", storage.ErrObjectStoreUnavailable),
			wantCode:  ErrorCodeObjectStoreUnavailable,
			retryable: true,
		},
		{
			name:      "ingest WAL unavailable",
			status:    http.StatusBadRequest,
			err:       fmt.Errorf("%w: short write", storage.ErrIngestWALFenced),
			wantCode:  ErrorCodeIngestWALUnavailable,
			retryable: true,
		},
		{
			name:      "index stale",
			status:    http.StatusBadRequest,
			err:       fmt.Errorf("%w: catalog version mismatch", query.ErrIndexUnavailable),
			wantCode:  ErrorCodeIndexStale,
			retryable: false,
		},
		{
			name:      "tenant disabled",
			status:    http.StatusForbidden,
			err:       fmt.Errorf("%w: disabled by operator", storage.ErrTenantDisabled),
			wantCode:  ErrorCodeTenantDisabled,
			retryable: false,
		},
		{
			name:      "tenant deleted",
			status:    http.StatusGone,
			err:       fmt.Errorf("%w: deleted by operator", storage.ErrTenantDeleted),
			wantCode:  ErrorCodeTenantDeleted,
			retryable: false,
		},
		{
			name:      "query limit",
			status:    http.StatusTooManyRequests,
			err:       fmt.Errorf("%w: queue full", query.ErrLimitExceeded),
			wantCode:  ErrorCodeQueryLimitExceeded,
			retryable: true,
		},
		{
			name:      "manifest conflict",
			status:    http.StatusBadRequest,
			err:       fmt.Errorf("%w: manifest for tenant changed while publishing", storage.ErrConflict),
			wantCode:  ErrorCodeManifestCASConflict,
			retryable: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			writeErrorErr(rr, tt.status, tt.err)
			var body ErrorResponse
			decodeResponse(t, rr, &body)
			if body.Code != tt.wantCode || body.Retryable != tt.retryable {
				t.Fatalf("body = %#v, want code=%s retryable=%v", body, tt.wantCode, tt.retryable)
			}
		})
	}
}

func TestWriteStorageErrorUsesContractStatus(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   ErrorCode
	}{
		{name: "object store", err: storage.ErrObjectStoreUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: ErrorCodeObjectStoreUnavailable},
		{name: "ingest WAL", err: storage.ErrIngestWALFenced, wantStatus: http.StatusServiceUnavailable, wantCode: ErrorCodeIngestWALUnavailable},
		{name: "coordinator", err: storage.ErrCoordinatorUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: ErrorCodeCoordinatorUnavailable},
		{name: "write conflict", err: storage.ErrWriteConflict, wantStatus: http.StatusConflict, wantCode: ErrorCodeWriteConflict},
		{name: "task lease", err: storage.ErrTaskLeaseHeld, wantStatus: http.StatusConflict, wantCode: ErrorCodeTaskConflict},
		{name: "ingest repair", err: storage.ErrIngestRepairRequired, wantStatus: http.StatusConflict, wantCode: ErrorCodeRepairRequired},
		{name: "timeout", err: context.DeadlineExceeded, wantStatus: http.StatusGatewayTimeout, wantCode: ErrorCodeRequestTimeout},
		{name: "validation", err: fmt.Errorf("invalid field"), wantStatus: http.StatusBadRequest, wantCode: ErrorCodeBadRequest},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			writeStorageError(rr, tt.err)
			var body ErrorResponse
			decodeResponse(t, rr, &body)
			if rr.Code != tt.wantStatus || body.Code != tt.wantCode {
				t.Fatalf("status/code = %d/%s, want %d/%s", rr.Code, body.Code, tt.wantStatus, tt.wantCode)
			}
		})
	}
}

func TestHTTPErrorCodeContractMapsProductMessages(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		message   string
		wantCode  ErrorCode
		retryable bool
	}{
		{name: "reader not fresh", status: http.StatusServiceUnavailable, message: "reader not fresh: visible version is behind target", wantCode: ErrorCodeReaderNotFresh, retryable: true},
		{name: "task conflict", status: http.StatusConflict, message: `task "task-a" is still running`, wantCode: ErrorCodeTaskConflict, retryable: false},
		{name: "repair required", status: http.StatusConflict, message: "repair required before continuing", wantCode: ErrorCodeRepairRequired, retryable: false},
		{name: "quota exceeded", status: http.StatusTooManyRequests, message: "tenant entity quota exceeded", wantCode: ErrorCodeQuotaExceeded, retryable: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			writeError(rr, tt.status, tt.message)
			var body ErrorResponse
			decodeResponse(t, rr, &body)
			if body.Code != tt.wantCode || body.Retryable != tt.retryable {
				t.Fatalf("body = %#v, want code=%s retryable=%v", body, tt.wantCode, tt.retryable)
			}
		})
	}
}

func TestStableErrorCodesAreDocumented(t *testing.T) {
	assertUniqueStableCodes(t)
	openapi := readTextFile(t, "../../docs/openapi.yaml")
	docs := readTextFile(t, "../../docs/error_codes.md")
	want := stableErrorCodeStrings()
	if got := openAPIErrorCodeEnum(t, openapi); !reflect.DeepEqual(got, want) {
		t.Fatalf("openapi error code enum mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
	if got := documentedErrorCodes(t, docs); !reflect.DeepEqual(got, want) {
		t.Fatalf("docs/error_codes.md code table mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestErrorResponseRejectsUndocumentedCode(t *testing.T) {
	body := buildErrorResponse(ErrorCode("future_code"), "boom", true, nil)
	if body.Code != ErrorCodeInternal || body.Retryable {
		t.Fatalf("body = %#v, want internal non-retryable fallback", body)
	}
}

func TestErrorCodeWritersUseConstants(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read httpapi dir: %v", err)
	}
	for _, file := range files {
		name := file.Name()
		if file.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(".", name)
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			function, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			codeArg := -1
			switch function.Name {
			case "buildErrorResponse":
				codeArg = 0
			case "writeErrorDetail":
				codeArg = 2
			}
			if codeArg < 0 || len(call.Args) <= codeArg || !isInlineStringCode(call.Args[codeArg]) {
				return true
			}
			pos := fset.Position(call.Args[codeArg].Pos())
			t.Fatalf("%s:%d uses inline error code string; use an ErrorCode constant and update docs/openapi/tests first", path, pos.Line)
			return false
		})
	}
}

func assertUniqueStableCodes(t *testing.T) {
	t.Helper()
	seen := map[ErrorCode]struct{}{}
	for _, code := range stableErrorCodes {
		if code == "" {
			t.Fatal("stable error code cannot be empty")
		}
		if _, ok := seen[code]; ok {
			t.Fatalf("duplicate stable error code %q", code)
		}
		seen[code] = struct{}{}
	}
}

func stableErrorCodeStrings() []string {
	out := make([]string, 0, len(stableErrorCodes))
	for _, code := range stableErrorCodes {
		out = append(out, string(code))
	}
	return out
}

func openAPIErrorCodeEnum(t *testing.T, spec string) []string {
	t.Helper()
	lines := strings.Split(spec, "\n")
	inErrorResponse := false
	inCode := false
	inEnum := false
	var out []string
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "    ErrorResponse:"):
			inErrorResponse = true
		case inErrorResponse && strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") && !strings.HasPrefix(line, "    ErrorResponse:"):
			inErrorResponse = false
		case inErrorResponse && strings.TrimSpace(line) == "code:":
			inCode = true
		case inCode && strings.TrimSpace(line) == "enum:":
			inEnum = true
		case inEnum && strings.HasPrefix(line, "            - "):
			out = append(out, strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- ")))
		case inEnum:
			return out
		}
	}
	if len(out) == 0 {
		t.Fatal("openapi.yaml ErrorResponse.code enum not found")
	}
	return out
}

func documentedErrorCodes(t *testing.T, docs string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(docs, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		rest := strings.TrimPrefix(line, "| `")
		code, _, ok := strings.Cut(rest, "`")
		if !ok || code == "" {
			t.Fatalf("malformed error code table row %q", line)
		}
		out = append(out, code)
	}
	if len(out) == 0 {
		t.Fatal("docs/error_codes.md code table not found")
	}
	return out
}

func isInlineStringCode(expr ast.Expr) bool {
	if literal, ok := expr.(*ast.BasicLit); ok && literal.Kind == token.STRING {
		return true
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	function, ok := call.Fun.(*ast.Ident)
	if !ok || function.Name != "ErrorCode" {
		return false
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	return ok && literal.Kind == token.STRING
}

func readTextFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func decodeResponse(t *testing.T, rr *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(rr.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rr.Body.String())
	}
}

func TestHTTPReaderModeUsesOperationDisabledCode(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "reader"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{})
	var body ErrorResponse
	decodeResponse(t, rr, &body)
	if rr.Code != http.StatusMethodNotAllowed || body.Code != ErrorCodeOperationDisabled {
		t.Fatalf("status=%d body=%#v raw=%s", rr.Code, body, rr.Body.String())
	}
}
