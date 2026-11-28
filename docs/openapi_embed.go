package docs

import _ "embed"

// OpenAPISpec embeds the canonical docs/openapi.yaml contract into the binary
// so GET /openapi.yaml works regardless of working directory or container layout.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
