#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cypher_tmp=$(mktemp -d "${TMPDIR:-/tmp}/graphdb-cypher.XXXXXX")
trap 'rm -rf "$cypher_tmp"' EXIT HUP INT TERM
curl -fsSL https://www.antlr.org/download/antlr-4.13.1-complete.jar -o "$cypher_tmp/antlr.jar"
python3 - "$cypher_tmp/antlr.jar" <<'PY'
import hashlib, pathlib, sys
expected = "bc13a9c57a8dd7d5196888211e5ede657cb64a3ce968608697e4f668251a8487"
if hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest() != expected:
    raise SystemExit("ANTLR generator checksum mismatch")
PY
cd "$repo"
java -jar "$cypher_tmp/antlr.jar" -Dlanguage=Go -package cypherparser -no-listener \
    -Xexact-output-dir -o "$cypher_tmp/output" internal/query/cypherparser/Cypher.g4
cp "$cypher_tmp/output/cypher_lexer.go" "$cypher_tmp/output/cypher_parser.go" internal/query/cypherparser/
python3 - internal/query/cypherparser/cypher_parser.go <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
source = path.read_text()
old = "\treturn localctx\n\tgoto errorExit // Trick to prevent compiler error if the label is not used"
if old not in source:
    raise SystemExit("unexpected ANTLR label keepalive template")
source = source.replace(old, "\tif false { goto errorExit } // Keep the generated error label referenced.\n\treturn localctx")
path.write_text(source)
PY
gofmt -w internal/query/cypherparser/cypher_lexer.go internal/query/cypherparser/cypher_parser.go
