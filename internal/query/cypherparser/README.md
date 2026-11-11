# Parser provenance

The grammar is the openCypher 9 M23 ANTLR artifact:
https://s3.amazonaws.com/artifacts.opencypher.org/M23/Cypher.g4

The matching upstream tag is `1.0.0-M23`, commit
`007895aff5f33097d67b2e48a0a2babd6bd18590` in
https://github.com/opencypher/openCypher.

One explicit adaptation adds `shortestPath` and `allShortestPaths` productions
from the version-9 reference, which the published ANTLR artifact omits. This
does not add updating syntax to the supported execution profile. The grammar's
copyright and attribution notice are retained; LICENSE and NOTICE are copied
from that upstream revision.

Generated Go sources use ANTLR **4.13.1** and the matching Go runtime. Regenerate
from the repository root with `sh scripts/generate_cypher.sh`. The script checks
the generator's SHA-256 and needs Java, Python 3, curl and gofmt. Java is a build
tool only; the database runtime remains a Go executable.

The script also moves ANTLR 4.13.1's generated label-keepalive `goto` from after
the return into a constant-false guard before it. This preserves parser behavior
and label references while allowing the repository's full `go vet` check.

The derived TCK fixture `../testdata/cypher-tck.json` comes from the same upstream
revision and retains file/scenario identifiers. Regenerate with
`python3 scripts/import_cypher_tck.py /path/to/openCypher`. It covers empty/any
graph expression and selected read-clause result scenarios. Graph fixtures,
error-category assertions and unsupported expected-value formats are reported
as adapter exclusions, rather than passed scenarios. It is not the complete TCK.
