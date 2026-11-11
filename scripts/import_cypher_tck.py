#!/usr/bin/env python3
"""Extract empty-graph expression scenarios from the pinned upstream TCK.

Usage: python3 scripts/import_cypher_tck.py /path/to/openCypher
The upstream checkout must be 1.0.0-M23. Expected values come from upstream
tables; no GraphDB executable is used to produce this fixture.
"""
import ast
import json
import pathlib
import re
import subprocess
import sys

COMMIT = "007895aff5f33097d67b2e48a0a2babd6bd18590"
root = pathlib.Path(sys.argv[1])
if subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip() != COMMIT:
    raise SystemExit("expected openCypher 1.0.0-M23")


def table(text):
    rows = []
    for line in text.splitlines():
        if not line.strip().startswith("|"):
            if rows:
                break
            continue
        line = line.strip()[1:-1]
        rows.append([x.strip().replace(r"\|", "|").replace(r"\\", "\\") for x in re.split(r"(?<!\\)\|", line)])
    return rows


def value(text):
    # Translate only bare Cypher literals; quoted string contents stay intact.
    token = re.compile(r"('(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\")|\b(null|true|false)\b|\b([A-Za-z_]\w*)\s*(?=:)")
    text = token.sub(lambda m: m[1] if m[1] else {"null": "None", "true": "True", "false": "False"}[m[2]] if m[2] else repr(m[3]), text)
    return ast.literal_eval(text)


cases = []
paths = list((root / "tck/features/expressions").rglob("*.feature"))
paths += [p for p in (root / "tck/features/clauses").rglob("*.feature") if p.relative_to(root / "tck/features/clauses").parts[0].split("-")[0] in ("with", "return", "unwind", "union")]
for path in sorted(paths):
    text = path.read_text()
    scenarios = list(re.finditer(r"^  Scenario(?: Outline)?:\s*(.*)$", text, re.M))
    for i, match in enumerate(scenarios):
        block = text[match.end():scenarios[i+1].start() if i+1 < len(scenarios) else len(text)]
        expansions = [{}]
        examples = re.split(r"^\s*Examples:.*$", block, flags=re.M)
        if len(examples) > 1:
            expansions = []
            for example in examples[1:]:
                rows = table(example)
                expansions.extend(dict(zip(rows[0], row)) for row in rows[1:])
            block = examples[0]
        for index, substitution in enumerate(expansions):
            scenario = block
            for name, replacement in substitution.items():
                scenario = scenario.replace("<" + name + ">", replacement)
            case = {"source": str(path.relative_to(root / "tck/features")), "name": match[1] + (" / " + str(index+1) if substitution else "")}
            query = re.search(r'When executing query:\s*"""\s*\n(.*?)\n\s*"""', scenario, re.S)
            if not query:
                case["excluded"] = "scenario uses a different query step"
            elif "having executed" in scenario or not re.search(r"Given (?:any|an empty) graph", scenario):
                case["excluded"] = "requires graph fixture setup"
            elif "should be raised" in scenario:
                case["excluded"] = "error category conformance is tracked separately"
            elif "And no side effects" not in scenario:
                case["excluded"] = "scenario does not specify read-only side effects"
            elif "0.0 / 0.0" in query[1]:
                case["excluded"] = "finite-number profile excludes NaN"
            else:
                case["query"] = query[1]
                result = re.search(r"Then the result should be, (in any order|in order):\s*\n(.*?)(?=\n\s*(?:And |$))", scenario, re.S)
                try:
                    if result:
                        rows = table(result[2])
                        case["columns"] = rows[0]
                        case["rows"] = [[value(cell) for cell in row] for row in rows[1:]]
                        case["ordered"] = result[1] == "in order"
                    else:
                        raise ValueError("missing result table")
                    parameters = re.search(r"And parameters are:\s*\n(.*?)(?=\n\s*When )", scenario, re.S)
                    case["parameters"] = {row[0]: value(row[1]) for row in table(parameters[1])} if parameters else {}
                    json.dumps(case, allow_nan=False)
                except (ValueError, SyntaxError) as error:
                    case = {"source": case["source"], "name": case["name"], "excluded": "expected value/setup needs extended TCK adapter: " + str(error)}
            cases.append(case)

target = pathlib.Path(__file__).resolve().parents[1] / "internal/query/testdata/cypher-tck.json"
target.parent.mkdir(exist_ok=True)
target.write_text(json.dumps({"upstream": "https://github.com/opencypher/openCypher", "tag": "1.0.0-M23", "commit": COMMIT, "scope": "expressions and WITH/RETURN/UNWIND/UNION clauses; empty/any graph; expected result tables", "license": "Apache-2.0", "copyright": "Copyright (c) 2015-2023 Neo Technology, Network Engine for Objects in Lund AB", "license_file": "../cypherparser/LICENSE", "notice_file": "../cypherparser/NOTICE", "cases": cases}, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
print(json.dumps({"scenarios": len(cases), "runnable": sum("excluded" not in c for c in cases), "excluded": sum("excluded" in c for c in cases)}))
