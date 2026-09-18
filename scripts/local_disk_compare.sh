#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${GRAPHDB_BENCH_RUN_ID:-disk-$(date -u +%Y%m%d%H%M%S)}"
OUTPUT="$ROOT/capacity-runs/$RUN_ID"
BASELINE=ffa854149b7d481cd56da58f4a1f2bf61a58af92
GO_IMAGE="${GRAPHDB_BENCH_GO_IMAGE:-golang:1.25-bookworm}"
MINIO_IMAGE="${GRAPHDB_BENCH_MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z}"
MC_IMAGE="${GRAPHDB_BENCH_MC_IMAGE:-minio/mc:RELEASE.2025-08-13T08-35-41Z}"
VOLUME="graphdb-$RUN_ID"
NETWORK="graphdb-$RUN_ID"
MINIO="graphdb-$RUN_ID-minio"
MODULE_CACHE="${GRAPHDB_BENCH_MODULE_CACHE:-$(go env GOMODCACHE)}"
WITH_S3="$(python3 - "$@" <<'PY'
import argparse
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--cases', nargs='+', default=['main-s3', 'main-local', 'new-local'])
args, _ = parser.parse_known_args()
print(int('main-s3' in args.cases))
PY
)"
[[ ! -e "$OUTPUT" ]] || { printf 'Output already exists: %s\n' "$OUTPUT" >&2; exit 1; }
mkdir -p "$OUTPUT/baseline"
cd "$ROOT"
git archive "$BASELINE" | tar -x -C "$OUTPUT/baseline"
cp internal/storage/disk_metrics.go "$OUTPUT/baseline/internal/storage/disk_metrics.go"
python3 - "$OUTPUT/baseline" <<'PY'
from pathlib import Path
import sys
root=Path(sys.argv[1])
for name in ('file.go','ingest_wal.go'):
 p=root/'internal/storage'/name
 p.write_text(p.read_text().replace('s.file.Sync()', 'syncStorageFile(s.file)').replace('file.Sync()', 'syncStorageFile(file)').replace('dir.Sync()', 'syncStorageFile(dir)'))
p=root/'internal/httpapi/server.go'
s=p.read_text()
needle='_, _ = w.Write(s.obs().Metrics.SnapshotPrometheus())'
assert needle in s
p.write_text(s.replace(needle,needle+'\n\tstorage.WriteDiskMetrics(w)'))
PY
git diff --binary >"$OUTPUT/candidate.diff"
python3 - "$OUTPUT" <<'PYHASH'
import hashlib,json,subprocess,sys,tarfile
from pathlib import Path
paths=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z']).split(b'\0')
files={}
for raw in paths:
 if not raw: continue
 path=Path(raw.decode())
 files[str(path)]=hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
(Path(sys.argv[1])/'candidate-files.json').write_text(json.dumps(files,indent=2)+'\n')
with tarfile.open(Path(sys.argv[1])/'candidate-source.tar.gz','w:gz') as archive:
 for name,digest in files.items():
  if digest is not None: archive.add(name,arcname=name,recursive=False)
PYHASH
docker info >"$OUTPUT/docker-info.txt"
IMAGES=("$GO_IMAGE")
if [[ "$WITH_S3" == 1 ]]; then IMAGES+=("$MINIO_IMAGE" "$MC_IMAGE"); fi
docker image inspect "${IMAGES[@]}" >"$OUTPUT/images.json"
docker volume create "$VOLUME" > /dev/null
docker network create "$NETWORK" > /dev/null
MINIO_STARTED=0
cleanup() {
  if [[ "$MINIO_STARTED" == 1 ]]; then
    docker stop --timeout 60 "$MINIO" > /dev/null || true
    docker rm "$MINIO" > /dev/null 2>&1 || true
  fi
  docker network rm "$NETWORK" > /dev/null 2>&1 || true
}
trap cleanup EXIT
docker run --rm --cpus=8 --memory=8g \
  -v "$ROOT:/src:ro" -v "$MODULE_CACHE:/go/pkg/mod:ro" \
  -v graphdb-local-disk-bench-gocache:/root/.cache/go-build -v "$VOLUME:/validation" \
  -w /src -e GOTOOLCHAIN=local "$GO_IMAGE" bash -c \
  'mkdir -p /validation/bin && go build -mod=readonly -o /validation/bin/graphdb-new ./cmd/graphdb && go build -mod=readonly -o /validation/bin/loadtest ./tools/loadtest && cd "$1" && go build -mod=readonly -o /validation/bin/graphdb-main ./cmd/graphdb' \
  bash "/src/capacity-runs/$RUN_ID/baseline"
if [[ "$WITH_S3" == 1 ]]; then
  docker create --name "graphdb-$RUN_ID-mc" "$MC_IMAGE" > /dev/null
  docker cp "graphdb-$RUN_ID-mc:/usr/bin/mc" "$OUTPUT/mc"
  docker rm "graphdb-$RUN_ID-mc" > /dev/null
  docker run --rm -v "$OUTPUT:/evidence:ro" -v "$VOLUME:/validation" "$GO_IMAGE" cp /evidence/mc /validation/bin/mc
  docker run -d --name "$MINIO" --network "$NETWORK" --cpus=2 --memory=2g \
    -v "$VOLUME:/validation" -e MINIO_ROOT_USER=graphdbbench \
    -e MINIO_ROOT_PASSWORD=graphdbbench-local-only "$MINIO_IMAGE" server /validation/minio > /dev/null
  MINIO_STARTED=1
fi
set +e
docker run --rm --name "graphdb-$RUN_ID-run" --network "$NETWORK" --cpus=8 --memory=8g \
  -v "$ROOT:/src:ro" -v "$VOLUME:/validation" -w /src "$GO_IMAGE" \
  python3 scripts/local_disk_benchmark.py --output "/validation/$RUN_ID" \
  --s3-endpoint "http://$MINIO:9000" "$@" 2>&1 | tee "$OUTPUT/progress.log"
RESULT="${PIPESTATUS[0]}"
set -e
docker run --rm -i -v "$VOLUME:/validation:ro" -v "$OUTPUT:/evidence" "$GO_IMAGE" \
  python3 - "$RUN_ID" <<'PY'
from pathlib import Path
import shutil,sys
root=Path('/validation')/sys.argv[1]
for path in root.rglob('*'):
 if path.is_file() and path.suffix in ('.json','.log') and 'data' not in path.relative_to(root).parts:
  dest=Path('/evidence/reports')/path.relative_to(root)
  dest.parent.mkdir(parents=True,exist_ok=True)
  shutil.copy2(path,dest)
PY
python3 scripts/local_disk_report.py "$OUTPUT"
printf 'Evidence: %s; retained Linux volume: %s\n' "$OUTPUT" "$VOLUME"
exit "$RESULT"
