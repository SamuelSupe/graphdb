"""Exercise partitioned WAL crash recovery and offline restore on Linux.

Set GRAPHDB_GATE_IMAGE to a locally built candidate; this never publishes images
or releases. All containers and volumes belong to a fresh, private project.
"""

import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.environ["GRAPHDB_GATE_OUTPUT"]).resolve()
OUT.mkdir(mode=0o700, parents=True, exist_ok=False)
IMAGE = os.environ["GRAPHDB_GATE_IMAGE"]
HOME_PROTOCOL = int(os.environ.get("GRAPHDB_GATE_HOME_PROTOCOL", "7"))
DATA_PROTOCOL = int(os.environ.get("GRAPHDB_GATE_DATA_PROTOCOL", "5"))
RECEIPT_RETENTION = os.environ.get("GRAPHDB_GATE_RECEIPT_RETENTION")
if RECEIPT_RETENTION and HOME_PROTOCOL < 7:
    raise ValueError("receipt expiry requires home protocol 7")
PROJECT = "graphdb-partition-wal-" + secrets.token_hex(4)
SOURCE_PROJECT = PROJECT
TOKEN = secrets.token_hex(32)
OFFSET = int(os.environ.get("GRAPHDB_GATE_PORT_OFFSET", "-20000"))
ENV = {**os.environ, "GRAPHDB_HA_IMAGE": IMAGE, "GRAPHDB_RAFT_TOKEN": TOKEN, "GRAPHDB_INGEST_MODE": "wal"}
NODES = [group + str(i) for group in ("catalog", "a", "b") for i in range(1, 4)]
services = {}
for group, port in (("catalog", 47100), ("a", 47200), ("b", 47300)):
    for i in range(1, 4):
        services[group + str(i)] = {"ports": [f"127.0.0.1:{port + i + OFFSET}:8080"], "environment": {
            "GRAPHDB_RAFT_PROTOCOL_VERSION": str(HOME_PROTOCOL if group == "a" else DATA_PROTOCOL if group == "b" else 5), "GRAPHDB_RAFT_SNAPSHOT_ENTRIES": "1000",
            "GRAPHDB_RAFT_ELECTION_TICKS": "10", "GRAPHDB_INGEST_FLUSH_INTERVAL": "1h",
            "GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES": "1048576"}}
        if group == "a":
            services[group + str(i)]["ports"].append(f"127.0.0.1:{47400 + i + OFFSET}:8081")
            if RECEIPT_RETENTION:
                services[group + str(i)]["environment"]["GRAPHDB_PARTITION_INGEST_RECEIPT_RETENTION"] = RECEIPT_RETENTION
for i, name in enumerate(("router", "router2")):
    services[name] = {"ports": [f"127.0.0.1:{47083 + i + OFFSET}:8080"]}
override = {"services": services}
if os.environ.get("GRAPHDB_GATE_SUBNET"):
    override["networks"] = {"default": {"ipam": {"config": [{"subnet": os.environ["GRAPHDB_GATE_SUBNET"]}]}}}
(OUT / "override.json").write_text(json.dumps(override))
COMPOSE_FILES = ["-f", str(ROOT / "docker-compose.sharded.yml"), "-f", str(OUT / "override.json")]
events = []


def command(*args, check=True, timeout=None):
    result = subprocess.run(args, env=ENV, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


def compose(*args, check=True, timeout=None):
    return command("docker", "compose", "-p", PROJECT, *COMPOSE_FILES, *args, check=check, timeout=timeout)


def container(name):
    return PROJECT + "-" + name + "-1"


def request(method, path, body=None, router=0, base=None, headers=None):
    base = base or f"http://127.0.0.1:{47083 + router + OFFSET}"
    hdr = {"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json", "X-Tenant-ID": "big", **(headers or {})}
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=hdr)
    try:
        response = urllib.request.urlopen(req, timeout=12)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        try:
            return response.code, json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return response.code, raw.decode(errors="replace")


def expect(method, path, body=None, status=200, **kwargs):
    code, value = request(method, path, body, **kwargs)
    assert code == status, (method, path, code, value)
    return value


def wait(fn, seconds=60):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            if last:
                return last
        except (OSError, TimeoutError, AssertionError) as error:
            last = str(error)
        time.sleep(0.1)
    raise AssertionError(("condition did not complete", last))


def leader(group):
    port = {"a": 47200, "b": 47300}[group] + OFFSET
    def find():
        for i in range(1, 4):
            try:
                code, health = request("GET", "/v1/health", base=f"http://127.0.0.1:{port + i}")
                if code == 200 and health.get("raft", {}).get("leader_id") == i:
                    return i
            except OSError:
                pass
        return False
    return wait(find)


def kill(*names):
    command("docker", "update", "--restart=no", *(container(name) for name in names))
    command("docker", "kill", "--signal=KILL", *(container(name) for name in names))
    for name in names:
        state = json.loads(command("docker", "inspect", "--format", "{{json .State}}", container(name)).stdout)
        assert state["ExitCode"] == 137, (name, state)


def record(name, **details):
    events.append({"scenario": name, "result": "PASS", **details})
    (OUT / "results.json").write_text(json.dumps(events, indent=2) + "\n")


def entity_id(name, group):
    while hashlib.sha256(name.encode()).digest()[0] % 2 != group:
        name += "x"
    return name


def restore_offline():
    global PROJECT
    # Stop the home group before resuming data replicas so the pending
    # acceptances are captured without a later graph decision.
    compose("stop", "router", "router2", "a1", "a2", "a3")
    command("docker", "unpause", *(container("b" + str(i)) for i in range(1, 4)))
    compose("stop", "catalog1", "catalog2", "catalog3", "b1", "b2", "b3")
    archives = OUT / "archives"
    archives.mkdir(mode=0o777)
    archives.chmod(0o777)
    mount = str(archives) + ":/archives"
    for name in NODES:
        compose("run", "--rm", "--no-deps", "-T", "-v", mount, name, "runtime-backup", f"/archives/{name}.runtime")
    (OUT / "source-containers.log").write_text(compose("logs", "--no-color").stdout)
    compose("down")
    PROJECT = SOURCE_PROJECT + "-restored"
    for name in NODES:
        compose("run", "--rm", "--no-deps", "-T", "-v", mount + ":ro", name, "runtime-restore",
                f"/archives/{name}.runtime", "--isolated-cluster-recovery")
    old_image = os.environ.get("GRAPHDB_GATE_PREVIOUS_HOME_IMAGE") or os.environ.get("GRAPHDB_GATE_PROTOCOL5_IMAGE")
    if old_image:
        ENV["GRAPHDB_HA_IMAGE"] = old_image
        try:
            result = compose("run", "--rm", "--no-deps", "-T", "-e", "GRAPHDB_RAFT_PROTOCOL_VERSION=5", "a1", "serve",
                             check=False, timeout=20)
        finally:
            ENV["GRAPHDB_HA_IMAGE"] = IMAGE
        assert result.returncode != 0 and f"Raft data requires protocol {HOME_PROTOCOL}" in result.stderr, result.stderr
        record("older binary rejected restored home state", required_protocol=HOME_PROTOCOL)
    compose("up", "-d", "--no-build", *NODES, "router", "router2")


try:
    compose("up", "-d", "--no-build", *NODES, "router", "router2")
    wait(lambda: request("GET", "/v1/cluster")[0] == 200)
    leader("a")
    leader("b")
    for group in ("a", "b"):
        expect("POST", "/v1/cluster/shards", {"id": "shard-" + group, "cluster_id": "graphdb-shard-" + group,
               "peers": {str(i): f"http://{group}{i}:8081" for i in range(1, 4)}}, status=202)
    expect("POST", "/v1/tenants", {"tenant_id": "big"})
    expect("POST", "/v1/cluster/graphs", {"tenant_id": "big", "data_groups": ["shard-a", "shard-b"]}, status=202)
    wait(lambda: value.get("format") == 1 and value.get("state") == "active" if (value := expect("GET", "/v1/cluster/graphs/big")) else False)
    expect("POST", "/v1/commits", {"idempotency_key": "schema", "mutations": {"upsert_ci_types": [{"name": "host"}],
           "upsert_relation_types": [{"name": "calls", "from_kind": "host", "to_kind": "host", "directed": True}]}})
    left, right = entity_id("seed-left", 0), entity_id("seed-right", 1)
    seed = {"idempotency_key": "seed", "mutations": {
        "upsert_entities": [{"id": left, "kind": "host"}, {"id": right, "kind": "host"}],
        "upsert_edges": [{"from": left, "to": right, "type": "calls"}]}}
    published_seed = expect("POST", "/v1/commits", seed)
    assert published_seed["version"] == 2, published_seed
    orphan_bytes = 0
    if HOME_PROTOCOL >= 7:
        payload = secrets.token_bytes(32768)
        home_url = f"http://127.0.0.1:{47400 + leader('a') + OFFSET}"
        for i in range(12):
            expect("POST", "/cluster/graph/action", {"operation": "stage_input", "tenant_id": "big", "generation": 2,
                   "txn": f"{i + 1:032x}", "data": base64.b64encode(payload).decode(), "digest": hashlib.sha256(payload).hexdigest()},
                   base=home_url, headers={"X-Raft-Cluster": "graphdb-shard-a"})
            orphan_bytes += len(payload)
    first_id = entity_id("durable-first", 1)
    first = {"source": "agent", "collector_id": "collector", "batch_id": "one", "idempotency_key": "one", "expected_version": 2,
             "preconditions": [{"resource_type": "entity", "id": first_id, "op": "not_exists"}],
             "items": [{"entity": {"id": first_id, "kind": "host", "fields": {"payload": "survives-sigkill"}}}]}
    second = {"source": "agent", "collector_id": "collector", "batch_id": "two", "expected_version": 3,
              "items": [{"entity": {"id": "durable-second", "kind": "host"}}]}
    command("docker", "pause", *(container("b" + str(i)) for i in range(1, 4)))
    accepted = expect("POST", "/v1/ingest/batches", first, status=202)
    assert accepted["durability"] == "raft_majority" and accepted["accepted_lsn"] > 0
    if RECEIPT_RETENTION:
        assert accepted["status_url"].endswith(f"?accepted_lsn={accepted['accepted_lsn']}"), accepted
    expect("POST", "/v1/ingest/batches", second, status=202)
    wait(lambda: expect("GET", "/v1/cluster/graphs/big").get("active"))
    assert expect("GET", accepted["status_url"])["state"] == "accepted"
    record("202 with unavailable data group", acceptance=accepted)
    old = leader("a")
    kill("a" + str(old))
    new = leader("a")
    assert new != old
    replay = expect("POST", "/v1/ingest/batches", first, status=202, router=1)
    assert replay["accepted_lsn"] == accepted["accepted_lsn"] and replay["accepted_at"] == accepted["accepted_at"]
    record("SIGKILL home leader before publication", old_leader=old, new_leader=new)
    # Kill the remaining home processes and both routers. No graceful close or
    # shutdown flush is available to preserve the acknowledged requests.
    kill(*(name for name in ("a1", "a2", "a3") if name != "a" + str(old)), "router", "router2")
    command("docker", "start", *(container(name) for name in ("a1", "a2", "a3", "router", "router2")))
    wait(lambda: expect("GET", accepted["status_url"], router=1).get("state") == "accepted")
    alias = {**first, "batch_id": "new-batch-id"}
    replay = expect("POST", "/v1/ingest/batches", alias, status=202, router=1)
    assert replay["status_url"] == accepted["status_url"] and replay["accepted_lsn"] == accepted["accepted_lsn"]
    record("all home replicas and routers SIGKILL", acceptance_lsn=replay["accepted_lsn"])
    restore_offline()
    for i, url in enumerate((accepted["status_url"], "/v1/ingest/batches/agent/collector/two")):
        terminal = wait(lambda: (value if value.get("result") else None) if (value := expect("GET", url, router=1)) else None)
        assert terminal["state"] == "committed" and terminal["result"]["version"] == i + 3 and terminal["result"]["applied"] == 1, terminal
        if i == 0:
            assert terminal["accepted_lsn"] == accepted["accepted_lsn"] and terminal["accepted_at"] == accepted["accepted_at"], terminal
        if RECEIPT_RETENTION:
            assert terminal.get("receipt_expires_at"), terminal
    result = expect("POST", "/v1/ingest/batches", first, router=1, headers={"Prefer": "wait=committed"})
    assert result["version"] == 3
    if RECEIPT_RETENTION:
        replay = expect("POST", "/v1/ingest/batches", alias, router=1, headers={"Prefer": "wait=committed"})
        assert replay["version"] == 3, replay
    replay_seed = expect("POST", "/v1/commits", seed)
    assert replay_seed["idempotent_replay"] and replay_seed["data_hash"] == published_seed["data_hash"], replay_seed
    expect("POST", "/v1/ingest/batches", {**first, "cursor": "conflicting-payload"}, status=409)
    descriptor = expect("GET", "/v1/cluster/graphs/big")
    assert descriptor["version"] == 4 and descriptor["entity_count"] == 4 and descriptor["edge_count"] == 1, descriptor
    for start, end, direction in ((left, right, "out"), (right, left, "in")):
        neighbors = expect("POST", "/v1/query", {"op": "neighbors", "id": start, "direction": direction, "limit": 10})
        assert neighbors["version"] == 4 and len(neighbors["results"]) == 1 and neighbors["results"][0]["entity"]["id"] == end, neighbors
    entity = expect("GET", "/v1/entities/" + first_id)
    assert entity["entity"]["fields"]["payload"] == "survives-sigkill", entity
    record("offline restore to fresh volumes and exactly-once recovery", archives=len(NODES),
           version=descriptor["version"], data_hash=descriptor["data_hash"])
    if HOME_PROTOCOL >= 7:
        wait(lambda: expect("GET", "/v1/cluster/graphs/big").get("active") is None)
        expect("POST", "/v1/cluster/graphs/big/gc", {}, status=202)
        wait(lambda: expect("GET", "/v1/cluster/graphs/big/gc") is None, seconds=120)
        input_dir = "/var/lib/graphdb/graphdb/control/graphs/big/2/inputs"
        wait(lambda: all(command("docker", "exec", container("a" + str(i)), "test", "!", "-d", input_dir, check=False).returncode == 0 for i in range(1, 4)))
        home_url = f"http://127.0.0.1:{47400 + leader('a') + OFFSET}"
        expired = expect("POST", "/cluster/graph/action", {"operation": "begin", "tenant_id": "big", "generation": 2,
                         "txn": f"{1:032x}", "identity": "direct:expired-stage-probe", "purpose": "commit",
                         "base_version": 4, "at": accepted["accepted_at"], "digest": hashlib.sha256(payload).hexdigest(),
                         "inputs": [{"stage": f"{1:032x}", "digest": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}]},
                         status=409, base=home_url, headers={"X-Raft-Cluster": "graphdb-shard-a"})
        assert expired["code"] == "graph_input_expired" and expired["retryable"], expired
        if not RECEIPT_RETENTION:
            replay = expect("POST", "/v1/ingest/batches", alias, router=1, headers={"Prefer": "wait=committed"})
            assert replay["version"] == 3, replay
        assert expect("GET", "/v1/cluster/graphs/big")["data_hash"] == descriptor["data_hash"]
        record("GC reclaimed staged inputs after crash and restore", orphan_chunks=12, reclaimed_bytes=orphan_bytes,
               input_directory_removed_on_replicas=3)
    if RECEIPT_RETENTION:
        wait(lambda: request("GET", accepted["status_url"])[0] == 404)
        reused = {**first, "expected_version": 4, "preconditions": [],
                  "items": [{"entity": {"id": first_id, "kind": "host", "fields": {"payload": "reused-after-expiry"}}}]}
        replacement = expect("POST", "/v1/ingest/batches", reused, status=202)
        assert replacement["accepted_lsn"] > accepted["accepted_lsn"] and replacement["status_url"] != accepted["status_url"], replacement
        expect("GET", accepted["status_url"], status=404)
        replay = expect("POST", "/v1/ingest/batches", {**reused, "batch_id": "reused-alias"}, router=1, headers={"Prefer": "wait=committed"})
        assert replay["version"] == 5, replay
        latest = replacement
        for i in range(72):
            batch = {"source": "agent", "collector_id": "collector", "batch_id": f"expiry-{i}",
                     "idempotency_key": f"expiry-{i}", "items": reused["items"]}
            latest = expect("POST", "/v1/ingest/batches", batch, status=202)
        def latest_terminal():
            code, status = request("GET", latest["status_url"])
            return code == 200 and status.get("result") is not None
        wait(latest_terminal)
        control = "/var/lib/graphdb/graphdb/control"
        def retained_files():
            for i in range(1, 4):
                result = command("docker", "exec", container("a" + str(i)), "sh", "-c",
                                 f"for p in {control}/partition-ingest-receipts {control}/partition-ingest-identities; do if [ -d \"$p\" ]; then find \"$p\" -type f; fi; done")
                if result.stdout.strip():
                    return True
            return False
        wait(lambda: not retained_files(), seconds=90)
        expect("GET", latest["status_url"], status=404)
        assert expect("GET", "/v1/cluster/graphs/big")["version"] == 5
        record("terminal receipts and aliases expired after reuse and sustained intake", additional_batches=72,
               replicas_checked=3, remaining_receipt_and_alias_files=0, version=5)
finally:
    logs = compose("logs", "--no-color", check=False)
    (OUT / "containers.log").write_text(logs.stdout + logs.stderr)
    for i in range(1, 4):
        command("docker", "unpause", container("b" + str(i)), check=False)
    compose("down", "-v", "--remove-orphans", check=False)
    if PROJECT != SOURCE_PROJECT:
        PROJECT = SOURCE_PROJECT
        compose("down", "-v", "--remove-orphans", check=False)

print("Partitioned WAL crash and offline recovery gate passed. Evidence:", OUT)
