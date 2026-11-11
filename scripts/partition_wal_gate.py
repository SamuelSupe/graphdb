"""Exercise partitioned WAL recovery using isolated Compose replicas on Linux.

Set GRAPHDB_GATE_IMAGE to a locally built candidate; this never publishes images
or releases. All containers and volumes belong to a fresh, private project.
"""

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
OUT.mkdir(parents=True, exist_ok=False)
IMAGE = os.environ["GRAPHDB_GATE_IMAGE"]
PROJECT = "graphdb-partition-wal-" + secrets.token_hex(4)
TOKEN = secrets.token_hex(32)
OFFSET = int(os.environ.get("GRAPHDB_GATE_PORT_OFFSET", "-20000"))
ENV = {**os.environ, "GRAPHDB_HA_IMAGE": IMAGE, "GRAPHDB_RAFT_TOKEN": TOKEN, "GRAPHDB_INGEST_MODE": "wal"}
NODES = [group + str(i) for group in ("catalog", "a", "b") for i in range(1, 4)]
services = {}
for group, port in (("catalog", 47100), ("a", 47200), ("b", 47300)):
    for i in range(1, 4):
        services[group + str(i)] = {"ports": [f"127.0.0.1:{port + i + OFFSET}:8080"], "environment": {
            "GRAPHDB_RAFT_PROTOCOL_VERSION": "5", "GRAPHDB_RAFT_SNAPSHOT_ENTRIES": "1000",
            "GRAPHDB_RAFT_ELECTION_TICKS": "10", "GRAPHDB_INGEST_FLUSH_INTERVAL": "1h",
            "GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES": "1048576"}}
for i, name in enumerate(("router", "router2")):
    services[name] = {"ports": [f"127.0.0.1:{47083 + i + OFFSET}:8080"]}
override = {"services": services}
if os.environ.get("GRAPHDB_GATE_SUBNET"):
    override["networks"] = {"default": {"ipam": {"config": [{"subnet": os.environ["GRAPHDB_GATE_SUBNET"]}]}}}
(OUT / "override.json").write_text(json.dumps(override))
COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", str(ROOT / "docker-compose.sharded.yml"), "-f", str(OUT / "override.json")]
events = []


def command(*args, check=True):
    result = subprocess.run(args, env=ENV, capture_output=True, text=True)
    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


def compose(*args, check=True):
    return command(*COMPOSE, *args, check=check)


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
        return response.code, json.loads(raw or b"{}")


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
    expect("POST", "/v1/commits", {"idempotency_key": "schema", "mutations": {"upsert_ci_types": [{"name": "host"}]}})
    first_id = "durable-first"
    while hashlib.sha256(first_id.encode()).digest()[0] % 2 != 1:
        first_id += "x"
    first = {"source": "agent", "collector_id": "collector", "batch_id": "one", "idempotency_key": "one", "expected_version": 1,
             "preconditions": [{"resource_type": "entity", "id": first_id, "op": "not_exists"}],
             "items": [{"entity": {"id": first_id, "kind": "host", "fields": {"payload": "survives-sigkill"}}}]}
    second = {"source": "agent", "collector_id": "collector", "batch_id": "two", "expected_version": 2,
              "items": [{"entity": {"id": "durable-second", "kind": "host"}}]}
    command("docker", "pause", *(container("b" + str(i)) for i in range(1, 4)))
    accepted = expect("POST", "/v1/ingest/batches", first, status=202)
    assert accepted["durability"] == "raft_majority" and accepted["accepted_lsn"] > 0
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
    command("docker", "unpause", *(container("b" + str(i)) for i in range(1, 4)))
    for i, url in enumerate((accepted["status_url"], "/v1/ingest/batches/agent/collector/two")):
        terminal = wait(lambda: (value if value.get("result") else None) if (value := expect("GET", url, router=1)) else None)
        assert terminal["state"] == "committed" and terminal["result"]["version"] == i + 2 and terminal["result"]["applied"] == 1, terminal
    result = expect("POST", "/v1/ingest/batches", first, router=1, headers={"Prefer": "wait=committed"})
    assert result["version"] == 2
    expect("POST", "/v1/ingest/batches", {**first, "cursor": "conflicting-payload"}, status=409)
    descriptor = expect("GET", "/v1/cluster/graphs/big")
    assert descriptor["version"] == 3 and descriptor["entity_count"] == 2, descriptor
    entity = expect("GET", "/v1/entities/" + first_id)
    assert entity["entity"]["fields"]["payload"] == "survives-sigkill", entity
    record("ordered recovery and exactly-once retry", version=descriptor["version"], data_hash=descriptor["data_hash"])
finally:
    logs = compose("logs", "--no-color", check=False)
    (OUT / "containers.log").write_text(logs.stdout + logs.stderr)
    for i in range(1, 4):
        command("docker", "unpause", container("b" + str(i)), check=False)
    compose("down", "-v", "--remove-orphans", check=False)

print("Partitioned WAL crash gate passed. Evidence:", OUT)
