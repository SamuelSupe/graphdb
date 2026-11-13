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
UPGRADE_IMAGE = os.environ.get("GRAPHDB_GATE_UPGRADE_FROM_IMAGE")
SOURCE_HOME_PROTOCOL = 11 if UPGRADE_IMAGE else HOME_PROTOCOL
if UPGRADE_IMAGE and (HOME_PROTOCOL, DATA_PROTOCOL) != (12, 11):
    raise ValueError("protocol 11 upgrade requires home protocol 12 and data protocol 11")
RECEIPT_RETENTION = os.environ.get("GRAPHDB_GATE_RECEIPT_RETENTION")
if RECEIPT_RETENTION and HOME_PROTOCOL < 7:
    raise ValueError("receipt expiry requires home protocol 7")
PROJECT = "graphdb-partition-wal-" + secrets.token_hex(4)
SOURCE_PROJECT = PROJECT
TOKEN = secrets.token_hex(32)
OFFSET = int(os.environ.get("GRAPHDB_GATE_PORT_OFFSET", "-20000"))
ENV = {**os.environ, "GRAPHDB_HA_IMAGE": UPGRADE_IMAGE or IMAGE, "GRAPHDB_RAFT_TOKEN": TOKEN, "GRAPHDB_INGEST_MODE": "wal"}
PORTS = {"catalog": 47100, "a": 47200, "b": 47300}
NODES = [group + str(i) for group in PORTS for i in range(1, 4)]
HOME_NODES = ["a1", "a2", "a3"]
services = {}
for group, port in PORTS.items():
    for i in range(1, 4):
        services[group + str(i)] = {"ports": [f"127.0.0.1:{port + i + OFFSET}:8080"], "environment": {
            "GRAPHDB_RAFT_PROTOCOL_VERSION": str(SOURCE_HOME_PROTOCOL if group == "a" else DATA_PROTOCOL if group == "b" else 5),
            "GRAPHDB_RAFT_SNAPSHOT_ENTRIES": "1000",
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
OVERRIDE_PATH = OUT / "override.json"
OVERRIDE_PATH.write_text(json.dumps(override))
COMPOSE_FILES = ["-f", str(ROOT / "docker-compose.sharded.yml"), "-f", str(OVERRIDE_PATH)]
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


def accept(body, router=0):
    # Preserve the complete request on a transient transport/leader failure.
    # A write_outcome_unknown response is safe to probe with this identity.
    path = "/v1/ingest/batches"
    deadline = time.monotonic() + 25
    while True:
        try:
            code, value = request("POST", path, body, router=router)
        except OSError:
            if time.monotonic() >= deadline:
                raise
        else:
            if code == 202:
                return value
            assert code in (502, 503, 504) and time.monotonic() < deadline, (path, code, value)
        time.sleep(0.1)


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


def node_url(name):
    return f"http://127.0.0.1:{PORTS[name[:-1]] + int(name[-1]) + OFFSET}"


def raft_status(name):
    return expect("GET", "/v1/health", base=node_url(name))["raft"]


def leader(group):
    def find():
        for i in range(1, 4):
            try:
                if raft_status(group + str(i)).get("leader_id") == i:
                    return i
            except (OSError, AssertionError):
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


def wait_replica(name, protocol, applied=0):
    def caught_up():
        status = raft_status(name)
        if (status["protocol_max"] >= HOME_PROTOCOL and status["protocol_version"] == protocol
                and status["applied_index"] >= applied and not status.get("error") and not status.get("snapshot_error")):
            return status
        return False
    return wait(caught_up)


def stop_legacy_followers():
    followers = {}
    for group in ("a", "b"):
        source_leader = leader(group)
        follower = next(group + str(i) for i in range(1, 4) if i != source_leader)
        status = raft_status(follower)
        assert status["protocol_version"] == status["protocol_max"] == 11, status
        followers[follower] = status
    compose("stop", *followers)
    return followers


def replace_protocol11_binaries(followers, seed):
    # Snapshotting is above this fixture's log length. The increase in applied
    # application entries proves the candidate replayed the seed's commands.
    committed = {name: raft_status(name[0] + str(leader(name[0]))) for name in followers}
    ENV["GRAPHDB_HA_IMAGE"] = IMAGE
    compose("up", "-d", "--no-build", "--no-deps", *followers)
    for name, before in followers.items():
        target = committed[name]
        assert target["protocol_version"] == target["protocol_max"] == 11, target
        assert target["commit_index"] > before["applied_index"], (before, target)
        after = wait_replica(name, 11, target["commit_index"])
        assert after["snapshot_index"] <= before["applied_index"] and after["application_entries"] > 0, (before, after)
        record("protocol 11 log replay by upgraded follower", replica=name, source_build=target["build"],
               target_build=after["build"], before=before["applied_index"], after=after["applied_index"],
               snapshot=after["snapshot_index"], replayed_application_entries=after["application_entries"])
    compose("up", "-d", "--no-build", *NODES, "router", "router2")
    for name in NODES:
        status = wait_replica(name, 5 if name.startswith("catalog") else 11)
        assert status["build"] == after["build"], status
    for router in range(2):
        status = wait(lambda: expect("GET", "/v1/health", router=router))
        assert status["build"] == after["build"], status
    graph = wait(lambda: expect("GET", "/v1/cluster/graphs/big"))
    assert (graph["version"], graph["data_hash"]) == (seed["version"], seed["data_hash"]), graph
    record("all binaries replaced at original protocols", replicas=len(NODES), routers=2, data_hash=graph["data_hash"])


def activate_protocol12(accepted, request_body):
    # Keep the participant group paused so a protocol-11 acceptance and its
    # in-flight transaction survive the maintenance-window restart.
    for name in HOME_NODES:
        wait_replica(name, 11, accepted["accepted_lsn"])
        services[name]["environment"]["GRAPHDB_RAFT_PROTOCOL_VERSION"] = "12"
    OVERRIDE_PATH.write_text(json.dumps(override))
    compose("stop", *HOME_NODES)
    compose("up", "-d", "--no-build", "--no-deps", *HOME_NODES)
    for name in HOME_NODES:
        wait_replica(name, 12, accepted["accepted_lsn"])
    leader("a")
    pending = wait(lambda: expect("GET", accepted["status_url"]))
    for key in ("accepted_lsn", "accepted_at"):
        assert pending[key] == accepted[key], pending
    assert pending["state"] == "accepted", pending
    replay = accept(request_body, router=1)
    assert replay["status_url"] == accepted["status_url"] and replay["accepted_lsn"] == accepted["accepted_lsn"], replay
    record("protocol 11 acceptance survived home upgrade to protocol 12",
           acceptance_lsn=accepted["accepted_lsn"], replicas=len(HOME_NODES))


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
    old_image = UPGRADE_IMAGE or os.environ.get("GRAPHDB_GATE_PREVIOUS_HOME_IMAGE") or os.environ.get("GRAPHDB_GATE_PROTOCOL5_IMAGE")
    if old_image:
        ENV["GRAPHDB_HA_IMAGE"] = old_image
        try:
            old_protocol = SOURCE_HOME_PROTOCOL if UPGRADE_IMAGE else 5
            result = compose("run", "--rm", "--no-deps", "-T", "-e", f"GRAPHDB_RAFT_PROTOCOL_VERSION={old_protocol}", "a1", "serve",
                             check=False, timeout=20)
        finally:
            ENV["GRAPHDB_HA_IMAGE"] = IMAGE
        assert result.returncode != 0 and f"Raft data requires protocol {HOME_PROTOCOL}" in result.stderr, result.stderr
        record("older binary rejected restored home state", required_protocol=HOME_PROTOCOL)
    if UPGRADE_IMAGE:
        # Let participant elections finish before the restored home runs its
        # pending batches and plans the protocol-12 handoff.
        compose("up", "-d", "--no-build", "b1", "b2", "b3")
        leader("b")
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
    if UPGRADE_IMAGE:
        followers = stop_legacy_followers()
    published_seed = expect("POST", "/v1/commits", seed)
    assert published_seed["version"] == 2, published_seed
    if UPGRADE_IMAGE:
        replace_protocol11_binaries(followers, published_seed)
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
    accepted = accept(first)
    assert accepted["durability"] == "raft_majority" and accepted["accepted_lsn"] > 0
    if RECEIPT_RETENTION:
        assert accepted["status_url"].endswith(f"?accepted_lsn={accepted['accepted_lsn']}"), accepted
    wait(lambda: expect("GET", "/v1/cluster/graphs/big").get("active"))
    if UPGRADE_IMAGE:
        activate_protocol12(accepted, first)
    second_acceptance = accept(second)
    assert second_acceptance["durability"] == "raft_majority" and second_acceptance["accepted_lsn"] > accepted["accepted_lsn"]
    receipts = [accepted, second_acceptance]
    if UPGRADE_IMAGE:
        # Resuming the active first transaction uses its original plan.
        # Queue two more so a new execution turn must use the handoff path.
        third = {**second, "batch_id": "three", "expected_version": 4,
                 "items": [{"entity": {"id": "durable-third", "kind": "host"}}]}
        receipt = accept(third)
        assert receipt["durability"] == "raft_majority" and receipt["accepted_lsn"] > second_acceptance["accepted_lsn"]
        receipts.append(receipt)
    assert expect("GET", accepted["status_url"])["state"] == "accepted"
    record("202 with unavailable data group", acceptance=accepted, second_acceptance=second_acceptance, home_protocol=HOME_PROTOCOL)
    old = leader("a")
    kill("a" + str(old))
    new = leader("a")
    assert new != old
    replay = accept(first, router=1)
    assert replay["accepted_lsn"] == accepted["accepted_lsn"] and replay["accepted_at"] == accepted["accepted_at"]
    record("SIGKILL home leader before publication", old_leader=old, new_leader=new)
    # Kill the remaining home processes and both routers. No graceful close or
    # shutdown flush is available to preserve the acknowledged requests.
    kill(*(name for name in HOME_NODES if name != "a" + str(old)), "router", "router2")
    command("docker", "start", *(container(name) for name in (*HOME_NODES, "router", "router2")))
    wait(lambda: expect("GET", accepted["status_url"], router=1).get("state") == "accepted")
    alias = {**first, "batch_id": "new-batch-id"}
    replay = accept(alias, router=1)
    assert replay["status_url"] == accepted["status_url"] and replay["accepted_lsn"] == accepted["accepted_lsn"]
    record("all home replicas and routers SIGKILL", acceptance_lsn=replay["accepted_lsn"])
    restore_offline()
    for i, receipt in enumerate(receipts):
        terminal = wait(lambda: (value if value.get("result") else None) if (value := expect("GET", receipt["status_url"], router=1)) else None)
        assert terminal["state"] == "committed" and terminal["result"]["version"] == i + 3 and terminal["result"]["applied"] == 1, terminal
        assert terminal["accepted_lsn"] == receipt["accepted_lsn"] and terminal["accepted_at"] == receipt["accepted_at"], terminal
        if RECEIPT_RETENTION:
            assert terminal.get("receipt_expires_at"), terminal
    if UPGRADE_IMAGE:
        prefix = 'graphdb_ha_operation_seconds_count{operation="partition_ingest_handoff",status="ok"} '
        count = sum(int(line.removeprefix(prefix)) for name in HOME_NODES
                    for line in expect("GET", "/metrics", base=node_url(name)).splitlines() if line.startswith(prefix))
        assert count > 0, "the restored queue did not exercise the protocol-12 handoff"
        record("protocol 12 handoff completed restored acceptances", handoffs=count)
    result = expect("POST", "/v1/ingest/batches", first, router=1, headers={"Prefer": "wait=committed"})
    assert result["version"] == 3
    if RECEIPT_RETENTION:
        replay = expect("POST", "/v1/ingest/batches", alias, router=1, headers={"Prefer": "wait=committed"})
        assert replay["version"] == 3, replay
    replay_seed = expect("POST", "/v1/commits", seed)
    assert replay_seed["idempotent_replay"] and replay_seed["data_hash"] == published_seed["data_hash"], replay_seed
    expect("POST", "/v1/ingest/batches", {**first, "cursor": "conflicting-payload"}, status=409)
    descriptor = expect("GET", "/v1/cluster/graphs/big")
    final_version = 2 + len(receipts)
    assert descriptor["version"] == final_version and descriptor["entity_count"] == final_version and descriptor["edge_count"] == 1, descriptor
    for start, end, direction in ((left, right, "out"), (right, left, "in")):
        neighbors = expect("POST", "/v1/query", {"op": "neighbors", "id": start, "direction": direction, "limit": 10})
        assert neighbors["version"] == final_version and len(neighbors["results"]) == 1 and neighbors["results"][0]["entity"]["id"] == end, neighbors
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
                         "base_version": final_version, "at": accepted["accepted_at"], "digest": hashlib.sha256(payload).hexdigest(),
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
        reused = {**first, "expected_version": final_version, "preconditions": [],
                  "items": [{"entity": {"id": first_id, "kind": "host", "fields": {"payload": "reused-after-expiry"}}}]}
        replacement = accept(reused)
        assert replacement["accepted_lsn"] > accepted["accepted_lsn"] and replacement["status_url"] != accepted["status_url"], replacement
        expect("GET", accepted["status_url"], status=404)
        replay = expect("POST", "/v1/ingest/batches", {**reused, "batch_id": "reused-alias"}, router=1, headers={"Prefer": "wait=committed"})
        assert replay["version"] == final_version + 1, replay
        accepted_lsns = []
        for i in range(72):
            batch = {"source": "agent", "collector_id": "collector", "batch_id": f"expiry-{i}",
                     "idempotency_key": f"expiry-{i}", "items": reused["items"]}
            latest = accept(batch)
            accepted_lsns.append(latest["accepted_lsn"])
        assert sorted(set(accepted_lsns)) == accepted_lsns and accepted_lsns[0] > replacement["accepted_lsn"], accepted_lsns
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
        assert expect("GET", "/v1/cluster/graphs/big")["version"] == final_version + 1
        record("terminal receipts and aliases expired after reuse and sustained intake", additional_batches=72,
               replicas_checked=3, remaining_receipt_and_alias_files=0, version=final_version + 1)
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
