import json
import hashlib
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[1])
OUT = Path(os.environ['GRAPHDB_GATE_OUTPUT']) / 'dual'
OUT.mkdir(parents=True, exist_ok=False)
DOCKER = ['docker']
if os.environ.get('DOCKER_CONTEXT'):
    DOCKER += ['--context', os.environ['DOCKER_CONTEXT']]
IMAGE = os.environ['GRAPHDB_GATE_IMAGE']
PORT_OFFSET = int(os.environ.get('GRAPHDB_GATE_PORT_OFFSET', '0'))
PROJECT = os.environ['GRAPHDB_GATE_PROJECT'] + '-dual'
TOKEN = os.environ['GRAPHDB_RAFT_TOKEN']
COMPOSE = DOCKER + ['compose', '-p', PROJECT, '-f', ROOT+'/docker-compose.raft.yml', '-f', str(OUT/'override.yml')]
ENV = {**os.environ, 'GRAPHDB_HA_IMAGE': IMAGE, 'GRAPHDB_RAFT_TOKEN': TOKEN}
fixture = (Path(__file__).parent/'raft-gate'/'dual.yml').read_text()
(OUT/'override.yml').write_text(re.sub(r'(?<=127.0.0.1:)\d+', lambda match: str(int(match[0])+PORT_OFFSET), fixture))
TENANT = 'same-tenant'
results = []
request_retries = []

def docker(*args, check=True):
    return subprocess.run(DOCKER + list(args), check=check, text=True, capture_output=True)

def step(name, **details):
    row = {'check': name, 'result': 'PASS', **details}
    results.append(row)
    print(json.dumps(row), flush=True)
    (OUT/'results.json').write_text(json.dumps(results, indent=2)+'\n')

def request(base, method, path, body=None, headers=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base+path, method=method, data=data,
        headers={'Content-Type': 'application/json', 'X-Tenant-ID': TENANT, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read() or b'{}')
    except urllib.error.HTTPError as err:
        raw = err.read()
        try:
            value = json.loads(raw)
        except ValueError:
            value = {'error': raw.decode()}
        return err.code, value

def expect(base, method, path, body=None, status=200, headers=None):
    safe = status < 400 and (method == 'GET' or isinstance(body, dict) and body.get('idempotency_key'))
    deadline = time.monotonic()+25
    while True:
        try:
            code, value = request(base, method, path, body, headers)
            if code == status:
                return value
        except (OSError, TimeoutError) as err:
            if not safe:
                raise
            code, value = 0, str(err)
        assert safe and code in [0,502,503,504] and time.monotonic() < deadline, (base, method, path, code, value)
        # Fault injection can invalidate a health probe already in flight. Only
        # safe reads and the same durable write identity are retried here.
        request_retries.append({'base':base,'method':method,'path':path,'status':code,'error':value})
        (OUT/'request-retries.json').write_text(json.dumps(request_retries,indent=2)+'\n')
        time.sleep(.1)

def wait_ready(base):
    deadline = time.monotonic()+30
    while time.monotonic() < deadline:
        try:
            if request(base, 'GET', '/v1/readiness')[0] == 200:
                return
        except (OSError, TimeoutError):
            pass
        time.sleep(.1)
    raise AssertionError(('service not ready', base))

def node(i):
    return 'http://127.0.0.1:'+str(36080+i+PORT_OFFSET)

def leader(exclude=None):
    deadline = time.monotonic()+30
    while time.monotonic() < deadline:
        for i in range(1, 4):
            if i == exclude:
                continue
            try:
                status = request(node(i), 'GET', '/v1/health')[1].get('raft', {})
                if status.get('leader_id') == i and not status.get('draining') and request(node(i), 'GET', '/v1/readiness')[0] == 200:
                    return i
            except (OSError, TimeoutError):
                pass
        time.sleep(.1)
    raise AssertionError('no leader')

def batch(mode):
    return {'source':'agent', 'collector_id':'dual-mode', 'batch_id':'batch-'+mode,
        'idempotency_key':'batch-'+mode, 'items':[{'external_id':'host:2', 'entity':
        {'id':'host:2', 'kind':'host', 'fields':{'name':mode}}}]}

def wait_committed(base, mode):
    deadline = time.monotonic()+25
    while time.monotonic() < deadline:
        value = expect(base, 'GET', '/v1/ingest/batches/agent/dual-mode/batch-'+mode)
        if value['state'] == 'committed':
            return value
        assert value['state'] != 'failed', value
        time.sleep(.1)
    raise AssertionError(('batch did not commit', value))

try:
    subprocess.run(COMPOSE+['up','-d','--no-build'], env=ENV, check=True, capture_output=True, text=True)
    for mode, port in [('direct',46081),('wal',46082)]:
        port += PORT_OFFSET
        name = PROJECT+'-standalone-'+mode
        volume = name+'-data'
        docker('run','-d','--name',name,'--restart=no','-p',f'127.0.0.1:{port}:8080',
            '-v',volume+':/var/lib/graphdb','-e','GRAPHDB_INGEST_MODE='+mode,
            '-e','GRAPHDB_INGEST_FLUSH_INTERVAL=5s',IMAGE)
    direct = f'http://127.0.0.1:{46081+PORT_OFFSET}'
    wal = f'http://127.0.0.1:{46082+PORT_OFFSET}'
    raft = f'http://127.0.0.1:{46080+PORT_OFFSET}'
    for base, mode in [(direct,'direct'),(wal,'wal'),(raft,'raft')]:
        wait_ready(base)
        health = expect(base,'GET','/v1/health')
        if mode != 'raft':
            assert not health.get('raft'), health
        expect(base,'POST','/v1/tenants',{'tenant_id':TENANT})
        value = expect(base,'POST','/v1/commits',{'idempotency_key':'first',
            'mutations':{'upsert_entities':[{'id':'host:1','kind':'host','fields':{'name':mode}}]}})
        assert value['version'] == 1, value
        entity = expect(base,'GET','/v1/entities/host:1')
        assert entity['entity']['fields']['name'] == mode, entity
    ids = [docker('inspect','--format','{{.Image}}',name).stdout.strip() for name in
        [PROJECT+'-standalone-direct', PROJECT+'-standalone-wal', PROJECT+'-node1-1', PROJECT+'-node2-1', PROJECT+'-node3-1']]
    assert len(set(ids)) == 1, ids
    step('same image concurrently serves standalone direct, standalone WAL and three-replica Raft; tenant data stays independent')

    expect(direct,'POST','/v1/ingest/batches',batch('direct'))
    before = expect(direct,'GET','/v1/export/snapshot')
    docker('kill','--signal=KILL',PROJECT+'-standalone-direct')
    docker('start',PROJECT+'-standalone-direct')
    wait_ready(direct)
    assert before == expect(direct,'GET','/v1/export/snapshot')
    expect(direct,'POST','/v1/compact',{})
    expect(direct,'POST','/v1/control/gc',{'keep_snapshots':1})
    step('standalone direct commits and ingest survive SIGKILL; compact and GC remain available')

    accepted = expect(wal,'POST','/v1/ingest/batches',batch('wal'),202)
    assert accepted['durability'] == 'durable', accepted
    docker('kill','--signal=KILL',PROJECT+'-standalone-wal')
    docker('start',PROJECT+'-standalone-wal')
    wait_ready(wal)
    terminal = wait_committed(wal,'wal')
    assert terminal['result']['version'] == 2, terminal
    before_retry = expect(wal,'GET','/v1/export/snapshot')
    retry = expect(wal,'POST','/v1/ingest/batches',batch('wal'),202)
    assert retry['batch_id'] == accepted['batch_id'], (accepted,retry)
    assert wait_committed(wal,'wal')['result']['version'] == 2
    assert before_retry == expect(wal,'GET','/v1/export/snapshot')
    step('standalone WAL accepted response is durable; SIGKILL recovery publishes once and idempotent retry preserves identity')

    partition_headers = {'X-Tenant-ID': 'partition-test'}
    expect(raft, 'POST', '/v1/tenants', {'tenant_id': 'partition-test'})
    old = leader()
    isolated = PROJECT+f'-node{old}-1'
    network = PROJECT+'_raft'
    docker('network', 'disconnect', network, isolated)
    new = leader(exclude=old)
    wait_ready(raft)
    confirmed = {'idempotency_key': 'partition-write', 'mutations': {'upsert_entities': [
        {'id': 'host:partition', 'kind': 'host', 'fields': {'name': 'majority'}}]}}
    expect(node(new), 'POST', '/v1/commits', confirmed, headers=partition_headers)
    deadline = time.monotonic()+30
    while request(node(old), 'GET', '/v1/readiness')[0] != 503:
        assert time.monotonic() < deadline, 'isolated leader did not lose readiness'
        time.sleep(.1)
    expect(node(old), 'GET', '/v1/entities/host:partition', status=503, headers=partition_headers)
    expect(node(old), 'POST', '/v1/commits', confirmed, status=503, headers=partition_headers)
    docker('network', 'connect', '--alias', f'raft-node{old}', network, isolated)
    replay = expect(raft, 'POST', '/v1/commits', confirmed, headers=partition_headers)
    assert replay['idempotent_replay'] and replay['version'] == 1, replay
    step('live isolated leader rejects strong reads and writes; majority serves and confirmed write survives reconnection', old=old, new=new)

    old = leader()
    accepted = expect(raft,'POST','/v1/ingest/batches',batch('raft'),202)
    assert accepted['durability'] == 'raft_majority', accepted
    docker('update','--restart=no',PROJECT+f'-node{old}-1')
    start = time.monotonic()
    docker('kill','--signal=KILL',PROJECT+f'-node{old}-1')
    new = leader()
    wait_ready(raft)
    assert old != new
    terminal = wait_committed(raft,'raft')
    assert terminal['result']['version'] == 2, terminal
    expect(direct,'GET','/v1/entities/host:1')
    expect(wal,'GET','/v1/entities/host:2')
    docker('start',PROJECT+f'-node{old}-1')
    step('Raft majority WAL and leader takeover remain available alongside standalone services',old=old,new=new,seconds=round(time.monotonic()-start,3))

    docker('stop','--time=30',PROJECT+'-standalone-direct')
    cli = docker('run','--rm','--volumes-from',PROJECT+'-standalone-direct',IMAGE,'init-tenant','offline-tenant')
    assert cli.returncode == 0
    listed = docker('run','--rm','--volumes-from',PROJECT+'-standalone-direct',IMAGE,'list-tenants')
    assert 'offline-tenant' in listed.stdout, listed.stdout
    bad = docker('run','--rm','--volumes-from',PROJECT+'-standalone-direct',
        '-e','GRAPHDB_RAFT_NODE_ID=1','-e','GRAPHDB_RAFT_CLUSTER_ID=mode-check',
        '-e','GRAPHDB_RAFT_ADDR=:8081','-e','GRAPHDB_RAFT_TOKEN='+TOKEN,
        '-e','GRAPHDB_RAFT_PEERS={"1":"http://node1:8081","2":"http://node2:8081","3":"http://node3:8081"}',IMAGE,check=False)
    assert bad.returncode != 0 and 'empty data directory' in bad.stderr, (bad.returncode,bad.stderr)
    docker('start',PROJECT+'-standalone-direct')
    wait_ready(direct)
    expect(direct,'GET','/v1/tenants/offline-tenant')
    step('standalone offline CLI works; existing standalone directory refuses Raft bootstrap without losing standalone access')

    follower = next(i for i in range(1,4) if i != leader())
    replica = PROJECT+f'-node{follower}-1'
    docker('stop','--time=30',replica)
    bad = docker('run','--rm','--volumes-from',replica,IMAGE,check=False)
    assert bad.returncode != 0 and 'cannot be opened in standalone mode' in bad.stderr, (bad.returncode,bad.stderr)
    bad_cli = docker('run','--rm','--volumes-from',replica,IMAGE,'init-tenant','unsafe-offline',check=False)
    assert bad_cli.returncode != 0 and 'cannot be opened in standalone mode' in bad_cli.stderr
    docker('start',replica)
    wait_ready(raft)
    expect(raft,'GET','/v1/entities/host:2')
    step('Raft replica refuses standalone service and offline writes when Raft settings are omitted; configured restart works')

    large_headers = {'X-Tenant-ID': 'large-restore'}
    expect(raft, 'POST', '/v1/tenants', {'tenant_id': 'large-restore'})
    payload = 'x' * (4 << 20)
    for i in range(9):
        expect(raft, 'POST', '/v1/commits', {'mutations': {'upsert_entities': [
            {'id': f'host:{i}', 'kind': 'host', 'fields': {'payload': payload}}]}}, headers=large_headers)
    backup = expect(raft, 'POST', '/v1/tenants/large-restore/backup', {}, 202, large_headers)

    def completed_task(id):
        deadline = time.monotonic()+120
        while time.monotonic() < deadline:
            try:
                task = expect(raft, 'GET', '/v1/tasks/'+id, headers=large_headers)
                assert task['status'] not in ['failed', 'canceled'], task
                if task['status'] == 'succeeded':
                    return task
            except (OSError, TimeoutError):
                pass
            time.sleep(.1)
        raise AssertionError(('task did not complete', id))

    backup = completed_task(backup['id'])
    expect(raft, 'POST', '/v1/commits', {'mutations': {'delete_entities': ['host:0']}}, headers=large_headers)
    old = leader()
    task = expect(raft, 'POST', '/v1/tenants/large-restore/restore',
        {'backup_key': backup['result_key'], 'overwrite': True}, 202, large_headers)
    deadline = time.monotonic()+30
    while time.monotonic() < deadline:
        staged = docker('exec', PROJECT+f'-node{old}-1', 'sh', '-c',
            'find /var/lib/graphdb/graphdb/control/restore-staging -type f 2>/dev/null | head -n 1', check=False)
        if staged.stdout.strip():
            break
        time.sleep(.05)
    assert staged.stdout.strip(), 'restore never reached replicated staging'
    docker('update', '--restart=no', PROJECT+f'-node{old}-1')
    docker('kill', '--signal=KILL', PROJECT+f'-node{old}-1')
    new = leader(exclude=old)
    wait_ready(raft)
    completed_task(task['id'])
    entity = expect(raft, 'GET', '/v1/entities/host:0', headers=large_headers)
    assert hashlib.sha256(entity['entity']['fields']['payload'].encode()).digest() == hashlib.sha256(payload.encode()).digest()
    assert entity['version'] == 9, entity.keys()
    expect(raft, 'GET', '/v1/entities/host:0', status=409,
        headers={**large_headers, 'X-GraphDB-Read-Generation': '1'})
    docker('start', PROJECT+f'-node{old}-1')
    step('36 MiB backup restore resumes after leader SIGKILL and invalidates old read generation', old=old, new=new, payload_bytes=9*len(payload))

    if os.environ.get('GRAPHDB_GATE_SOAK') == '1':
        runner = PROJECT+'-soak'
        command = ('soak_status=0; go run -mod=readonly ./tools/soaktest -writer http://gateway:8080 -reader http://gateway:8080 '
            '-tenant raft-release-soak -duration 30m -writers 4 -readers 16 -batch-size 1 -write-interval 5s '
            '-http-timeout 120s -maintenance-timeout 10m -compact-interval 5m -gc-interval 10m '
            '-index-rebuild-interval 10m -out /evidence/soak.ndjson || soak_status=$?; '
            'go run -mod=readonly ./tools/soakreport -in /evidence/soak.ndjson -min-duration 30m -warmup 1m '
            '> /evidence/soak-report.txt; report_status=$?; '
            '[ "$soak_status" -eq 0 ] && [ "$report_status" -eq 0 ]')
        print('Starting 30-minute three-replica WAL soak with compact, GC and index rebuild', flush=True)
        with (OUT/'soak.log').open('w') as log:
            subprocess.run(DOCKER+['run','--rm','--name',runner,'--network',PROJECT+'_default',
                '-v',ROOT+':/src:ro','-v',str(OUT)+':/evidence',
                '-v','graphdb-raft-go-mod:/go/pkg/mod','-v','graphdb-raft-go-build:/root/.cache/go-build',
                '-w','/src','-e','GOMAXPROCS=2', 'golang:1.26.7-bookworm','bash','-c',command],
                check=True, stdout=log, stderr=subprocess.STDOUT)
        step('thirty-minute three-replica WAL mixed workload with compact, GC and index rebuild')
finally:
    docker('rm','-f',PROJECT+'-soak',check=False)
    for name in [PROJECT+'-standalone-direct',PROJECT+'-standalone-wal']:
        log = docker('logs',name,check=False)
        (OUT/(name+'.log')).write_text(log.stdout+log.stderr)
        docker('rm','-f',name,check=False)
        docker('volume','rm',name+'-data',check=False)
    subprocess.run(COMPOSE+['logs','--no-color'], env=ENV, text=True, stdout=(OUT/'raft.log').open('w'), stderr=subprocess.STDOUT)
    subprocess.run(COMPOSE+['down','-v'], env=ENV, text=True, stdout=(OUT/'cleanup.log').open('w'), stderr=subprocess.STDOUT)
