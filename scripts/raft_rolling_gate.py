#!/usr/bin/env python3
"""Qualify the review4 bridge and protocol-1 rolling restart on OrbStack/Linux.

GRAPHDB_ROLLING_BASE_IMAGE must contain the qualified previous binary (default d7b535b0).
Set GRAPHDB_ROLLING_BASE_SHA256/BASE_COMMIT and TARGET_VERSION/TARGET_COMMIT
when testing another explicitly identified compatibility window.
GRAPHDB_ROLLING_TARGET_IMAGE, GRAPHDB_ROLLING_OUTPUT and GRAPHDB_RAFT_TOKEN are
required. This is a one-host correctness gate, not cross-host qualification.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.environ['GRAPHDB_ROLLING_OUTPUT'])
BASE = os.environ['GRAPHDB_ROLLING_BASE_IMAGE']
TARGET = os.environ['GRAPHDB_ROLLING_TARGET_IMAGE']
TOKEN = os.environ['GRAPHDB_RAFT_TOKEN']
PROJECT = os.environ.get('GRAPHDB_ROLLING_PROJECT', 'graphdb-rolling-'+str(os.getpid()))
OFFSET = int(os.environ.get('GRAPHDB_ROLLING_PORT_OFFSET', '0'))
DOCKER = ['docker', '--context', os.environ.get('DOCKER_CONTEXT', 'orbstack')]
BASE_SHA = os.environ.get('GRAPHDB_ROLLING_BASE_SHA256', 'cb3688cfd2433fd644458a42fffc35211b9471abdd261d7fd22cfcbbe706783a')
BASE_COMMIT = os.environ.get('GRAPHDB_ROLLING_BASE_COMMIT', 'd7b535b0')
TARGET_VERSION = os.environ.get('GRAPHDB_ROLLING_TARGET_VERSION', '2.1.2-raft-rolling1')
TARGET_COMMIT = os.environ.get('GRAPHDB_ROLLING_TARGET_COMMIT', 'd7b535b0-rolling-dirty')
OUT.mkdir(parents=True, exist_ok=False)
events = []


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, text=True, capture_output=True, **kwargs)


def event(check, **details):
    row = {'check': check, 'result': 'PASS', **details}
    events.append(row)
    print(json.dumps(row), flush=True)
    (OUT/'results.json').write_text(json.dumps(events, indent=2)+'\n')


def image_identity(image):
    info = json.loads(run(DOCKER+['image', 'inspect', image]).stdout)[0]
    container = run(DOCKER+['create', image]).stdout.strip()
    try:
        binary = OUT/'binary'
        run(DOCKER+['cp', container+':/usr/local/bin/graphdb', str(binary)])
        digest = hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest()
        binary.unlink()
    finally:
        run(DOCKER+['rm', container])
    return {'image': image, 'image_id': info['Id'], 'binary_sha256': digest,
        'version_output': run(DOCKER+['run', '--rm', image, 'version']).stdout.strip()}


def request(base, method, path, body=None, tenant=None, cluster=None):
    headers = {'Content-Type': 'application/json'}
    if tenant:
        headers['X-Tenant-ID'] = tenant
    if cluster is not None:
        headers.update(Authorization='Bearer '+TOKEN, **{'X-Raft-Cluster': cluster})
    data = json.dumps(body).encode() if body is not None else (b'' if method == 'POST' else None)
    req = urllib.request.Request(base+path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=35) as response:
            raw = response.read()
            return response.status, json.loads(raw or b'{}')
    except urllib.error.HTTPError as err:
        raw = err.read()
        try:
            value = json.loads(raw)
        except ValueError:
            value = {'error': raw.decode(errors='replace')}
        return err.code, value


def expect(base, method, path, body=None, tenant=None, cluster=None, code=200):
    deadline = time.monotonic()+25
    safe = method == 'GET' or (path == '/v1/commits' and body and body.get('idempotency_key'))
    while True:
        actual, value = request(base, method, path, body, tenant, cluster)
        if actual == code:
            return value
        assert safe and actual in [502,503,504] and time.monotonic() < deadline, (base, method, path, actual, value)
        time.sleep(.05)


def wait(condition, timeout=120):
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = condition()
            if last:
                return last
        except (OSError, TimeoutError, urllib.error.URLError):
            pass
        time.sleep(.1)
    raise AssertionError(('timed out', last))


class Deployment:
    def __init__(self, sharded):
        self.sharded = sharded
        self.name = PROJECT+('-sharded' if sharded else '-single')
        self.folder = OUT/('sharded' if sharded else 'single')
        self.folder.mkdir()
        self.file = self.folder/'override.yml'
        self.base = ROOT/('docker-compose.sharded.yml' if sharded else 'docker-compose.raft.yml')
        self.compose = DOCKER+['compose', '-p', self.name, '-f', str(self.base), '-f', str(self.file)]
        self.groups = [
            {'cluster_id': 'graphdb-catalog', 'services': ['catalog1','catalog2','catalog3'], 'port': 57100},
            {'cluster_id': 'graphdb-shard-a', 'services': ['a1','a2','a3'], 'port': 57200},
            {'cluster_id': 'graphdb-shard-b', 'services': ['b1','b2','b3'], 'port': 57300},
        ] if sharded else [{'cluster_id': 'graphdb-ha', 'services': ['node1','node2','node3'], 'port': 56100}]
        self.images, self.legacy = {}, {}
        self.gateway = 'http://127.0.0.1:'+str((57080 if sharded else 56080)+OFFSET)
        self.stop = threading.Event()
        self.workers, self.stats = [], []
        self.write_override()

    def write_override(self):
        lines = ['services:', '  gateway:', f'    ports: !override ["127.0.0.1:{(57080 if self.sharded else 56080)+OFFSET}:8080"]']
        for group in self.groups:
            for i, service in enumerate(group['services'], 1):
                lines += [f'  {service}:', '    ports: !override ["127.0.0.1:'+str(group['port']+i+OFFSET)+':8081"]',
                    '    image: '+self.images.get(service, BASE), '    environment:',
                    '      GRAPHDB_RAFT_SNAPSHOT_ENTRIES: "5"', '      GRAPHDB_INGEST_MODE: "wal"',
                    '      GRAPHDB_INGEST_FLUSH_INTERVAL: "100ms"',
                    '      GRAPHDB_RAFT_ALLOW_LEGACY_PROTOCOL: "'+str(self.legacy.get(service, True)).lower()+'"']
        if self.sharded:
            for i, service in enumerate(['router', 'router2'], 1):
                lines += [f'  {service}:', '    image: '+self.images.get(service, BASE),
                    f'    ports: !override ["127.0.0.1:{57400+i+OFFSET}:8080"]']
        self.file.write_text('\n'.join(lines)+'\n')

    def command(self, *args):
        return run(self.compose+list(args), env={**os.environ, 'GRAPHDB_HA_IMAGE': BASE})

    def node(self, group, i):
        return 'http://127.0.0.1:'+str(group['port']+i+OFFSET)

    def statuses(self, group):
        return [expect(self.node(group, i), 'GET', '/raft/status', cluster=group['cluster_id']) for i in range(1,4)]

    def caught_up(self, group):
        leader = expect(self.node(group, 1), 'GET', '/raft/status', cluster=group['cluster_id'])['leader_id']
        if not leader:
            return False
        position = expect(self.node(group, leader), 'GET', '/raft/status', cluster=group['cluster_id'])
        commit = position['commit_index']
        values = self.statuses(group)
        if not leader or any(value.get('error') or value.get('snapshot_error') or value['leader_id'] != leader for value in values):
            return False
        return values if position['leader_id'] == leader and commit and all(value['applied_index'] >= commit for value in values) else False

    def transfer(self, group, target):
        def attempt():
            if self.statuses(group)[0]['leader_id'] == target:
                return True
            code, value = request(self.node(group,target),'POST','/raft/transfer',{'target':target},cluster=group['cluster_id'])
            if code == 409:
                return False
            assert code == 204, (code,value)
            return True
        wait(attempt)

    def replace(self, group, i, image, legacy=True, drain=False):
        service = group['services'][i-1]
        (self.folder/f'{service}-before-replace-{len(events)}.log').write_text(
            self.command('logs', '--no-color', '--tail', '2000', service).stdout)
        if drain:
            expect(self.node(group, i), 'POST', '/raft/drain', cluster=group['cluster_id'], code=204)
        self.images[service], self.legacy[service] = image, legacy
        self.write_override()
        self.command('up', '-d', '--no-build', '--no-deps', '--force-recreate', service)
        values = wait(lambda: self.caught_up(group))
        assert run(DOCKER+['inspect', '--format', '{{.Image}}', self.name+'-'+service+'-1']).stdout.strip() == metadata['target' if image == TARGET else 'base']['image_id']
        if image == TARGET:
            assert values[i-1]['protocol_version'] == 1 and values[i-1]['build']['version'] == TARGET_VERSION and values[i-1]['build']['commit'] == TARGET_COMMIT
        event('replica replaced and caught up', deployment=self.name, group=group['cluster_id'], node=i, image=image, legacy=legacy)

    def traffic(self, tenant):
        stats = {'tenant': tenant, 'writes': 0, 'reads': 0, 'wal': 0, 'attempts': 0, 'retries': 0, 'failures': []}
        self.stats.append(stats)
        def retry(method, path, body=None, expected=200):
            deadline = time.monotonic()+25
            while True:
                stats['attempts'] += 1
                try:
                    code, value = request(self.gateway, method, path, body, tenant)
                    if code == expected:
                        return value
                    if code not in [502,503,504]:
                        raise AssertionError((method,path,code,value))
                except (OSError, TimeoutError, urllib.error.URLError) as err:
                    value = str(err)
                if time.monotonic() >= deadline:
                    raise AssertionError(('logical request failed after retries',path,value))
                stats['retries'] += 1
                time.sleep(.05)
        def worker():
            i = 0
            try:
                while not self.stop.is_set():
                    i += 1
                    result = retry('POST', '/v1/commits', {'idempotency_key': f'write-{i}', 'mutations': {'upsert_entities': [
                        {'id': 'host:rolling', 'kind': 'host', 'fields': {'seq': i}}]}})
                    entity = retry('GET', '/v1/entities/host:rolling?min_version='+str(result['version']))
                    assert entity['entity']['fields']['seq'] == i, (i, entity)
                    stats['writes'] += 1
                    stats['reads'] += 1
                    if i % 8 == 0:
                        batch = {'source':'agent','collector_id':'rolling','batch_id':f'wal-{i}','idempotency_key':f'wal-{i}',
                            'items':[{'external_id': 'host:wal','entity':{'id':'host:wal','kind':'host','fields':{'seq':i}}}]}
                        accepted = retry('POST','/v1/ingest/batches', batch, 202)
                        assert accepted['durability'] == 'raft_majority', accepted
                        deadline = time.monotonic()+25
                        while True:
                            terminal = retry('GET', f'/v1/ingest/batches/agent/rolling/wal-{i}')
                            if terminal['state'] == 'committed':
                                break
                            assert terminal['state'] != 'failed' and time.monotonic() < deadline, terminal
                            time.sleep(.05)
                        stats['wal'] += 1
                    time.sleep(.015)
            except Exception as err:
                stats['failures'].append(str(err))
        thread = threading.Thread(target=worker)
        thread.start()
        self.workers.append(thread)

    def qualify(self):
        self.command('up', '-d', '--no-build')
        for group in self.groups:
            wait(lambda: self.caught_up(group))
        wait(lambda: request(self.gateway, 'GET', '/v1/readiness')[0] == 200)
        tenants = ['rolling-a','rolling-b'] if self.sharded else ['rolling']
        if self.sharded:
            for group, tenant, shard in zip(self.groups[1:], tenants, ['shard-a','shard-b']):
                definition = {'id':shard, 'cluster_id':group['cluster_id'],
                    'peers':{str(i):'http://'+service+':8081' for i,service in enumerate(group['services'],1)}}
                expect(self.gateway,'POST','/v1/cluster/shards',definition,cluster='',code=202)
                expect(self.gateway,'POST','/v1/cluster/placements',{'tenant_id':tenant,'target':shard},cluster='',code=202)
                wait(lambda: expect(self.gateway,'GET','/v1/cluster',cluster='')['tenants'][tenant]['state'] == 'active')
        for tenant in tenants:
            expect(self.gateway,'POST','/v1/tenants',{'tenant_id':tenant}, tenant, code=200)
        self.traffic(tenants[0])
        if self.sharded:
            self.traffic(tenants[1])
        for group in self.groups:
            original = self.statuses(group)[0]['leader_id']
            followers = [i for i in range(1,4) if i != original]
            for i in followers:
                self.replace(group,i,TARGET)
            target = followers[0]
            self.transfer(group,target)
            wait(lambda: self.statuses(group)[0]['leader_id'] == target)
            # The bridge leader has no drain/forwarding API. Observe the proxy's
            # health-check convergence before issuing a non-idempotent request.
            # Background traffic still measures identity-preserving retries.
            wait(lambda: request(self.gateway, 'GET', '/v1/readiness')[0] == 200, timeout=25)
            if not self.sharded:
                # New leader prepares the SHA256 task execution payload while
                # the remaining old voter still applies it.
                expect(self.gateway,'POST','/v1/tenants',{'tenant_id':'rolling-import'},'rolling-import')
                task = expect(self.gateway,'POST','/v1/imports?format=jsonl&batch_size=1', {'entity':{'id':'host:import','kind':'host'}}, 'rolling-import', code=202)
                wait(lambda: expect(self.gateway,'GET','/v1/tasks/'+task['id'],tenant='rolling-import')['status'] == 'succeeded')
                expect(self.gateway,'GET','/v1/entities/host:import',tenant='rolling-import')
                rollback = followers[1]
                expect(self.node(group,rollback),'POST','/raft/drain',cluster=group['cluster_id'],code=204)
                self.command('stop','--timeout','30',group['services'][rollback-1])
                for i in range(15):
                    expect(self.gateway,'POST','/v1/commits', {'idempotency_key':f'compact-{i}','mutations':{'upsert_entities':[{'id':f'host:compact-{i}','kind':'host'}]}},'rolling-import')
                self.replace(group,rollback,BASE)
                logs = self.command('logs','--no-color',group['services'][rollback-1]).stdout
                assert 'restored snapshot' in logs, 'old binary did not install a compacted new snapshot'
                event('new commands and snapshots replay on qualified previous binary', source_commit=BASE_COMMIT, deployment=self.name)
                self.replace(group,rollback,TARGET)
            self.replace(group,original,TARGET)
            wait(lambda: self.caught_up(group))
        if self.sharded:
            for i, service in enumerate(['router','router2'],1):
                self.images[service] = TARGET
                self.write_override()
                self.command('up','-d','--no-build','--no-deps','--force-recreate',service)
                wait(lambda: request(f'http://127.0.0.1:{57400+i+OFFSET}','GET','/v1/readiness')[0] == 200)
        # Close the legacy window one replica at a time. Strict peers then reject
        # unversioned traffic; the old program cannot accidentally rejoin.
        for group in self.groups:
            for i in range(1,4):
                self.replace(group,i,TARGET,legacy=False,drain=True)
        inventory = {'groups': []}
        for group in self.groups:
            inventory['groups'].append({'cluster_id':group['cluster_id'], 'nodes':[
                {'id':i,'url':self.node(group,i),'restart':self.compose+['up','-d','--no-build','--no-deps','--force-recreate',service]}
                for i,service in enumerate(group['services'],1)]})
        if self.sharded:
            inventory['routers'] = [{'id':i,'url':f'http://127.0.0.1:{57400+i+OFFSET}',
                'restart':self.compose+['up','-d','--no-build','--no-deps','--force-recreate',service]}
                for i,service in enumerate(['router','router2'],1)]
        inventory_path = self.folder/'inventory.json'
        inventory_path.write_text(json.dumps(inventory,indent=2)+'\n')
        command = [sys.executable,str(ROOT/'scripts/raft_rolling_upgrade.py'),'--inventory',str(inventory_path),
            '--execute','--force','--target-version',TARGET_VERSION,'--target-commit',TARGET_COMMIT,'--report',str(self.folder/'controller.json')]
        (self.folder/'before-controller.log').write_text(
            self.command('logs', '--no-color', '--tail', '2000').stdout)
        result = run(command,env={**os.environ,'GRAPHDB_HA_IMAGE':BASE})
        (self.folder/'controller.log').write_text(result.stdout+result.stderr)
        self.stop.set()
        for worker in self.workers:
            worker.join(timeout=40)
            assert not worker.is_alive(), 'traffic worker failed to finish'
        assert all(row['writes'] > 10 and row['wal'] > 0 and not row['failures'] for row in self.stats), self.stats
        for row in self.stats:
            entity = expect(self.gateway,'GET','/v1/entities/host:rolling',tenant=row['tenant'])
            assert entity['entity']['fields']['seq'] == row['writes'], (entity,row)
        # Elect every replica and compare its own strong-read export.
        expected = {tenant:expect(self.gateway,'GET','/v1/export/snapshot',tenant=tenant) for tenant in tenants}
        for group in self.groups:
            if 'catalog' in group['cluster_id']:
                continue
            for i in range(1,4):
                if self.statuses(group)[0]['leader_id'] != i:
                    self.transfer(group,i)
                for tenant in tenants:
                    actual = expect(self.gateway,'GET','/v1/export/snapshot',tenant=tenant)
                    assert actual == expected[tenant], (group['cluster_id'],i,tenant)
        event('rolling bridge and strict controller preserve direct/WAL writes and strong reads', deployment=self.name, traffic=self.stats)

    def close(self):
        self.stop.set()
        for worker in self.workers:
            worker.join(timeout=40)
        try:
            (self.folder/'compose.log').write_text(self.command('logs','--no-color').stdout)
        finally:
            self.command('down','--volumes','--remove-orphans')


metadata = {'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'result':'RUNNING',
    'base':image_identity(BASE), 'target':image_identity(TARGET), 'cross_host_qualification':'NOT RUN'}
assert metadata['base']['binary_sha256'] == BASE_SHA, 'unqualified legacy source binary'
assert metadata['base']['binary_sha256'] != metadata['target']['binary_sha256'], 'mixed-version gate used one binary twice'
path = OUT/'metadata.json'
path.write_text(json.dumps(metadata,indent=2)+'\n')
try:
    for sharded in [False,True]:
        deployment = Deployment(sharded)
        try:
            deployment.qualify()
        finally:
            deployment.close()
    metadata['result'] = 'PASS'
except Exception as err:
    metadata.update(result='FAIL',error=str(err))
    raise
finally:
    metadata['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    path.write_text(json.dumps(metadata,indent=2)+'\n')
