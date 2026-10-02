import hashlib
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from raft_gate_metrics import collect

ROOT = str(Path(__file__).resolve().parents[1])
OUT = Path(os.environ['GRAPHDB_GATE_OUTPUT']) / 'sharded'
OUT.mkdir(parents=True, exist_ok=False)
DOCKER = ['docker']
if os.environ.get('DOCKER_CONTEXT'):
    DOCKER += ['--context', os.environ['DOCKER_CONTEXT']]
IMAGE = os.environ['GRAPHDB_GATE_IMAGE']
PORT_OFFSET = int(os.environ.get('GRAPHDB_GATE_PORT_OFFSET', '0'))
PROJECT = os.environ['GRAPHDB_GATE_PROJECT'] + '-sharded'
TOKEN = os.environ['GRAPHDB_RAFT_TOKEN']
COMPOSE = DOCKER + ['compose', '-p', PROJECT, '-f', ROOT+'/docker-compose.sharded.yml', '-f', str(OUT/'override.yml')]
ENV = {**os.environ, 'GRAPHDB_HA_IMAGE': IMAGE, 'GRAPHDB_RAFT_TOKEN': TOKEN, 'GRAPHDB_INGEST_MODE': 'wal'}
ROUTER = f'http://127.0.0.1:{47080+PORT_OFFSET}'
ROUTER_LOCAL = f'http://127.0.0.1:{47083+PORT_OFFSET}'
fixture = (Path(__file__).parent/'raft-gate'/'sharded.yml').read_text()
if os.environ.get('GRAPHDB_GATE_ENHANCED') == 'true':
    protocol = int(os.environ.get('GRAPHDB_GATE_PROTOCOL_VERSION', '2'))
    assert protocol in (2, 3)
    fixture = fixture.replace('GRAPHDB_RAFT_SNAPSHOT_ENTRIES: "5"', f'GRAPHDB_RAFT_SNAPSHOT_ENTRIES: "5"\n      GRAPHDB_RAFT_PROTOCOL_VERSION: "{protocol}"\n      GRAPHDB_RAFT_STREAM_SNAPSHOTS: "true"')
(OUT/'override.yml').write_text(re.sub(r'(?<=127.0.0.1:)\d+', lambda match: str(int(match[0])+PORT_OFFSET), fixture))
results = []
route_retries = []

def docker(*args, check=True):
    return subprocess.run(DOCKER+list(args), check=check, capture_output=True, text=True)

def compose(*args, check=True):
    return subprocess.run(COMPOSE+list(args), env=ENV, check=check, capture_output=True, text=True)

def service(name):
    return PROJECT+'-'+name+'-1'

def node(group, i):
    return 'http://127.0.0.1:'+str({'catalog':47100,'a':47200,'b':47300}[group]+i+PORT_OFFSET)

def request(base, method, path, body=None, tenant=None, headers=None):
    hdr = {'Content-Type':'application/json', 'Authorization':'Bearer '+TOKEN, **(headers or {})}
    if tenant:
        hdr['X-Tenant-ID'] = tenant
    req = urllib.request.Request(base+path, data=None if body is None else json.dumps(body).encode(), method=method, headers=hdr)
    try:
        with urllib.request.urlopen(req, timeout=12) as response:
            return response.status, json.loads(response.read() or b'{}'), {name.lower():value for name,value in response.headers.items()}
    except urllib.error.HTTPError as err:
        raw = err.read()
        try:
            body = json.loads(raw)
        except ValueError:
            body = {'error':raw.decode()}
        return err.code, body, {name.lower():value for name,value in err.headers.items()}

def expect(method, path, body=None, tenant=None, status=200, base=ROUTER, headers=None):
    deadline = time.monotonic()+10
    while True:
        code, value, hdr = request(base,method,path,body,tenant,headers)
        if code == status:
            return value,hdr
        # This response guarantees rejection before graph/WAL publication.
        # Replay only this explicit routing error, preserving the original body.
        if code == 409 and isinstance(value,dict) and value.get('code') == 'shard_epoch_changed' and value.get('retryable') is True and time.monotonic()<deadline:
            route_retries.append({'method':method,'path':path,'tenant':tenant,'response':value})
            (OUT/'route-retries.json').write_text(json.dumps(route_retries,indent=2)+'\n')
            time.sleep(.1)
            continue
        raise AssertionError((method,path,code,value))

def wait(fn, seconds=60):
    deadline = time.monotonic()+seconds
    last = None
    while time.monotonic()<deadline:
        try:
            last = fn()
            if last:
                return last
        except (OSError, TimeoutError, AssertionError) as err:
            last = str(err)
        time.sleep(.1)
    raise AssertionError(('condition did not complete',last))

def catalog():
    return expect('GET','/v1/cluster')[0]

def placement(tenant, target=None, epoch=None, phase=None, complete=False, error=False):
    value = catalog()['tenants'][tenant]
    if target and value['shard_id']!=target:
        return False
    if epoch and value['epoch']!=epoch:
        return False
    if phase and value.get('move',{}).get('phase')!=phase:
        return False
    if complete and (value['state']!='active' or 'move' in value):
        return False
    if error and not value.get('move',{}).get('error'):
        return False
    return value

def leader(group):
    def find():
        for i in range(1,4):
            try:
                status = request(node(group,i),'GET','/v1/health')[1].get('raft', {})
                if status.get('leader_id') == i and not status.get('draining') and request(node(group,i),'GET','/v1/readiness')[0]==200:
                    return i
            except (OSError,TimeoutError):
                pass
        return False
    return wait(find)

def commit(identity, name):
    return {'idempotency_key':identity, 'mutations':{'upsert_entities':[{'id':'host:1','kind':'host','fields':{'name':name}}]}}

def register(group):
    return expect('POST','/v1/cluster/shards',{'id':'shard-'+group,'cluster_id':'graphdb-shard-'+group,'peers':{str(i):f'http://{group}{i}:8081' for i in range(1,4)}},status=202)

def step(name, **details):
    results.append({'check':name,'result':'PASS',**details})
    print(json.dumps(results[-1]),flush=True)
    (OUT/'results.json').write_text(json.dumps(results,indent=2)+'\n')

try:
    compose('up','-d','--no-build','catalog1','catalog2','catalog3','a1','a2','a3','router','router2','gateway')
    for mode,port in [('direct',47081),('wal',47082)]:
        port += PORT_OFFSET
        docker('run','-d','--name',PROJECT+'-standalone-'+mode,'--restart=no',
            '-p',f'127.0.0.1:{port}:8080','-v',PROJECT+'-standalone-'+mode+':/var/lib/graphdb',
            '-e','GRAPHDB_INGEST_MODE='+mode,'-e','GRAPHDB_INGEST_FLUSH_INTERVAL=200ms',IMAGE)
        base = 'http://127.0.0.1:'+str(port)
        wait(lambda: request(base,'GET','/v1/readiness')[0]==200)
        expect('POST','/v1/tenants',{'tenant_id':'tenant-a'},base=base)
        expect('POST','/v1/commits',commit('same-key',mode),tenant='tenant-a',base=base)
    leader('catalog'); leader('a')
    wait(lambda: request(ROUTER,'GET','/v1/readiness')[0] == 200)
    register('a')
    with urllib.request.urlopen(ROUTER+'/openapi.yaml',timeout=10) as response:
        assert b'/v1/cluster/moves:' in response.read()
    expect('POST','/v1/tenants',{'tenant_id':'tenant-a'})
    expect('POST','/v1/commits',commit('same-key','sharded'),tenant='tenant-a')
    step('standalone direct/WAL coexist with sharded Raft',processes_before_expansion=11)
    observations = []
    for group in ['catalog','a']:
        for i in range(1,4):
            expected = {'graphdb_filesystem_inspection_success{role="raft"}': 1, 'graphdb_raft_voters': 3}
            if group == 'catalog' and i == leader('catalog'):
                expected['graphdb_catalog_observation_known'] = 1
                expected['graphdb_catalog_shards'] = 1
            observations.append(collect(node(group,i), OUT, 'metrics-'+group+str(i), expected))
    expect('GET','/v1/entities/host:1',tenant='tenant-a',base=ROUTER_LOCAL)
    observations.append(collect(ROUTER_LOCAL, OUT, 'metrics-router', {'graphdb_router_draining': 0, 'graphdb_router_events_total{event="proxy_response_2xx"}': None}, {'Authorization':'Bearer '+TOKEN}))
    step('catalog, shard replicas and authenticated router expose local diagnostics',observations=observations)
    compose('up','-d','--no-build','b1','b2','b3')
    leader('b'); register('b')
    assert placement('tenant-a','shard-a',1,complete=True)
    expect('POST','/v1/tenants',{'tenant_id':'tenant-b'})
    assert placement('tenant-b','shard-b',1,complete=True)
    expect('POST','/v1/commits',commit('new-shard','new-group'),tenant='tenant-b')
    step('add three-replica shard without remapping existing tenant',processes=14)
    accepted,_ = expect('POST','/v1/ingest/batches',{
        'source':'agent','collector_id':'expansion','batch_id':'large','idempotency_key':'large',
        'items':[{'external_id':'host:2','entity':{'id':'host:2','kind':'host','fields':{'payload':'transfer-data'*750000}}}]},tenant='tenant-a',status=202)
    for i in range(1,4):
        docker('pause',service('b'+str(i)))
    expect('POST','/v1/cluster/moves',{'tenant_id':'tenant-a','target':'shard-b'},status=202)
    wait(lambda: placement('tenant-a',phase='copy',error=True))
    observation = collect(node('catalog',leader('catalog')), OUT, 'metrics-migration-error', {'graphdb_catalog_move_errors': 1, 'graphdb_catalog_moves{phase="copy"}': 1})
    step('catalog diagnostics expose the blocked migration phase and retained error',observation=observation)
    assert request(ROUTER,'GET','/v1/entities/host:1',tenant='tenant-a')[0]==503
    old_catalog = leader('catalog')
    docker('update','--restart=no',service('catalog'+str(old_catalog)))
    docker('kill',service('catalog'+str(old_catalog)))
    wait(lambda: catalog())
    docker('start',service('catalog'+str(old_catalog)))
    for i in range(1,4):
        docker('unpause',service('b'+str(i)))
    interrupted = False
    until = time.monotonic()+30
    while time.monotonic()<until:
        b_leader = leader('b')
        probe = docker('exec',service('b'+str(b_leader)),'sh','-c','find /var/lib/graphdb/graphdb/control/sharding/transfers -type f 2>/dev/null | head -n 1',check=False)
        if probe.stdout.strip():
            docker('update','--restart=no',service('b'+str(b_leader)))
            docker('kill',service('b'+str(b_leader)))
            interrupted = True
            break
        if placement('tenant-a','shard-b',complete=True):
            break
        time.sleep(.05)
    wait(lambda: placement('tenant-a','shard-b',2,complete=True))
    if interrupted:
        docker('start',service('b'+str(b_leader)))
    assert interrupted, 'destination failover was not exercised during transfer'
    replay,_ = expect('POST','/v1/commits',commit('same-key','sharded'),tenant='tenant-a')
    assert replay['idempotent_replay'] is True and replay['version']==1,replay
    status,_ = expect('GET','/v1/ingest/writers/raft-graphdb-shard-a/batches/agent/expansion/large',tenant='tenant-a')
    assert status['accepted_lsn']==accepted['accepted_lsn'] and status['state']=='committed',status
    _,headers = expect('POST','/v1/query',{'op':'match','kind':'host','min_version':2,'limit':10},tenant='tenant-a')
    assert headers['x-graphdb-shard-id']=='shard-b',headers
    source = leader('a')
    assert request(node('a',source),'POST','/v1/commits',commit('stale','bad'),tenant='tenant-a',headers={'X-GraphDB-Route-Epoch':'1'})[0]==409
    files = docker('exec',service('a'+str(source)),'sh','-c','find /var/lib/graphdb/graphdb/tenants/tenant-a -type f 2>/dev/null',check=False)
    assert not files.stdout.strip(),files.stdout
    step('chunked migration survives catalog and destination leader interruption',accepted_lsn=status['accepted_lsn'],destination_interrupted_during_transfer=interrupted)
    collect(node('catalog',leader('catalog')), OUT, 'metrics-migration-completed', {'graphdb_catalog_move_errors': 0, 'graphdb_catalog_moves{phase="copy"}': 0})
    expect('POST','/v1/cluster/moves',{'tenant_id':'tenant-a','target':'shard-a'},status=202)
    wait(lambda: placement('tenant-a','shard-a',3,complete=True))
    expect('POST','/v1/commits',commit('after-return','returned'),tenant='tenant-a')
    step('tenant can return to retired source and commit again')
    for i in range(1,4):
        docker('pause',service('a'+str(i)))
    expect('POST','/v1/commits',commit('independent','still-up'),tenant='tenant-b')
    for mode,port in [('direct',47081),('wal',47082)]:
        port += PORT_OFFSET
        value,_ = expect('GET','/v1/entities/host:1',tenant='tenant-a',base='http://127.0.0.1:'+str(port))
        assert value['entity']['fields']['name']==mode,value
    for i in range(1,4):
        docker('unpause',service('a'+str(i)))
    leader('a')
    step('one shard loses quorum while other shard and standalones serve')
    for i in range(1,4):
        docker('pause',service('b'+str(i)))
    expect('POST','/v1/cluster/moves',{'tenant_id':'tenant-a','target':'shard-b'},status=202)
    wait(lambda: placement('tenant-a',phase='copy',error=True))
    expect('POST','/v1/cluster/moves/tenant-a/cancel',status=202)
    wait(lambda: placement('tenant-a','shard-a',4,phase='discard'))
    expect('GET','/v1/entities/host:1',tenant='tenant-a')
    for i in range(1,4):
        docker('unpause',service('b'+str(i)))
    wait(lambda: placement('tenant-a','shard-a',4,complete=True))
    step('cancel before cutover resumes source before unavailable target cleanup')
    a_node = 1
    docker('stop','-t','10',service('a'+str(a_node)))
    invalid = docker('run','--rm','--name',PROJECT+'-role-check','--network',PROJECT+'_default',
        '-v',PROJECT+'_a1:/var/lib/graphdb','-e','GRAPHDB_RAFT_NODE_ID=1',
        '-e','GRAPHDB_RAFT_CLUSTER_ID=graphdb-shard-a','-e','GRAPHDB_RAFT_ADDR=:8081',
        '-e','GRAPHDB_RAFT_TOKEN='+TOKEN,
        '-e','GRAPHDB_RAFT_PEERS='+json.dumps({str(i):f'http://a{i}:8081' for i in range(1,4)}),IMAGE,check=False)
    assert invalid.returncode!=0 and 'directory role' in invalid.stderr,invalid.stderr
    docker('start',service('a1')); leader('a')
    wait(lambda: expect('GET','/v1/entities/host:1',tenant='tenant-a'))
    images = {docker('inspect','--format','{{.Image}}',name).stdout.strip() for name in [service('router'),service('a1'),PROJECT+'-standalone-direct',PROJECT+'-standalone-wal']}
    assert len(images)==1,images
    step('replica cannot remove shard role; original configuration recovers',image_id=images.pop())
    expect('GET','/v1/entities/host:1',tenant='tenant-a',base=ROUTER_LOCAL)
    expect('GET','/v1/entities/host:1',tenant='tenant-a',base=ROUTER)
    for i in range(1,4):
        docker('pause',service('catalog'+str(i)))
    time.sleep(6)
    expect('GET','/v1/readiness',base=ROUTER)
    expect('POST','/v1/commits',commit('catalog-outage','available-without-catalog'),tenant='tenant-a')
    value,_ = expect('GET','/v1/entities/host:1',tenant='tenant-a')
    assert value['entity']['fields']['name']=='available-without-catalog',value
    expect('GET','/v1/entities/host:1',tenant='unknown-tenant',status=503,base=ROUTER_LOCAL)
    observation = collect(ROUTER_LOCAL, OUT, 'metrics-router-catalog-fallback', {'graphdb_router_catalog_unavailable': 1, 'graphdb_router_events_total{event="placement_stale_fallback"}': None}, {'Authorization':'Bearer '+TOKEN})
    step('gateway keeps known tenant strong reads and idempotent writes during catalog outage; unknown routes fail closed',observation=observation)
    expect('POST','/v1/router/drain',status=204,base=ROUTER_LOCAL)
    observation = collect(ROUTER_LOCAL, OUT, 'metrics-router-no-catalog', {'graphdb_router_draining': 1}, {'Authorization':'Bearer '+TOKEN})
    for i in range(1,4):
        docker('unpause',service('catalog'+str(i)))
    expect('POST','/v1/router/resume',status=204,base=ROUTER_LOCAL)
    step('drained router diagnostics do not require a reachable catalog',observation=observation)
    (OUT/'final-catalog.json').write_text(json.dumps(catalog(),indent=2)+'\n')
finally:
    logs = compose('logs','--no-color',check=False)
    (OUT/'containers.log').write_text(logs.stdout+logs.stderr)
    for name in ['a1','a2','a3','b1','b2','b3','catalog1','catalog2','catalog3']:
        docker('unpause',service(name),check=False)
    compose('down','-v','--remove-orphans',check=False)
    for mode in ['direct','wal']:
        docker('rm','-f',PROJECT+'-standalone-'+mode,check=False)
        docker('volume','rm',PROJECT+'-standalone-'+mode,check=False)
