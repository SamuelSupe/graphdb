#!/usr/bin/env python3
"""Compare Raft binaries in a Linux container using fresh, independent directories.

Run inside OrbStack with the binaries and this repository mounted read-only.
Use the same binary twice to calibrate before comparing revisions. The existing
loadtest checks publication counts and minimum query versions in every run.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.error
import urllib.request

from local_disk_benchmark import proc_sample

TOKEN = 'raft-performance-local-only-token-0123456789'
CASES = {'read-1': (0, 1, 'direct'), 'read-8': (0, 8, 'direct'),
         'read-32': (0, 32, 'direct'), 'mixed-direct': (4, 16, 'direct'),
         'write-direct': (4, 0, 'direct'), 'mixed-wal': (4, 16, 'wal')}


def request(base, path, body=None, cluster_id=None):
    headers = {'X-Tenant-ID': 'bench', 'Content-Type': 'application/json',
               'Authorization': 'Bearer ' + TOKEN}
    if cluster_id:
        headers['X-Raft-Cluster'] = cluster_id
    req = urllib.request.Request(base + path,
        data=None if body is None else json.dumps(body).encode(),
        headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        data = response.read()
        return json.loads(data) if data else None


class Cluster:
    def __init__(self, binary, folder, mode, topology, replicas):
        self.binary, self.folder, self.mode, self.topology = binary, folder, mode, topology
        self.replicas = replicas
        self.processes, self.logs = [], []

    def spawn(self, name, environment, command='serve'):
        env = {k: v for k, v in os.environ.items() if not k.startswith(('GRAPHDB_', 'S3_'))}
        env.update(environment, GOMAXPROCS='2')
        log = (self.folder / (name + '.log')).open('ab')
        self.logs.append(log)
        self.processes.append(subprocess.Popen([self.binary, command], env=env, stdout=log, stderr=log))

    def group(self, name, port, role, reopen):
        peers = json.dumps({str(i): f'http://127.0.0.1:{port+1000+i}' for i in range(1, self.replicas+1)})
        initial_peers = json.dumps({str(i): f'http://127.0.0.1:{port+1000+i}' for i in range(1, 4)})
        for i in range(1, self.replicas+1):
            self.spawn(name+str(i), {
                'GRAPHDB_ADDR': f'127.0.0.1:{port+i}',
                'GRAPHDB_DATA_DIR': str(self.folder / (name+str(i))),
                'GRAPHDB_RAFT_NODE_ID': str(i), 'GRAPHDB_RAFT_CLUSTER_ID': name,
                'GRAPHDB_RAFT_ADDR': f'127.0.0.1:{port+1000+i}',
                'GRAPHDB_RAFT_PEERS': peers if reopen or i > 3 else initial_peers,
                'GRAPHDB_RAFT_BOOTSTRAP': 'false' if reopen or i > 3 else 'true',
                'GRAPHDB_RAFT_TOKEN': TOKEN,
                'GRAPHDB_INGEST_MODE': self.mode, 'GRAPHDB_INGEST_FLUSH_INTERVAL': '100ms',
                **({'GRAPHDB_ADMIN_ADDR': f'127.0.0.1:{port+2000+i}',
                    'GRAPHDB_PPROF_ENABLED': 'true'} if self.profiling else {}), **role})
        return peers

    def expand(self, name, port):
        for i in range(4, self.replicas+1):
            leader = self.leader(port)
            base = f'http://127.0.0.1:{port+1000+leader}'
            request(base, '/raft/members', {'action': 'add_learner', 'id': i,
                'url': f'http://127.0.0.1:{port+1000+i}'}, name)
            deadline = time.monotonic()+30
            while True:
                try:
                    request(base, '/raft/members', {'action': 'promote', 'id': i}, name)
                    break
                except urllib.error.HTTPError as err:
                    if err.code != 409 or time.monotonic() >= deadline:
                        raise
                    time.sleep(.1)

    def leader(self, port):
        deadline = time.monotonic()+30
        while time.monotonic()<deadline:
            if any(p.poll() is not None for p in self.processes):
                raise RuntimeError('replica exited; inspect server logs')
            for i in range(1, self.replicas+1):
                try:
                    request(f'http://127.0.0.1:{port+i}', '/v1/readiness')
                    return i
                except (OSError, urllib.error.URLError):
                    pass
            time.sleep(.1)
        raise RuntimeError('no ready leader')

    def start(self, profiling=False, reopen=False):
        self.profiling = profiling
        role = {'GRAPHDB_RAFT_SHARD_ID': 'data'} if self.topology == 'sharded' else {}
        self.group('data', 40080, role, reopen)
        leader = self.leader(40080)
        if not reopen:
            self.expand('data', 40080)
            leader = self.leader(40080)
        self.base = f'http://127.0.0.1:{40080+leader}'
        self.profile_base = f'http://127.0.0.1:{42080+leader}'
        if self.topology == 'sharded':
            peers = self.group('catalog', 44080, {'GRAPHDB_RAFT_CATALOG': 'true'}, reopen)
            self.leader(44080)
            if not reopen:
                self.expand('catalog', 44080)
            self.spawn('router', {'GRAPHDB_ADDR': '127.0.0.1:40080',
                'GRAPHDB_ROUTER_TOKEN': TOKEN, 'GRAPHDB_ROUTER_CATALOG_CLUSTER_ID': 'catalog',
                'GRAPHDB_ROUTER_CATALOG_PEERS': peers}, 'serve-router')
            self.base = 'http://127.0.0.1:40080'
            for attempt in range(50):
                try:
                    request(self.base, '/v1/readiness')
                    break
                except (OSError, urllib.error.URLError):
                    if attempt == 49:
                        raise
                    time.sleep(.1)
            if not reopen:
                request(self.base, '/v1/cluster/shards', {'id': 'data', 'cluster_id': 'data',
                    'peers': {str(i): f'http://127.0.0.1:{41080+i}' for i in range(1, self.replicas+1)}})
        if not reopen:
            request(self.base, '/v1/tenants', {'tenant_id': 'bench'})

    def stop(self):
        for p in self.processes:
            if p.poll() is None:
                p.terminate()
        for p in self.processes:
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
                raise RuntimeError('replica did not shut down')
        for log in self.logs:
            log.close()


def load(args, cluster, folder, stage, writers, readers, mode, duration=0, seed=False):
    command = [args.loadtest, '-base', cluster.base, '-tenant', 'bench',
        '-entities', str(args.entities), '-batch-size', '20', '-writers', str(writers),
        '-readers', str(readers), '-http-timeout', '60s', '-timeout', '10m',
        '-warmup', '0', '-run-id', folder.name+'-'+stage, '-update-epoch', stage,
        '-report-json', str(folder / (stage+'.json'))]
    command += ['-seed-only'] if seed else ['-skip-seed', '-duration', str(duration)+'s']
    if mode == 'wal' and not seed:
        command.append('-wal')
    with (folder / (stage+'.log')).open('wb') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)


def run(args, name, binary, case, iteration):
    folder = args.output / f'{iteration}-{case}-{name}'
    folder.mkdir(parents=True, exist_ok=False)
    writers, readers, mode = CASES[case]
    cluster = Cluster(binary, folder, mode, args.topology, args.replicas)
    profile_errors = []
    def profile():
        try:
            url = cluster.profile_base+f'/debug/pprof/profile?seconds={int(args.seconds)}'
            with urllib.request.urlopen(url, timeout=args.seconds+30) as response:
                (folder/'cpu.pprof').write_bytes(response.read())
        except Exception as err:
            profile_errors.append(str(err))
    try:
        cluster.start()
        load(args, cluster, folder, 'seed', 4, 0, mode, seed=True)
        if args.profile:
            cluster.stop()
            cluster = Cluster(binary, folder, mode, args.topology, args.replicas)
            cluster.start(profiling=True, reopen=True)
        if args.warmup > 0:
            load(args, cluster, folder, 'warm', 0, max(1, readers), mode, args.warmup)
        before = [proc_sample(p.pid) for p in cluster.processes]
        sampler = threading.Thread(target=profile) if args.profile else None
        if sampler:
            sampler.start()
        load(args, cluster, folder, 'measure', writers, readers, mode, args.seconds)
        after = [proc_sample(p.pid) for p in cluster.processes]
        if sampler:
            sampler.join()
        if profile_errors:
            raise RuntimeError(profile_errors)
        report = json.loads((folder/'measure.json').read_text())
        if not report['success']:
            raise RuntimeError('loadtest integrity or freshness check failed')
        row = {'variant': name, 'case': case, 'round': iteration, 'topology': args.topology,
            'replicas': args.replicas,
            'binary_sha256': hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
            'cpu_seconds': sum(b['cpu_seconds']-a['cpu_seconds'] for a,b in zip(before,after)),
            'write_bytes': sum(b['io']['write_bytes']-a['io']['write_bytes'] for a,b in zip(before,after)),
            'resources_before': before, 'resources_after': after, 'load': report}
        (folder/'result.json').write_text(json.dumps(row, indent=2)+'\n')
        print(json.dumps({k: row[k] for k in ['variant','case','round','cpu_seconds','write_bytes']}), flush=True)
        return row
    finally:
        cluster.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--variant', action='append', required=True, help='label=/absolute/binary/path')
    parser.add_argument('--loadtest', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cases', nargs='+', choices=CASES, default=list(CASES))
    parser.add_argument('--topology', choices=['raft', 'sharded'], default='raft')
    parser.add_argument('--replicas', type=int, choices=[3, 5], default=3)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--seconds', type=int, default=20)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--entities', type=int, default=1002)
    parser.add_argument('--profile', action='store_true')
    args = parser.parse_args()
    if args.rounds < 1 or args.seconds < 1 or args.warmup < 0 or args.entities < 2:
        parser.error('rounds and seconds must be positive, warmup nonnegative, and entities at least 2')
    variants = [v.split('=', 1) for v in args.variant]
    if any(len(v) != 2 or not v[0] or not Path(v[1]).is_file() for v in variants):
        parser.error('each variant must be label=/absolute/binary/path pointing to an existing file')
    if len({v[0] for v in variants}) != len(variants):
        parser.error('variant labels must be unique')
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    for iteration in range(args.rounds):
        for case in args.cases:
            order = variants if iteration % 2 == 0 else list(reversed(variants))
            for name, binary in order:
                results.append(run(args, name, binary, case, iteration))
                (args.output/'results.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
