#!/usr/bin/env python3
"""Serial rolling upgrades using private Raft endpoints and explicit restart argv.

An inventory contains groups[{cluster_id, nodes[{id, url, restart}]}]. Tokens are
read from GRAPHDB_RAFT_TOKEN, or each group's token_env. Run without --execute
to inspect readiness. Each group specifies protocol_version (default 1);
the selected version pair must be qualified before executing an upgrade.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request


def request(group, node, path, method='GET'):
    token = os.environ[group.get('token_env', 'GRAPHDB_RAFT_TOKEN')]
    req = urllib.request.Request(node['url'].rstrip('/')+path, method=method,
        data=b'' if method == 'POST' else None,
        headers={'Authorization': 'Bearer '+token, 'X-Raft-Cluster': group['cluster_id']})
    with urllib.request.urlopen(req, timeout=35) as response:
        return json.loads(response.read() or b'{}')


def status(group, node):
    value = request(group, node, '/raft/status')
    if value.get('node_id') != node['id'] or value.get('cluster_id') != group['cluster_id']:
        raise RuntimeError(f"wrong node identity: {group['cluster_id']}/{node['id']}")
    if value.get('protocol_version') != group.get('protocol_version', 1) or value.get('allow_legacy_protocol'):
        raise RuntimeError('requires the inventory protocol with the legacy bridge disabled')
    return value


def wait_group(group, timeout):
    deadline = time.monotonic()+timeout
    reason = 'no reachable replica'
    while time.monotonic() < deadline:
        for node in group['nodes']:
            try:
                report = request(group, node, '/raft/upgrade')
                if report.get('safe_to_restart'):
                    reported = {peer['node_id'] for peer in report['members']}
                    expected = {peer['id'] for peer in group['nodes']}
                    if reported != expected:
                        raise RuntimeError('inventory does not contain exactly the voting members')
                    for peer in group['nodes']:
                        value = status(group, peer)
                        if value.get('error') or value.get('snapshot_error') or value.get('draining'):
                            raise RuntimeError('a voting member is failed or draining')
                    return report
                reason = report.get('reason', reason)
            except (OSError, urllib.error.URLError, TimeoutError) as err:
                reason = str(err)
        time.sleep(.2)
    raise RuntimeError(f"group {group['cluster_id']} not ready: {reason}")


def upgrade(inventory, execute, target_version, target_commit, timeout, emit, force=False):
    for group in inventory['groups']:
        if group.get('protocol_version', 1) not in (1, 2, 3):
            parser.error('each group protocol_version must be 1, 2 or 3')
        report = wait_group(group, timeout)
        emit(group['cluster_id'], 'preflight', leader_id=report['leader_id'])
    router_group = {'cluster_id': '', 'token_env': inventory.get('router_token_env', 'GRAPHDB_RAFT_TOKEN')}
    for router in inventory.get('routers', []):
        health = request(router_group, router, '/v1/health')
        if health.get('deployment') != 'sharded_raft' or not health.get('build') or health.get('draining'):
            raise RuntimeError('router must support rolling maintenance and be healthy')
        request(router_group, router, '/v1/readiness')
    if not execute:
        return
    for group in inventory['groups']:
        report = wait_group(group, timeout)
        # Each drain rechecks the group and hands off if this node became leader.
        ordered = sorted(group['nodes'], key=lambda node: node['id'] == report['leader_id'])
        for node in ordered:
            current = status(group, node)
            if (not force and current['build']['version'] == target_version and
                    (not target_commit or current['build']['commit'] == target_commit)):
                emit(group['cluster_id'], 'already_upgraded', node_id=node['id'])
                continue
            wait_group(group, timeout)
            request(group, node, '/raft/drain', 'POST')
            emit(group['cluster_id'], 'drained', node_id=node['id'])
            time.sleep(inventory.get('replica_drain_seconds', 7))
            # The command replaces only this process and keeps its directories.
            subprocess.run(node['restart'], check=True, timeout=timeout)
            deadline = time.monotonic()+timeout
            while True:
                try:
                    current = status(group, node)
                    if (current['build']['version'] == target_version and
                            (not target_commit or current['build']['commit'] == target_commit) and
                            not current.get('draining') and not current.get('error') and
                            not current.get('snapshot_error') and current['applied_index'] >= current['commit_index']):
                        break
                except (OSError, urllib.error.URLError, TimeoutError):
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"replica {node['id']} did not rejoin with the target build; upgrade stopped")
                time.sleep(.2)
            wait_group(group, timeout)
            emit(group['cluster_id'], 'rejoined', node_id=node['id'], build=current['build'])
    for router in inventory.get('routers', []):
        current = request(router_group, router, '/v1/health')
        if not force and current['build']['version'] == target_version and (not target_commit or current['build']['commit'] == target_commit):
            emit('', 'already_upgraded', router_id=router['id'])
            continue
        for peer in inventory['routers']:
            if peer['id'] != router['id']:
                request(router_group, peer, '/v1/readiness')
        request(router_group, router, '/v1/router/drain', 'POST')
        emit('', 'router_drained', router_id=router['id'])
        time.sleep(inventory.get('router_drain_seconds', 7))
        subprocess.run(router['restart'], check=True, timeout=timeout)
        deadline = time.monotonic()+timeout
        while True:
            try:
                current = request(router_group, router, '/v1/health')
                if (current['build']['version'] == target_version and (not target_commit or current['build']['commit'] == target_commit)
                        and not current.get('draining')):
                    request(router_group, router, '/v1/readiness')
                    break
            except (OSError, urllib.error.URLError, TimeoutError):
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError('router failed to rejoin; upgrade stopped')
            time.sleep(.2)
        emit('', 'router_rejoined', router_id=router['id'], build=current['build'])
    emit('', 'complete')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--target-version')
    parser.add_argument('--target-commit')
    parser.add_argument('--force', action='store_true', help='also restart processes already on the target build')
    parser.add_argument('--timeout', type=float, default=180)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.execute and not args.target_version:
        parser.error('--execute requires --target-version')
    inventory = json.loads(args.inventory.read_text())
    seen = set()
    if inventory.get('routers') and len(inventory['routers']) < 2:
        parser.error('router upgrades require at least two routers behind a load balancer')
    for group in inventory['groups']:
        if group['cluster_id'] in seen:
            parser.error('duplicate group')
        seen.add(group['cluster_id'])
        ids = [node['id'] for node in group['nodes']]
        if len(ids) < 3 or len(ids) != len(set(ids)):
            parser.error('each group needs at least three distinct voting nodes')
        for node in group['nodes']:
            restart = node.get('restart')
            if args.execute and (not isinstance(restart, list) or not restart or
                    not all(isinstance(arg, str) for arg in restart)):
                parser.error('restart must be an argv array')
    for router in inventory.get('routers', []):
        if args.execute and (not isinstance(router.get('restart'), list) or not router['restart'] or
                not all(isinstance(arg, str) for arg in router['restart'])):
            parser.error('router restart must be an argv array')
    events = []

    def emit(group, stage, **details):
        row = {'group': group, 'stage': stage, **details}
        events.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
        if args.report:
            args.report.write_text(json.dumps(events, ensure_ascii=False, indent=2)+'\n')

    # This guards coordinators using the same inventory on one host. Operations
    # must also serialize coordinators across different hosts and inventories.
    with args.inventory.with_suffix(args.inventory.suffix+'.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error('another coordinator is using this inventory')
        try:
            upgrade(inventory, args.execute, args.target_version, args.target_commit, args.timeout, emit, args.force)
        except Exception as err:
            emit('', 'failed', error=str(err))
            raise SystemExit(1)


if __name__ == '__main__':
    main()
