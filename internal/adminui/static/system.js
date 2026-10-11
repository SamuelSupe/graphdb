import { t } from './i18n.js';
import { tenant, settings, saveSettings, request, pathID, params } from './api.js';
import { e, button, field, tabs, table, badge, structuredView, link, notice, dialog, toast, confirmRequest } from './ui.js';

let clusterTab = 'shards';
let placementFilter = { endpoint: '', q: '', cursors: [''] };

export async function connectionPage(ctx) {
  const original = { ...settings };
  ctx.html(`<form id="connection-form" class="connection-form form-stack"><fieldset class="connection-group"><legend>${e(t('apiConnection'))}</legend>${notice(t('connectionHint'))}<div class="form-grid">${field('dataURL', settings.data, 'text', 'required')}${field('adminURL', settings.admin, 'text', 'required')}</div>${field('token', settings.token, 'password', 'autocomplete="off"')}</fieldset><details class="connection-cluster" ${settings.router || settings.raft || settings.clusterID ? 'open' : ''}><summary>${e(t('clusterConnection'))}<span class="muted">${e(t('optional'))}</span></summary><p class="hint">${e(t('clusterConnectionHint'))}</p><fieldset class="connection-group"><legend>${e(t('router'))}</legend><div class="form-grid">${field('routerURL', settings.router)}${field('routerToken', settings.routerToken, 'password', 'autocomplete="off"')}</div></fieldset><fieldset class="connection-group"><legend>${e(t('raft'))}</legend><div class="form-grid">${field('raftURL', settings.raft)}${field('raftToken', settings.raftToken, 'password', 'autocomplete="off"')}</div>${field('clusterID', settings.clusterID)}</fieldset></details><p class="hint">${e(t('credentialsHint'))}</p><div class="toolbar"><button type="submit" class="primary">${e(t('connect'))}</button>${button('forget', 'forget')}${button('apiReference', 'reference')}</div><p class="form-error" role="alert"></p></form>`);
  const form = ctx.root.querySelector('form');
  const values = () => { const fields = Object.fromEntries(new FormData(form)); return { data: fields.dataURL.trim(), admin: fields.adminURL.trim(), router: fields.routerURL.trim(), raft: fields.raftURL.trim(), token: fields.token, routerToken: fields.routerToken, raftToken: fields.raftToken, clusterID: fields.clusterID.trim() }; };
  ctx.dirty = () => Object.entries(values()).some(([key, value]) => value !== original[key]);
  form.onsubmit = async event => {
    event.preventDefault();
    const submit = form.querySelector('[type=submit]'); submit.disabled = true;
    try {
      saveSettings(values());
      const results = await Promise.allSettled([request('data', '/v1/health', { tenantID: '' }), request('admin', '/v1/health', { tenantID: '' }), request('admin', '/v1/tenants?limit=1', { tenantID: '' })]);
      const failures = results.filter(result => result.status === 'rejected');
      ctx.dirty = () => false;
      ctx.connection(failures.length ? 'disconnected' : 'connected');
      if (failures.length) { form.querySelector('.form-error').textContent = failures.map(result => result.reason.message).join('\n'); toast(t('requestFailed'), true); }
      else { await ctx.reloadTenants(); toast(t('connected')); await ctx.refresh(); }
    } catch (error) { form.querySelector('.form-error').textContent = error.message; }
    finally { submit.disabled = false; }
  };
  ctx.action = async action => {
    if (action === 'reference') ctx.result(await ctx.get('admin', '/openapi.yaml', { raw: true }));
    if (action === 'forget') {
      settings.token = ''; settings.routerToken = ''; settings.raftToken = '';
      for (const key of ['token', 'routerToken', 'raftToken']) form.elements.namedItem(key).value = '';
      toast(t('noCredentials'));
    }
  };
}

export async function clusterPage(ctx) {
  if (placementFilter.endpoint !== settings.router) placementFilter = { endpoint: settings.router, q: '', cursors: [''] };
  let state, shards = [], placements = [], sequence = 0;
  const load = async (candidate = placementFilter) => {
    const current = ++sequence;
    const needsRaft = clusterTab === 'raft';
    if (!(needsRaft ? settings.raft : settings.router)) {
      ctx.html(tabs(['shards', 'placements', 'raft', 'router'], clusterTab) + notice(t('clusterHint')) + button('connection', 'navigate:connection', 'primary'));
      return;
    }
    if (needsRaft) {
      const result = await ctx.get('raft', '/raft/status');
      if (!ctx.valid() || current !== sequence) return;
      state = result;
      ctx.html(`${tabs(['shards', 'placements', 'raft', 'router'], clusterTab)}<div class="toolbar">${button('upgrade', 'upgrade')}${button('member', 'member')}${button('transfer', 'transfer')}${button('barrier', 'barrier')}${button('drain', 'raft-drain')}${button('resume', 'raft-resume')}</div>${structuredView(state)}`);
    } else if (clusterTab === 'router') {
      const result = await ctx.get('router', '/v1/diagnostics');
      if (!ctx.valid() || current !== sequence) return;
      ctx.html(tabs(['shards', 'placements', 'raft', 'router'], clusterTab) + `<div class="toolbar">${button('drain', 'router-drain')}${button('resume', 'router-resume')}${button('metrics', 'router-metrics')}</div>${structuredView(result)}`);
    } else {
      const result = await ctx.get('router', '/v1/cluster' + params(clusterTab === 'placements' ? { limit: 25, q: candidate.q, cursor: candidate.cursors.at(-1) } : { limit: 1 }));
      if (!ctx.valid() || current !== sequence) return;
      state = result; placementFilter = candidate;
      shards = Object.values(state.shards || {}); placements = Object.values(state.tenants || {}).sort((a, b) => a.tenant_id < b.tenant_id ? -1 : a.tenant_id > b.tenant_id ? 1 : 0);
      ctx.html(`${tabs(['shards', 'placements', 'raft', 'router'], clusterTab)}<div class="toolbar">${clusterTab === 'shards' ? button('registerShard', 'register', 'primary') : button('assign', 'assign', 'primary') + button('move', 'move')}<span class="muted">${e(t('version'))} ${e(state.version)}</span></div>${clusterTab === 'shards' ? table([['shardID', row => link(row.id, 'inspect-shard:' + shards.indexOf(row))], ['clusterID', row => e(row.cluster_id)], ['status', row => badge(row.draining ? 'disabled' : 'active')], ['peers', row => Object.values(row.peers || {}).map(value => `<div class="code">${e(value)}</div>`).join('')], ['actions', row => `<div class="row-actions">${button(row.draining ? 'resume' : 'drain', (row.draining ? 'resume:' : 'drain:') + shards.indexOf(row), 'small')}${button('unregister', 'unregister:' + shards.indexOf(row), 'small')}</div>`]], shards) : `<form class="toolbar tenant-filters" id="placement-filter">${field('tenantSearch', placementFilter.q, 'search', 'maxlength="128"')}<button type="submit" class="primary">${e(t('search'))}</button></form>` + table([['tenantID', row => e(row.tenant_id)], ['shardID', row => e(row.shard_id)], ['epoch', row => e(row.epoch)], ['status', row => badge(row.state)], ['phase', row => e(row.move?.phase)], ['actions', (row, index) => button('inspect', 'inspect-placement:' + index, 'small') + ' ' + (row.move ? button('cancelMove', 'cancel-move:' + index, 'small') : '')]], placements) + `<div class="pagination">${button('previous', 'placement-previous', '', placementFilter.cursors.length === 1 ? 'disabled' : '')}${button('next', 'placement-next', '', state.next_cursor ? '' : 'disabled')}<span class="muted">${e(t('page'))} ${placementFilter.cursors.length} · ${e(t('shown'))} ${placements.length} / ${state.total} ${e(t('records'))}</span></div>`}`);
      const form = ctx.root.querySelector('#placement-filter');
      if (form) form.onsubmit = async event => {
        event.preventDefault();
        try { await load({ ...placementFilter, q: form.elements.namedItem('tenantSearch').value.trim(), cursors: [''] }); }
        catch (error) { if (ctx.valid()) toast(error.message, true); }
      };
    }
  };
  ctx.action = async action => {
    if (action === 'placement-next' && state.next_cursor) { await load({ ...placementFilter, cursors: [...placementFilter.cursors, state.next_cursor] }); return; }
    if (action === 'placement-previous' && placementFilter.cursors.length > 1) { await load({ ...placementFilter, cursors: placementFilter.cursors.slice(0, -1) }); return; }
    if (action.startsWith('tab:')) { clusterTab = action.slice(4); await load(); return; }
    if (action.startsWith('inspect-shard:')) dialog('details', structuredView(shards[Number(action.slice(14))]), async () => {}, 'close');
    else if (action.startsWith('inspect-placement:')) dialog('details', structuredView(placements[Number(action.slice(18))]), async () => {}, 'close');
    else if (action === 'register') await confirmRequest(ctx, 'registerShard', 'router', '/v1/cluster/shards', { id: '', cluster_id: '', peers: { '1': 'http://node1:8081', '2': 'http://node2:8081', '3': 'http://node3:8081' } }, { after: load });
    else if (action === 'assign' || action === 'move') await confirmRequest(ctx, action, 'router', action === 'assign' ? '/v1/cluster/placements' : '/v1/cluster/moves', { tenant_id: tenant, target: '' }, { destructive: action === 'move', after: load });
    else if (action.startsWith('cancel-move:')) { const row = placements[Number(action.slice(12))]; await confirmRequest(ctx, 'cancelMove', 'router', '/v1/cluster/moves/' + pathID(row.tenant_id) + '/cancel', undefined, { resource: row.tenant_id, destructive: true, after: load }); }
    else if (['drain', 'resume', 'unregister'].some(value => action.startsWith(value + ':'))) {
      const [operation, index] = action.split(':'), row = shards[Number(index)];
      await confirmRequest(ctx, operation, 'router', '/v1/cluster/shards/' + pathID(row.id) + '/' + operation, undefined, { destructive: operation !== 'resume', resource: row.id, after: load });
    } else if (action === 'upgrade') ctx.result(await ctx.get('raft', '/raft/upgrade'));
    else if (action === 'member') await confirmRequest(ctx, 'member', 'raft', '/raft/members', { action: 'add_learner', id: 4, url: 'http://node4:8081' }, { destructive: true, resource: settings.clusterID, after: load });
    else if (action === 'transfer') await confirmRequest(ctx, 'transfer', 'raft', '/raft/transfer', { target: 0 }, { destructive: true, resource: settings.clusterID, after: load });
    else if (action === 'barrier') await confirmRequest(ctx, 'barrier', 'raft', '/raft/upgrade/barrier', undefined, { after: load });
    else if (action.startsWith('raft-')) await confirmRequest(ctx, action.slice(5), 'raft', '/raft/' + action.slice(5), undefined, { destructive: action === 'raft-drain', resource: settings.clusterID, after: load });
    else if (action === 'router-metrics') ctx.result(await ctx.get('router', '/metrics', { raw: true }));
    else if (action === 'router-drain' || action === 'router-resume') await confirmRequest(ctx, action.slice(7), 'router', '/v1/router/' + action.slice(7), undefined, { destructive: action === 'router-drain', resource: 'router', after: load });
  };
  await load();
}
