import { objectEditor } from './forms.js';
import { t } from './i18n.js';
import { tenant, setTenant, settings, request, pathID, params } from './api.js';
import { e, button, field, select, checkbox, table, badge, structuredView, link, notice, dialog, toast, objectInput, confirmRequest } from './ui.js';

let state = { endpoint: '' };

export async function tenantsPage(ctx) {
  const key = settings.admin;
  if (state.endpoint !== key) state = { endpoint: key, q: '', limit: 25, include_legacy: false, cursors: [''] };
  let rows = [], next = '', sequence = 0;
  const load = async (candidate = state) => {
    const current = ++sequence;
    ctx.root.classList?.add('busy');
    try {
      const result = await ctx.get('admin', '/v1/tenants' + params({ limit: candidate.limit, q: candidate.q, include_legacy: candidate.include_legacy || '', cursor: candidate.cursors.at(-1) }), { tenantID: '' });
      if (!ctx.valid() || current !== sequence) return;
      state = candidate; rows = result.tenants || []; next = result.next_cursor || '';
      ctx.html(`<div class="toolbar">${button('createTenant', 'create', 'primary')}</div><form class="toolbar tenant-filters" id="tenant-filters">${field('tenantSearch', state.q, 'search', 'maxlength="128" autocomplete="off"')}${checkbox('includeLegacy', state.include_legacy)}<button class="primary" type="submit">${e(t('search'))}</button></form>${state.include_legacy ? notice(t('legacyTenantHint')) : ''}${table([['tenantID', (row, index) => link(row.tenant_id, 'select:' + index)], ['name', row => e(row.name)], ['status', row => badge(row.status)], ['version', row => e(row.manifest_version)], ['actions', (row, index) => `<div class="row-actions">${button('inspect', 'inspect:' + index, 'small')}${button('edit', 'edit:' + index, 'small')}<details class="action-menu"><summary>${e(t('moreActions'))}</summary><div>${button(row.status === 'active' ? 'disable' : 'enable', (row.status === 'active' ? 'disable:' : 'enable:') + index, 'small')}${button('clone', 'clone:' + index, 'small')}${button('softDelete', 'delete:' + index, 'small')}${button('purge', 'purge:' + index, 'small danger-outline')}</div></details></div>`]], rows)}<div class="pagination">${button('previous', 'previous', '', state.cursors.length === 1 ? 'disabled' : '')}${button('next', 'next', '', next ? '' : 'disabled')}<span class="muted">${e(t('page'))} ${state.cursors.length} · ${e(t('shown'))} ${rows.length} / ${result.total} ${e(t('records'))}</span>${select('limit', [25, 50, 100].map(value => [String(value), String(value)]), String(state.limit))}</div>`);
      const form = ctx.root.querySelector('#tenant-filters');
      form.onsubmit = async event => {
        event.preventDefault();
        try { await load({ ...state, q: form.elements.namedItem('tenantSearch').value.trim(), include_legacy: form.elements.namedItem('includeLegacy').checked, cursors: [''] }); }
        catch (error) { if (ctx.valid()) toast(error.message, true); }
      };
      ctx.root.querySelector('[name="limit"]').onchange = async event => {
        try { await load({ ...state, limit: Number(event.target.value), cursors: [''] }); }
        catch (error) { if (ctx.valid()) toast(error.message, true); }
      };
    } finally { if (ctx.valid() && current === sequence) ctx.root.classList?.remove('busy'); }
  };
  const after = async () => { state = { ...state, cursors: [''] }; await ctx.reloadTenants(); if (ctx.valid()) await ctx.refresh(); };
  ctx.action = async action => {
    if (action === 'next' && next) { await load({ ...state, cursors: [...state.cursors, next] }); return; }
    if (action === 'previous' && state.cursors.length > 1) { await load({ ...state, cursors: state.cursors.slice(0, -1) }); return; }
    const [operation, index] = action.split(':'), row = rows[Number(index)], id = row?.tenant_id;
    if (operation === 'select') { setTenant(id); await ctx.refresh(); return; }
    if (operation === 'inspect') { const current = sequence; const info = await ctx.get('admin', '/v1/tenants/' + pathID(id), { tenantID: id }); if (!ctx.valid() || current !== sequence) return; dialog('details', structuredView(info), async () => {}, 'close'); return; }
    if (operation === 'create' || operation === 'edit') {
      const value = row || { tenant_id: '', name: '', description: '', labels: {}, metadata: {} };
      dialog(operation === 'create' ? 'createTenant' : 'editTenant', `<div class="form-grid">${field('tenantID', value.tenant_id, 'text', operation === 'create' ? 'required' : 'readonly')}${field('name', value.name || '')}</div>${field('description', value.description || '')}${objectEditor('labels', { labels: value.labels || {} }, '')}${objectEditor('metadata', { metadata: value.metadata || {} }, '')}`, async form => {
        const input = Object.fromEntries(new FormData(form)), tenantID = input.tenantID.trim();
        const body = { name: input.name, description: input.description, labels: objectInput(form, 'labels').labels, metadata: objectInput(form, 'metadata').metadata };
        if (operation === 'create') body.tenant_id = tenantID;
        await request('admin', '/v1/tenants' + (operation === 'create' ? '' : '/' + pathID(id)), { method: operation === 'create' ? 'POST' : 'PUT', body, tenantID: operation === 'create' ? '' : id });
        if (!tenant) setTenant(tenantID);
        toast(t('success')); await after();
      }, operation === 'create' ? 'create' : 'save');
    } else if (operation === 'clone') await confirmRequest(ctx, 'clone', 'admin', '/v1/tenants/' + pathID(id) + '/clone', { target_tenant_id: '', name: '' }, { tenantID: id, after });
    else if (['enable', 'disable', 'delete', 'purge'].includes(operation)) {
      const path = '/v1/tenants/' + pathID(id) + (operation === 'delete' ? '' : '/' + operation);
      await confirmRequest(ctx, operation === 'delete' ? 'softDelete' : operation, 'admin', path, undefined, { method: operation === 'delete' ? 'DELETE' : 'POST', tenantID: id, destructive: operation !== 'enable', resource: id, after: async () => { if (operation === 'purge' && tenant === id) setTenant(''); await after(); } });
    }
  };
  await load();
}
