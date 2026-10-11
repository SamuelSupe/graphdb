import { t } from './i18n.js';
import { tenant, request, params } from './api.js';
import { e, button, field, badge, dialog, bindActions, toast } from './ui.js';

// Fetch pages only while the picker is open; a selected ID never depends on the
// current result page. Closing aborts requests and prevents late results.
export function openTenantPicker(onSelect) {
  dialog('selectTenant', `<div class="tenant-search">${field('tenantSearch', '', 'search', 'maxlength="128" autocomplete="off"')}${button('search', 'search', 'primary')}</div><p class="hint">${e(t('tenantSearchHint'))}</p><div id="tenant-matches" aria-live="polite"></div>`, async () => {}, 'close');
  const target = document.querySelector('#dialog'), root = target.querySelector('#tenant-matches');
  const input = target.querySelector('[name="tenantSearch"]');
  let controller, sequence = 0, query = '', history = [''], next = '';
  target.addEventListener('close', () => { sequence++; controller?.abort(); }, { once: true });
  const load = async () => {
    controller?.abort(); controller = new AbortController();
    const current = ++sequence;
    root.setAttribute('aria-busy', 'true');
    root.innerHTML = `<p class="muted">${e(t('loading'))}</p>`;
    try {
      const result = (await request('admin', '/v1/tenants' + params({ limit: 20, q: query, cursor: history.at(-1) }), { tenantID: '', signal: controller.signal })).data;
      if (current !== sequence || !target.open) return;
      next = result.next_cursor || '';
      root.innerHTML = `<div class="tenant-matches">${result.tenants.map(row => `<button type="button" class="tenant-match ${row.tenant_id === tenant ? 'selected' : ''}" data-tenant-id="${e(row.tenant_id)}"><span><strong>${e(row.name || row.tenant_id)}</strong><span class="code">${e(row.tenant_id)}</span></span>${badge(row.status)}</button>`).join('') || `<p class="empty">${e(t('noTenants'))}</p>`}</div><div class="pagination">${button('previous', 'previous', '', history.length === 1 ? 'disabled' : '')}${button('next', 'next', '', next ? '' : 'disabled')}<span class="muted">${result.total} ${e(t('records'))}</span></div>`;
    } catch (error) {
      if (current === sequence && target.open && error.name !== 'AbortError') root.innerHTML = `<p class="form-error">${e(error.message)}</p>${button('retry', 'retry')}`;
    } finally { if (current === sequence) root.removeAttribute('aria-busy'); }
  };
  bindActions(target.querySelector('.dialog-content'), async action => {
    if (action === 'search') { query = input.value.trim(); history = ['']; }
    if (action === 'next' && next) history.push(next);
    if (action === 'previous' && history.length > 1) history.pop();
    await load();
  });
  root.addEventListener('click', async event => {
    const item = event.target.closest('[data-tenant-id]');
    if (!item) return;
    // Wait for this dialog's queued close event before another confirmation
    // reuses it; otherwise that event could cancel the new confirmation.
    const closed = new Promise(resolve => target.addEventListener('close', resolve, { once: true }));
    target.close(); await closed;
    try { await onSelect(item.dataset.tenantId); } catch (error) { toast(error.message, true); }
  });
  input.onkeydown = event => { if (event.key === 'Enter') { event.preventDefault(); target.querySelector('[data-action="search"]').click(); } };
  input.focus();
  void load();
}
