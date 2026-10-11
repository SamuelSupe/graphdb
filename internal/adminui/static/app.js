import { t, language, setLanguage } from './i18n.js';
import { tenant, setTenant, settings, request } from './api.js';
import { e, button, bindActions, dialog, structuredView } from './ui.js';
import { bindObjectEditors, captureEditors } from './forms.js';
import { dataPage, queryPage, schemaPage } from './data.js';
import { indexesPage, tasksPage, governancePage, ingestionPage } from './operations.js';
import { overviewPage } from './overview.js';
import { connectionPage, clusterPage } from './system.js';
import { openTenantPicker } from './tenant-picker.js';
import { tenantsPage } from './tenants.js';

const groups = { workspace: ['overview', 'data', 'query', 'schema'], operations: ['tenants', 'indexes', 'tasks', 'ingestion', 'governance'], infrastructure: ['cluster', 'connection'] };
const pages = { overview: overviewPage, data: dataPage, query: queryPage, schema: schemaPage, tenants: tenantsPage, indexes: indexesPage, tasks: tasksPage, ingestion: ingestionPage, governance: governancePage, cluster: clusterPage, connection: connectionPage };
const paths = { overview: 'M3 10l9-7 9 7M5 9v12h14V9M9 21v-7h6v7', data: 'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6M4 12c0 4 16 4 16 0', query: 'M8 5l-6 7 6 7M16 5l6 7-6 7M14 3l-4 18', schema: 'M12 5v7M12 12l-7 6M12 12l7 6M9 3h6v4H9zM2 17h6v4H2zM16 17h6v4h-6z', tenants: 'M8 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8M2 22v-3c0-6 12-6 12 0v3M16 5c5-3 8 6 2 7M17 15c4 0 5 3 5 7', indexes: 'M4 5h16v14H4zM8 9h8M8 13h8M8 17h5', tasks: 'M5 3h10l4 4v14H5zM14 3v5h5M8 12h8M8 16h6', ingestion: 'M12 3v13M7 8l5-5 5 5M4 14v7h16v-7', governance: 'M12 2l9 4v7c0 4-4 8-9 10-5-2-9-6-9-10V6zM7 12l3 3 7-7', cluster: 'M3 3h18v7H3zM3 14h18v7H3zM7 6h1M7 17h1M12 6h5M12 17h5', connection: 'M10 14l4-4M8 16l-1 1c-5 5-10-1-5-5l5-5c3-3 5-1 6 0M16 8l1-1c5-5 10 1 5 5l-5 5c-3 3-5 1-6 0' };
const icon = key => `<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${paths[key]}"/></svg>`;
let currentPage = '', ctx, controller, generation = 0, connectionStatus = 'notChecked';
let tenantError = '';

async function allowLeave() {
  if (!ctx?.dirty?.()) return true;
  return new Promise(resolve => {
    dialog('discard', `<p>${e(t('discardDesc'))}</p>`, async () => resolve(true), 'leave');
    document.querySelector('#dialog').addEventListener('close', () => resolve(false), { once: true });
  });
}

function shell(page) {
  const group = Object.keys(groups).find(key => groups[key].includes(page));
  return `<div class="shell"><aside class="sidebar"><div class="brand"><svg viewBox="0 0 40 44" aria-hidden="true"><path d="M9 24L29 37V9" fill="none" stroke="currentColor" stroke-width="2"/><circle cx="9" cy="24" r="6" fill="currentColor"/><circle cx="29" cy="37" r="6" fill="currentColor"/><circle cx="29" cy="9" r="6" fill="currentColor"/></svg><div><strong>GGraphDB</strong><span>${e(t('console'))}</span></div></div><nav class="nav" aria-label="${e(t('console'))}">${Object.entries(groups).map(([name, entries]) => `<div class="nav-group"><p class="nav-label">${e(t(name))}</p>${entries.map(key => `<button type="button" class="${key === page ? 'active' : ''}" data-action="navigate:${key}" ${key === page ? 'aria-current="page"' : ''}>${icon(key)}${e(t(key))}</button>`).join('')}</div>`).join('')}</nav><div class="sidebar-footer"><div class="language-switch" aria-label="Language / 语言"><span aria-hidden="true">◎</span><button type="button" data-action="language:en" aria-pressed="${language === 'en'}">English</button><span>/</span><button type="button" data-action="language:zh" aria-pressed="${language === 'zh'}">中文</button></div></div></aside><main class="main"><header class="topbar">${button('console', 'menu', 'menu-toggle', 'aria-expanded="false"')}<div class="breadcrumb">${e(t(group))}<span>/</span><strong>${e(t(page))}</strong></div><div class="tenant-picker"><span>${e(t('tenant'))}</span><button type="button" data-action="choose-tenant" aria-haspopup="dialog" aria-label="${e(t('tenant') + ': ' + (tenant || t('selectTenant')))}" title="${e(tenant)}"><span class="tenant-current">${e(tenant || t('selectTenant'))}</span><span aria-hidden="true">⌄</span></button></div></header><div class="page-header"><div><h1>${e(t(page))}</h1><p>${e(t(page + 'Desc'))}</p></div><div class="header-actions">${page === 'connection' ? '' : button('refresh', 'refresh')}</div></div><div class="content" id="content"><p class="muted">${e(t('loading'))}</p></div><footer class="footer-status"><span class="dot ${connectionStatus === 'connected' ? 'connected' : ''}"></span><span>${e(t(connectionStatus))}</span><span>·</span><span>${e(settings.admin)}</span></footer></main></div>`;
}

async function render(page, restore = null) {
  page = pages[page] ? page : 'overview';
  const scrollPosition = currentPage === page ? window.scrollY : 0;
  if (currentPage !== page) { document.querySelector('#toast').className = ''; document.querySelector('#toast').textContent = ''; }
  currentPage = page;
  controller?.abort();
  controller = new AbortController();
  const signal = controller.signal, tenantID = tenant;
  const id = ++generation;
  document.querySelector('#app').innerHTML = shell(page);
  const root = document.querySelector('#content');
  const current = ctx = {
    root, signal, page, dirty: () => false,
    valid: () => id === generation,
    html: html => { if (id === generation) { root.innerHTML = html; bindObjectEditors(root); } },
    header: html => { if (id === generation) document.querySelector('.header-actions').innerHTML = button('refresh', 'refresh') + html; },
    get: async (endpoint, path, options = {}) => {
      const result = await request(endpoint, path, { tenantID, ...options, signal });
      if (id === generation && ['data', 'admin'].includes(endpoint)) current.connection('connected');
      return result.data;
    },
    refresh: () => render(page),
    navigate: navigate,
    reloadTenants: loadTenants,
    connection: value => {
      connectionStatus = value;
      if (id === generation) document.querySelector('.footer-status').innerHTML = `<span class="dot ${value === 'connected' ? 'connected' : ''}"></span><span>${e(t(value))}</span><span>·</span><span>${e(settings.admin)}</span>`;
    },
    result: value => {
      if (id !== generation) return;
      let target = root.querySelector('.last-response');
      if (!target) { target = document.createElement('div'); target.className = 'last-response'; root.append(target); }
      target.innerHTML = `<section class="response-panel"><header><h2>${e(t('response'))}</h2>${button('close', 'clear-response', 'small')}</header>${structuredView(value)}</section>`;
    },
  };
  bindActions(document.querySelector('#app'), async action => {
    if (action === 'menu') {
      const open = document.querySelector('.shell').classList.toggle('nav-open');
      document.querySelector('.menu-toggle').setAttribute('aria-expanded', String(open));
    } else if (action === 'clear-response') root.querySelector('.last-response')?.remove();
    else if (action.startsWith('navigate:')) await navigate(action.slice(9));
    else if (action === 'refresh') { if (await allowLeave()) await render(page); }
    else if (action === 'choose-tenant') openTenantPicker(async value => { if (await allowLeave()) { setTenant(value); await render(page); } });
    else if (action.startsWith('language:')) {
      if (document.querySelector('#dialog').open) return;
      captureEditors(root);
      const snapshot = [...root.querySelectorAll('input[name], select[name], textarea[name]')].map(input => ({ name: input.name, value: input.value, checked: input.checked }));
      document.querySelector('#toast').className = '';
      document.querySelector('#toast').textContent = '';
      setLanguage(action.slice(9));
      await render(page, snapshot);
    } else {
      try { await current.action?.(action); }
      catch (error) {
        if (action.startsWith('tab:') && current.valid()) current.html(`<div class="error-panel"><h2>${e(t('requestFailed'))}</h2><p>${e(error.message)}</p><div class="row-actions">${button('refresh', 'refresh')}</div></div>`);
        throw error;
      }
    }
  });
  try {
    if (!tenant && ['data', 'query', 'schema', 'indexes', 'tasks', 'ingestion', 'governance'].includes(page)) {
      current.html(`<div class="empty"><h2>${e(t('tenantRequired'))}</h2><div class="toolbar">${button('createTenant', 'navigate:tenants', 'primary')}</div>${tenantError ? `<p class="form-error">${e(tenantError)}</p>` : ''}</div>`);
    } else {
      await pages[page](current);
      if (restore && current.valid()) for (const value of restore) {
        const input = [...root.querySelectorAll('[name]')].find(input => input.name === value.name);
        if (input && input.type !== 'file') { input.value = value.value; if (input.type === 'checkbox') input.checked = value.checked; }
      }
      if (restore && current.valid()) bindObjectEditors(root, true);
      if (current.valid()) window.scrollTo(0, scrollPosition);
    }
  } catch (error) {
    if (current.valid() && error.name !== 'AbortError') current.html(`<div class="error-panel"><h2>${e(t('requestFailed'))}</h2><p>${e(error.message)}</p><div class="row-actions">${button('refresh', 'refresh')}${button('connection', 'navigate:connection')}</div></div>`);
  }
}

async function loadTenants() {
  if (tenant) return;
  try {
    const rows = (await request('admin', '/v1/tenants?limit=1', { tenantID: '' })).data.tenants || [];
    tenantError = '';
    if (rows.length) setTenant(rows[0].tenant_id);
  } catch (error) { tenantError = error.message; }
}
async function navigate(page) {
  if (!(await allowLeave())) return;
  ctx.dirty = () => false;
  if (location.hash === '#' + page) await render(page);
  else location.hash = page;
}
window.addEventListener('hashchange', async () => {
  const page = location.hash.slice(1);
  if (!(await allowLeave())) { history.replaceState(null, '', '#' + currentPage); return; }
  await render(page);
});
window.addEventListener('beforeunload', event => { if (ctx?.dirty?.()) { event.preventDefault(); event.returnValue = ''; } });
window.addEventListener('keydown', event => {
  if (event.key === 'Escape') {
    document.querySelector('.shell')?.classList.remove('nav-open');
    document.querySelector('.menu-toggle')?.setAttribute('aria-expanded', 'false');
  }
  if (event.key === 'Enter' && event.target.matches('.graph-node')) event.target.dispatchEvent(new MouseEvent('click', { bubbles: true }));
});
await loadTenants();
await render(location.hash.slice(1));
