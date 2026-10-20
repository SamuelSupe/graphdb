import { objectEditor } from './forms.js';
import { t } from './i18n.js';
import { tenant, request, pathID, params, json, parseJSON } from './api.js';
import { e, button, field, select, checkbox, tabs, table, badge, structuredView, taskView, dateView, notice, dialog, toast, objectInput, confirmRequest } from './ui.js';

let tasksTab = 'tasks', governanceTab = 'sourcePolicy', ingestionTab = 'commit';
const ingestionDrafts = new Map();

export async function indexesPage(ctx) {
  let rows;
  const load = async () => {
    rows = (await ctx.get('admin', '/v1/indexes/definitions')).indexes || [];
    ctx.html(`<div class="toolbar">${button('newIndex', 'create', 'primary')}${button('rebuild', 'rebuild')}${button('indexHealth', 'health')}${button('indexCatalog', 'catalog')}</div>${table([['name', row => e(row.name)], ['kind', row => e(row.kind)], ['field', row => e(row.field)], ['updated', row => dateView(row.updated_at)], ['actions', row => button('inspect', 'inspect:' + rows.indexOf(row), 'small') + ' ' + button('remove', 'delete:' + rows.indexOf(row), 'small')]], rows)}`);
  };
  ctx.action = async action => {
    if (action === 'create') dialog('newIndex', `<div class="form-grid">${field('kind', '', 'text', 'required')}${field('field', '', 'text', 'required')}</div>${field('name')}${checkbox('unique')}`, async form => {
      const values = Object.fromEntries(new FormData(form));
      await request('admin', '/v1/indexes', { method: 'POST', body: { ...values, unique: form.elements.namedItem('unique').checked } }); toast(t('accepted')); await load();
    }, 'create');
    else if (action === 'rebuild') await confirmRequest(ctx, 'rebuild', 'admin', '/v1/indexes/rebuild?async=true', undefined, { after: load });
    else if (action === 'health' || action === 'catalog') { const result = await ctx.get('admin', action === 'health' ? '/v1/indexes/health' : '/v1/indexes'); ctx.result(result); }
    else if (action.startsWith('inspect:')) dialog('details', structuredView(rows[Number(action.slice(8))]), async () => {}, 'close');
    else if (action.startsWith('delete:')) { const row = rows[Number(action.slice(7))]; await confirmRequest(ctx, 'remove', 'admin', '/v1/indexes/definitions/' + pathID(row.name), undefined, { method: 'DELETE', destructive: true, resource: row.name, after: load }); }
  };
  await load();
}

export async function tasksPage(ctx) {
  let rows = [], selectedTask, filters = { type: '', status: '', limit: 100 };
  let refreshTimer, loadSequence = 0;
  ctx.signal.addEventListener('abort', () => clearTimeout(refreshTimer), { once: true });
  const taskTypes = ['compact', 'gc', 'repair', 'export_snapshot', 'replay_deadletters', 'index_rebuild', 'tenant_backup', 'tenant_restore', 'tenant_restore_drill'];
  function drawTasks() {
    clearTimeout(refreshTimer);
    ctx.html(`<div class="${selectedTask ? 'split' : ''}"><section class="workbench">${tabs(['tasks', 'maintenance', 'automation'], tasksTab)}<div class="toolbar">${button('newTask', 'create', 'primary')}${button('backup', 'backup')}${button('restore', 'restore')}${button('restoreDrill', 'drill')}</div><form class="toolbar" id="task-filter">${select('type', [['', t('type')], ...taskTypes.map(value => [value, t(value)])], filters.type)}${select('status', [['', t('status')], ...['queued', 'running', 'succeeded', 'failed', 'canceled'].map(value => [value, t(value)])], filters.status)}<button type="submit">${e(t('search'))}</button></form>${table([['id', (row, index) => `<button type="button" class="text-button code" title="${e(row.id)}" data-action="inspect:${index}">${e(row.id.length > 20 ? row.id.slice(0, 16) + '…' : row.id)}</button>`], ['type', row => e(t(row.type))], ['status', row => badge(row.status)], ['updated', row => dateView(row.updated_at)], ['phase', row => e(row.phase)]], rows, selectedTask)}</section>${selectedTask ? `<aside class="inspector">${taskView(selectedTask)}<div class="row-actions">${['queued', 'running'].includes(selectedTask.status) ? button('cancelTask', 'cancel:' + rows.indexOf(selectedTask)) : ''}${['failed', 'canceled'].includes(selectedTask.status) ? button('retry', 'retry:' + rows.indexOf(selectedTask)) : ''}</div></aside>` : ''}</div>`);
    const form = ctx.root.querySelector('#task-filter');
    form.onsubmit = async event => { event.preventDefault(); filters = { ...filters, ...Object.fromEntries(new FormData(form)) }; try { await load(); } catch (error) { toast(error.message, true); } };
    if (rows.some(row => ['queued', 'running'].includes(row.status))) refreshTimer = setTimeout(async () => {
      if (!ctx.valid()) return;
      try { await load(); } catch (error) { if (ctx.valid()) toast(error.message, true); }
    }, 5000);
  }
  const load = async () => {
    clearTimeout(refreshTimer);
    const sequence = ++loadSequence;
    if (tasksTab === 'tasks') {
      const result = await ctx.get('admin', '/v1/tasks' + params(filters));
      if (!ctx.valid() || sequence !== loadSequence) return;
      rows = result.tasks || [];
      selectedTask = rows.find(row => row.id === selectedTask?.id) || rows[0];
      drawTasks();
    } else if (tasksTab === 'automation') {
      const result = await ctx.get('data', '/v1/backup-automation');
      if (!ctx.valid() || sequence !== loadSequence) return;
      ctx.html(tabs(['tasks', 'maintenance', 'automation'], tasksTab) + `<div class="toolbar">${button('resetAutomation', 'reset-automation')}</div>${structuredView(result)}`);
    } else ctx.html(`${tabs(['tasks', 'maintenance', 'automation'], tasksTab)}${notice(t('confirmDesc'))}<div class="toolbar">${button('audit', 'audit')}${button('recover', 'recover')}${button('repair', 'repair')}${button('cleanupCommits', 'cleanup')}${button('gc', 'gc')}</div>`);
  };
  ctx.action = async action => {
    if (action.startsWith('tab:')) { tasksTab = action.slice(4); await load(); }
    else if (action === 'create') dialog('newTask', select('taskType', taskTypes.map(value => [value, t(value)])) + objectEditor('params', {}, 'taskParams') + notice(t('confirmDesc')) + field('typeConfirm', '', 'text', 'required') + `<p class="hint code">${e(tenant)}</p>`, async form => {
      if (form.elements.namedItem('typeConfirm').value !== tenant) throw new Error(t('confirmMismatch'));
      const type = form.elements.namedItem('taskType').value, taskParams = objectInput(form, 'params');
      const result = await request('admin', '/v1/tasks', { method: 'POST', body: { type, params: taskParams } }); toast(t('accepted')); await load(); ctx.result(result.data);
    }, 'create');
    else if (action.startsWith('inspect:')) {
      const row = rows[Number(action.slice(8))], sequence = ++loadSequence;
      selectedTask = row;
      const task = await ctx.get('admin', '/v1/tasks/' + pathID(row.id));
      if (!ctx.valid() || sequence !== loadSequence || tasksTab !== 'tasks') return;
      selectedTask = task;
      rows = rows.map(item => item.id === task.id ? task : item);
      drawTasks();
    }
    else if (action.startsWith('cancel:') || action.startsWith('retry:')) { const [op, index] = action.split(':'); await confirmRequest(ctx, op === 'cancel' ? 'cancelTask' : 'retry', 'admin', '/v1/tasks/' + pathID(rows[Number(index)].id) + '/' + op, undefined, { after: load }); }
    else if (action === 'backup') await confirmRequest(ctx, 'backup', 'admin', '/v1/tenants/' + pathID(tenant) + '/backup', {}, { after: load });
    else if (action === 'restore' || action === 'drill') {
      const drill = action === 'drill', id = tenant;
      dialog(drill ? 'restoreDrill' : 'restore', field('backupKey', '', 'text', 'required') + (drill ? field('targetTenant') + checkbox('cleanup', true) : checkbox('overwrite')) + checkbox('dryRun', true) + notice(t('destructive')) + field('typeConfirm', '', 'text', 'required') + `<p class="hint code">${e(id)}</p>`, async form => {
        if (form.elements.namedItem('typeConfirm').value !== id) throw new Error(t('confirmMismatch'));
        const body = { backup_key: form.elements.namedItem('backupKey').value, dry_run: form.elements.namedItem('dryRun').checked };
        if (drill) { body.target_tenant_id = form.elements.namedItem('targetTenant').value; body.cleanup = form.elements.namedItem('cleanup').checked; }
        else body.overwrite = form.elements.namedItem('overwrite').checked;
        const result = await request('admin', '/v1/tenants/' + pathID(id) + (drill ? '/restore-drill' : '/restore'), { method: 'POST', body, tenantID: '' }); toast(t('accepted')); await load(); ctx.result(result.data);
      }, 'execute', true);
    } else if (action === 'audit') ctx.result(await ctx.get('admin', '/v1/control/integrity-audit?deep=true'));
    else if (action === 'recover') await confirmRequest(ctx, 'recover', 'admin', '/v1/control/recover', undefined);
    else if (action === 'repair') await confirmRequest(ctx, 'repair', 'admin', '/v1/control/repair', { apply: false }, { destructive: true });
    else if (action === 'cleanup') await confirmRequest(ctx, 'cleanupCommits', 'admin', '/v1/control/cleanup-commits', undefined, { destructive: true });
    else if (action === 'gc') await confirmRequest(ctx, 'gc', 'admin', '/v1/control/gc', { keep_snapshots: 2, dry_run: true, max_deletes: 1000 }, { destructive: true });
    else if (action === 'reset-automation') await confirmRequest(ctx, 'resetAutomation', 'admin', '/v1/backup-automation/reset', undefined, { after: load });
  };
  await load();
}

export async function governancePage(ctx) {
  let original;
  const load = async () => {
    const path = governanceTab === 'sourcePolicy' ? '/v1/source-policy' : governanceTab === 'tenantConfig' ? '/v1/tenant-config' : '/v1/tenant-usage';
    const result = await ctx.get(governanceTab === 'usage' ? 'admin' : 'data', path);
    const policy = result.policy || {};
    const configuration = governanceTab === 'sourcePolicy' ? { ...Object.fromEntries(['default_priority', 'sources'].filter(key => key in policy).map(key => [key, policy[key]])), field_aliases: [], field_priorities: [], ...policy } : result.config;
    original = json(configuration);
    ctx.html(tabs(['sourcePolicy', 'tenantConfig', 'usage'], governanceTab) + (governanceTab === 'usage' ? structuredView(result) : `<form class="form-stack" id="configuration">${objectEditor('configuration', configuration, governanceTab)}<div class="toolbar"><button type="submit" class="primary">${e(t('save'))}</button></div></form>`));
    const form = ctx.root.querySelector('form');
    if (form) form.onsubmit = async event => {
      event.preventDefault();
      try { await confirmRequest(ctx, 'save', 'admin', path, objectInput(form, 'configuration'), { method: 'PUT', readOnly: true, after: load }); } catch (error) { toast(error.message, true); }
    };
    ctx.dirty = () => { const form = ctx.root.querySelector('#configuration'); if (!form) return false; try { return json(objectInput(form, 'configuration')) !== original; } catch { return true; } };
  };
  ctx.action = async action => {
    if (action.startsWith('tab:')) {
      if (ctx.dirty() && !(await leaveEditor())) return;
      governanceTab = action.slice(4); await load();
    }
  };
  await load();
}

async function leaveEditor() {
  return new Promise(resolve => {
    dialog('discard', notice(t('discardDesc')), async () => resolve(true), 'leave');
    document.querySelector('#dialog').addEventListener('close', () => resolve(false), { once: true });
  });
}

export async function ingestionPage(ctx) {
  const key = tenant;
  const draft = () => ingestionDrafts.get(key + ':' + ingestionTab) || json(ingestionTab === 'batch' ? { source: '', collector_id: '', batch_id: crypto.randomUUID(), items: [{ entity: { id: '', kind: '', fields: {} } }] } : { idempotency_key: crypto.randomUUID(), mutations: { upsert_entities: [{ id: '', kind: '', fields: {} }] } });
  let payload = draft();
  const capture = () => {
    const input = ctx.root.querySelector('[name=payload]');
    if (!input) return;
    const value = objectInput(ctx.root, 'payload');
    payload = input.value;
    ingestionDrafts.set(key + ':' + ingestionTab, payload);
    return value;
  };
  ctx.dirty = () => { try { capture(); return false; } catch { return true; } };
  function draw() {
    let content;
    if (ingestionTab === 'import') content = `<form class="form-stack" id="import-form">${notice(t('importHint'))}<div class="form-grid">${field('source', '', 'text', 'required')}${field('collectorID', '', 'text', 'required')}${select('format', ['jsonl', 'csv'])}${field('batchSize', 500, 'number', 'min="1" max="5000" required')}${select('onError', [['abort', t('fail')], ['continue', t('skip')]])}<label class="field"><span>${e(t('file'))}</span><input type="file" name="file" accept=".csv,.jsonl,.ndjson" required></label></div><div><button type="submit" class="primary">${e(t('import'))}</button></div></form>`;
    else if (ingestionTab === 'collectors') content = `<form class="toolbar" id="collector-form">${field('source', '', 'text', 'required')}${field('collectorID')}${field('batchID')}<button type="submit" class="primary">${e(t('collectorStatus'))}</button>${button('batchStatus', 'batch-status')}${button('deadletters', 'deadletters')}${button('replay', 'replay')}</form>`;
    else content = `<form class="form-stack" id="write-form">${objectEditor('payload', parseJSON(payload))}<div class="toolbar"><button type="submit" class="primary">${e(t('write'))}</button></div></form>`;
    ctx.html(tabs(['commit', 'batch', 'import', 'collectors'], ingestionTab) + content);
    const writeForm = ctx.root.querySelector('#write-form');
    if (writeForm) writeForm.onsubmit = async event => {
      event.preventDefault();
      try { await confirmRequest(ctx, 'write', 'data', ingestionTab === 'commit' ? '/v1/commits' : '/v1/ingest/batches', capture()); } catch (error) { toast(error.message, true); }
    };
    const importForm = ctx.root.querySelector('#import-form');
    if (importForm) importForm.onsubmit = async event => {
      event.preventDefault();
      const values = Object.fromEntries(new FormData(importForm)), file = values.file;
      const submit = importForm.querySelector('[type=submit]'); submit.disabled = true;
      try {
        const result = await request('data', '/v1/imports' + params({ source: values.source, collector_id: values.collectorID, format: values.format, batch_size: values.batchSize, on_error: values.onError }), { method: 'POST', body: file, headers: { 'Content-Type': values.format === 'csv' ? 'text/csv' : 'application/x-ndjson' } });
        toast(t('accepted')); ctx.result(result.data);
      } catch (error) { toast(error.message, true); } finally { submit.disabled = false; }
    };
    const collectorForm = ctx.root.querySelector('#collector-form');
    if (collectorForm) collectorForm.onsubmit = async event => { event.preventDefault(); try { await collector('collector-status'); } catch (error) { toast(error.message, true); } };
  }
  const collector = async action => {
    const form = ctx.root.querySelector('#collector-form');
    if (!form.reportValidity()) return;
    const values = Object.fromEntries(new FormData(form)), source = pathID(values.source);
    if ((action === 'collector-status' || action === 'batch-status') && !values.collectorID) throw new Error(t('requiredFields') + ' ' + t('collectorID'));
    if (action === 'batch-status' && !values.batchID) throw new Error(t('requiredFields') + ' ' + t('batchID'));
    if (action === 'replay') { await confirmRequest(ctx, 'replay', 'admin', '/v1/ingest/deadletters/' + source + '/replay?limit=100', undefined); return; }
    const path = action === 'batch-status' ? '/v1/ingest/batches/' + source + '/' + pathID(values.collectorID) + '/' + pathID(values.batchID) : action === 'deadletters' ? '/v1/ingest/deadletters/' + source : '/v1/ingest/collectors/' + source + '/' + pathID(values.collectorID);
    ctx.result(await ctx.get(action === 'batch-status' ? 'data' : 'admin', path));
  };
  ctx.action = async action => {
    if (action.startsWith('tab:')) {
      try { capture(); } catch { toast(t('unfinishedDraft'), true); return; }
      ingestionTab = action.slice(4);
      payload = draft();
      draw();
    } else if (['batch-status', 'deadletters', 'replay'].includes(action)) await collector(action);
  };
  draw();
}
