import { objectEditor } from './forms.js';
import { t } from './i18n.js';
import { tenant, pathID, params, json, parseJSON, request } from './api.js';
import { e, button, field, select, editor, tabs, notice, table, details, pre, rawDetails, structuredView, valueView, fieldSummary, link, objectInput, dialog, toast, confirmRequest, download, bindActions } from './ui.js';
import { graphView, resultGraph } from './graph.js';

let dataTab = 'entities', schemaTab = 'entityTypes', queryTab = 'query', resultTab = 'table';
const drafts = new Map();
const dataStates = new Map();
const samples = {
  json: { op: 'match', limit: 50, timeout_ms: 10000, profile: true },
  graphql: { query: 'query Browse($request: QueryRequest!) { graph(request: $request) { version results stats } }', variables: { request: { op: 'match', limit: 50, timeout_ms: 10000 } } },
  legacy: { query: 'FIND host\nLIMIT 50' },
};

export async function dataPage(ctx) {
  const state = dataStates.get(tenant) || { filters: { kind: '', source: '', type: '', from: '', limit: 50 }, cursor: '', history: [], page: 1 };
  dataStates.set(tenant, state);
  let { filters, cursor, history, page } = state;
  let selected, entities = [], edges = [], version;
  const load = async () => {
    Object.assign(state, { filters, cursor, history, page });
    const values = { ...filters, cursor };
    if (dataTab === 'relationships') { delete values.kind; }
    else { delete values.type; delete values.from; }
    if (dataTab === 'graph') {
      const result = await Promise.all([ctx.get('data', '/v1/entities' + params({ ...values, limit: 100 })), ctx.get('data', '/v1/edges' + params({ limit: 100, source: filters.source }))]);
      entities = result[0].entities || []; edges = result[1].edges || []; version = result[0].version;
    } else {
      const result = await ctx.get('data', '/v1/' + (dataTab === 'entities' ? 'entities' : 'edges') + params(values));
      entities = result.entities || []; edges = result.edges || []; version = result.version;
      load.next = result.next_cursor || '';
    }
    selected = (dataTab === 'relationships' ? edges : entities)[0];
    draw();
  };
  function draw() {
    ctx.header(button('write', 'write', 'primary') + button('export', 'export'));
    const rows = dataTab === 'relationships' ? edges : entities;
    const cols = dataTab === 'relationships' ? [['id', row => link(row.id || `${row.from} → ${row.to}`, 'inspect:' + rows.indexOf(row))], ['type', row => e(row.type)], ['from', row => e(row.from)], ['to', row => e(row.to)], ['fields', row => fieldSummary(row.fields || {})]] : [['id', row => link(row.id, 'inspect:' + rows.indexOf(row))], ['kind', row => `<span class="kind-chip">${e(row.kind)}</span>`], ['fields', row => fieldSummary(row.fields || {})], ['source', row => e(row.source)], ['version', row => e(row.version)]];
    const inspector = selected ? `<aside class="inspector"><div class="section-title"><h2>${e(selected.id)}</h2>${button('close', 'clear', 'small')}</div><span class="kind-chip">${e(selected.kind || selected.type)}</span><section class="property-group"><h3>${e(t('properties'))}</h3>${valueView(selected.fields || {}, 'fields', 0, true)}</section><section class="property-group"><h3>${e(t('recordInformation'))}</h3>${valueView({ source: selected.source, version: selected.version, created_at: selected.created_at, updated_at: selected.updated_at, ...(dataTab === 'relationships' ? { from: selected.from, to: selected.to } : {}) })}</section><div class="row-actions">${button(dataTab === 'relationships' ? 'editEdge' : 'editEntity', 'edit', 'primary')}${dataTab === 'relationships' ? '' : button('neighbors', 'neighbors')}${button(dataTab === 'relationships' ? 'deleteEdge' : 'deleteEntity', 'delete', 'danger-outline')}</div>${rawDetails(selected)}</aside>` : '';
    ctx.html(`<div class="${selected ? 'split' : ''}"><section class="workbench ${dataTab === 'graph' ? '' : 'data-table'}">${tabs(['entities', 'relationships', 'graph'], dataTab)}<form class="toolbar" id="filters">${dataTab === 'relationships' ? field('type', filters.type) + field('from', filters.from) : field('kind', filters.kind)}${field('source', filters.source)}<button class="primary" type="submit">${e(t('search'))}</button></form>${dataTab === 'graph' ? graphView(entities, edges) : table(cols, rows, selected)}<div class="pagination">${dataTab === 'graph' ? '' : button('previous', 'previous', '', !history.length ? 'disabled' : '') + button('next', 'next', '', !load.next ? 'disabled' : '')}<span class="muted">${e(t('page'))} ${page} · ${e(t('shown'))} ${rows.length} · ${e(t('version'))} ${e(version)}</span>${select('limit', [25, 50, 100].map(value => [String(value), String(value)]), String(filters.limit))}</div></section>${inspector}</div>`);
    const form = ctx.root.querySelector('#filters');
    form.onsubmit = async event => {
      event.preventDefault();
      filters = { ...filters, ...Object.fromEntries(new FormData(form)) }; cursor = ''; history = []; page = 1;
      try { await load(); } catch (error) { toast(error.message, true); }
    };
    ctx.root.querySelector('[name=limit]').onchange = async event => { filters.limit = event.target.value; cursor = ''; history = []; page = 1; try { await load(); } catch (error) { toast(error.message, true); } };
  }
  ctx.action = async action => {
    if (action.startsWith('tab:')) { dataTab = action.slice(4); cursor = ''; history = []; page = 1; await load(); }
    else if (action.startsWith('inspect:')) { selected = (dataTab === 'relationships' ? edges : entities)[Number(action.slice(8))]; draw(); }
    else if (action.startsWith('node:')) { selected = await ctx.get('data', '/v1/entities/' + pathID(action.slice(5))); selected = selected.entity || selected; draw(); }
    else if (action === 'clear') { selected = null; draw(); }
    else if (action === 'next') { history.push(cursor); cursor = load.next; page++; await load(); }
    else if (action === 'previous') { cursor = history.pop() || ''; page--; await load(); }
    else if (action === 'export') {
      const result = await ctx.get('data', '/v1/export/snapshot/stream?inline=true', { raw: true }); download(result, tenant + '-snapshot.ndjson', 'application/x-ndjson');
    } else if (action === 'neighbors') {
      const result = await ctx.get('data', '/v1/query', { method: 'POST', body: { op: 'neighbors', id: selected.id, direction: 'both', limit: 100, timeout_ms: 10000 } });
      dialog('neighbors', resultGraph({ ...result, results: [{ entity: selected }, ...(result.results || [])] }) + '<div class="neighbor-detail"></div>' + details('response', result), async () => {}, 'close');
      const target = document.querySelector('#dialog');
      const inspector = target.querySelector('.neighbor-detail');
      bindActions(target, async action => {
        if (action.startsWith('node:')) {
          const entity = await ctx.get('data', '/v1/entities/' + pathID(action.slice(5)));
          if (inspector.isConnected && target.open) inspector.innerHTML = structuredView(entity);
        }
      });
    } else if (['write', 'edit', 'delete'].includes(action)) {
      const relationship = dataTab === 'relationships';
      const key = action === 'delete' ? relationship ? 'delete_edges' : 'delete_entities' : relationship ? 'upsert_edges' : 'upsert_entities';
      const item = action === 'write' ? relationship ? { type: '', from: '', to: '', fields: {} } : { id: '', kind: '', fields: {} } : action === 'delete' ? selected.id : selected;
      const body = { expected_version: version, idempotency_key: crypto.randomUUID(), mutations: { [key]: [item] } };
      await confirmRequest(ctx, action === 'delete' ? 'remove' : action === 'edit' ? 'edit' : 'write', 'data', '/v1/commits', body, { destructive: action === 'delete', resource: selected?.id, after: load });
    }
  };
  await load();
}

export async function schemaPage(ctx) {
  let rows = [], version;
  const config = {
    entityTypes: { path: '/v1/entity-types', list: 'entity_types', upsert: 'upsert_entity_types', remove: 'delete_entity_types', sample: { name: '', display_name: '', fields: { name: { type: 'string' } } } },
    relationTypes: { path: '/v1/relation-types', list: 'relation_types', upsert: 'upsert_relation_types', remove: 'delete_relation_types', sample: { name: '', from_kind: '', to_kind: '', directed: true } },
    relationSchemas: { path: '/v1/relation-schemas', list: 'relation_schemas', sample: { relation_type: '', fields: {} } },
  };
  const load = async () => {
    const result = await ctx.get('data', config[schemaTab].path);
    rows = result[config[schemaTab].list] || [];
    if (!Array.isArray(rows)) rows = Object.values(rows);
    version = result.version;
    ctx.html(`${tabs(Object.keys(config), schemaTab)}<div class="toolbar">${button('newType', 'create', 'primary')}</div>${table([['name', row => link(row.name || row.relation_type, 'inspect:' + rows.indexOf(row))], ['fields', row => fieldSummary(row.fields || {})], ['details', row => e(row.display_name || [row.from_kind, row.to_kind].filter(Boolean).join(' → '))], ['actions', row => button('edit', 'edit:' + rows.indexOf(row), 'small') + ' ' + button('remove', 'delete:' + rows.indexOf(row), 'small')]], rows)}`);
  };
  ctx.action = async action => {
    if (action.startsWith('tab:')) { schemaTab = action.slice(4); await load(); return; }
    const [operation, index] = action.split(':');
    const definition = config[schemaTab], row = rows[Number(index)], name = row?.name || row?.relation_type;
    if (operation === 'inspect') { dialog('details', structuredView(row), async () => {}, 'close'); return; }
    const deleting = operation === 'delete', item = operation === 'create' ? definition.sample : row;
    if (schemaTab === 'relationSchemas') {
      dialog(deleting ? 'remove' : 'schemaJSON', field('type', item.relation_type, 'text', deleting ? 'required readonly' : 'required') + (deleting ? notice(t('destructive')) + field('typeConfirm', '', 'text', 'required') + `<p class="hint code">${e(item.relation_type)}</p>` : objectEditor('definition', item, 'schemaJSON')), async form => {
        if (deleting && form.elements.namedItem('typeConfirm').value !== item.relation_type) throw new Error(t('confirmMismatch'));
        const path = definition.path + '/' + pathID(form.elements.namedItem('type').value);
        await request('admin', path, { method: deleting ? 'DELETE' : 'PUT', body: deleting ? undefined : objectInput(form, 'definition') });
        toast(t('success')); await load();
      }, deleting ? 'remove' : 'save', deleting);
    } else {
      const body = { idempotency_key: crypto.randomUUID(), mutations: { [deleting ? definition.remove : definition.upsert]: [deleting ? name : item] } };
      if (version !== undefined) body.expected_version = version;
      await confirmRequest(ctx, deleting ? 'remove' : 'schemaJSON', 'data', '/v1/commits', body, { destructive: deleting, resource: name, after: load });
    }
  };
  await load();
}

export async function queryPage(ctx) {
  const draftKey = tenant;
  let state = drafts.get(draftKey) || { mode: 'json', text: json(samples.json), result: null, elapsed: 0, next: '' };
  state.texts ||= { [state.mode]: state.text };
  state.example ||= 'match';
  drafts.set(draftKey, state);
  let queryController, saved = [], running = [];
  const retain = () => { const textarea = ctx.root.querySelector('[name=queryPayload]'); if (textarea) state.texts[state.mode] = state.text = textarea.value; };
  const execute = async (body, path = '/v1/query') => {
    if (queryController) return;
    queryController = new AbortController();
    const started = performance.now();
    state.text = ctx.root.querySelector('[name=queryPayload]')?.value || state.text;
    draw();
    try {
      const result = await request('data', path, { method: 'POST', body, signal: AbortSignal.any([ctx.signal, queryController.signal]) });
      if (!ctx.valid()) return;
      state.result = result.data.data?.graph || result.data;
      state.elapsed = Math.round(performance.now() - started);
      state.next = state.result.next_cursor || '';
    } catch (error) { if (error.name !== 'AbortError') toast(error.message, true); }
    finally { queryController = null; if (ctx.valid()) draw(); }
  };
  const loadLists = async () => {
    const tab = queryTab;
    if (tab === 'savedQueries') saved = (await ctx.get('admin', '/v1/query/templates')).queries || [];
    else running = (await ctx.get('admin', '/v1/queries/running')).queries || [];
    if (ctx.valid() && queryTab === tab) draw();
  };
  function draw() {
    let content;
    if (queryTab === 'savedQueries') content = `<div class="toolbar">${button('refresh', 'list-refresh')}</div>${table([['name', row => link(row.name, 'load:' + saved.indexOf(row))], ['description', row => e(row.description)], ['actions', row => button('run', 'saved:' + saved.indexOf(row), 'small')]], saved)}`;
    else if (queryTab === 'runningQueries') content = notice(t('stopHint')) + `<div class="toolbar">${button('refresh', 'list-refresh')}</div>${table([['id', row => e(row.id)], ['operation', row => e(row.op)], ['elapsed', row => e(row.duration_ms) + ' ms'], ['actions', row => button('cancel', 'cancel:' + running.indexOf(row), 'small')]], running)}`;
    else {
      const result = state.result;
      let resultView = '';
      if (result && resultTab === 'json') resultView = pre(result);
      else if (result && resultTab === 'graph') resultView = resultGraph(result);
      else if (result) {
        const columns = [...new Set((result.results || []).flatMap(row => Object.keys(row.fields || row.entity?.fields || row.edge?.fields || {})))].slice(0, 10);
        resultView = table([['id', row => e(row.entity?.id || row.edge?.id || row.path?.entities?.map(node => node.id).join(' → ') || '')], ...columns.map(key => ['@' + key, row => valueView((row.fields || row.entity?.fields || row.edge?.fields || {})[key])])], result.results || []);
      }
      content = `<div class="query-layout"><div class="query-editor"><div class="query-controls">${select('queryMode', [['json', 'JSON DSL'], ['graphql', 'GraphQL'], ['legacy', t('legacy')]], state.mode)}<div class="query-example">${select('example', ['match', 'neighbors', 'traverse', 'shortest', 'explain'], state.example)}${button('loadExample', 'load-example')}</div></div><p class="hint">${e(t('queryDraftHint'))}</p>${editor('queryPayload', state.text, 'queryPayload', 17)}<div class="toolbar">${button('formatJSON', 'format')}${button('saveQuery', 'save-query', '', state.mode !== 'json' ? 'disabled' : '')}${queryController ? button('stop', 'stop') : button('run', 'run', 'primary')}</div></div><section class="query-result"><h2>${e(t('result'))}</h2>${result ? `<div class="result-meta"><span>${e(t('version'))} ${e(result.version)}</span><span>${e(result.stats?.returned ?? result.results?.length ?? 0)} ${e(t('records'))}</span><span>${state.elapsed} ms</span>${button('download', 'download', 'small')}</div>${tabs(['table', 'json', 'graph'], resultTab)}${resultView}<div class="pagination">${button('next', 'query-next', '', !state.next || state.mode !== 'json' || queryController ? 'disabled' : '')}</div>${details('details', { stats: result.stats, plan: result.plan, profile: result.profile, aggregates: result.aggregates, groups: result.groups, errors: result.errors })}` : `<div class="empty"><p>${e(t('noResult'))}</p></div>`}</section></div>`;
    }
    ctx.html(tabs(['query', 'savedQueries', 'runningQueries'], queryTab) + content);
    const input = ctx.root.querySelector('[name=queryPayload]');
    if (input) input.oninput = retain;
    const mode = ctx.root.querySelector('[name=queryMode]');
    if (mode) mode.onchange = () => { retain(); state.mode = mode.value; state.text = state.texts[state.mode] ?? json(samples[state.mode]); state.next = ''; draw(); };
    const sample = ctx.root.querySelector('[name=example]');
    if (sample) sample.onchange = () => { state.example = sample.value; };
  }
  ctx.action = async action => {
    retain();
    if (action.startsWith('tab:')) { const tab = action.slice(4); if (['table', 'json', 'graph'].includes(tab)) resultTab = tab; else queryTab = tab; if (queryTab !== 'query') await loadLists(); else draw(); }
    else if (action === 'run' || action === 'query-next') {
      const body = parseJSON(state.text);
      if (action === 'query-next') body.cursor = state.next;
      await execute(body, '/v1/query' + (state.mode === 'json' ? '' : state.mode === 'graphql' ? '/graphql' : '/gql'));
    } else if (action === 'load-example') {
      const op = state.example;
      const payload = { op: op === 'shortest' ? 'shortest_path' : op, limit: 50, timeout_ms: 10000 };
      if (['neighbors', 'traverse', 'shortest'].includes(op)) payload.id = '';
      if (op === 'traverse') Object.assign(payload, { direction: 'out', depth: 3 });
      if (op === 'shortest') payload.target_id = '';
      if (op === 'explain') payload.target_op = 'match';
      state.mode = 'json'; state.texts.json = state.text = json(payload); state.next = ''; draw();
    } else if (action === 'stop') { queryController?.abort(); toast(t('stopHint')); }
    else if (action === 'format') { state.text = json(parseJSON(state.text)); draw(); }
    else if (action === 'download') download(state.result, tenant + '-query.json');
    else if (action === 'list-refresh') await loadLists();
    else if (action.startsWith('load:')) { state.mode = 'json'; state.text = json(saved[Number(action.slice(5))].request); queryTab = 'query'; draw(); }
    else if (action.startsWith('saved:')) { const row = saved[Number(action.slice(6))]; queryTab = 'query'; state.mode = 'json'; state.text = json(row.request); draw(); await execute({}, '/v1/query/templates/' + pathID(row.name) + '/run'); }
    else if (action.startsWith('cancel:')) await confirmRequest(ctx, 'cancelTask', 'admin', '/v1/queries/running/' + pathID(running[Number(action.slice(7))].id), undefined, { method: 'DELETE', after: loadLists });
    else if (action === 'save-query') dialog('saveQuery', field('queryName', '', 'text', 'required') + field('description'), async form => {
      await request('admin', '/v1/query/templates', { method: 'POST', body: { name: form.elements.namedItem('queryName').value, description: form.elements.namedItem('description').value, request: parseJSON(state.text) } }); toast(t('success'));
    });
    else if (action.startsWith('node:')) { const result = await ctx.get('data', '/v1/entities/' + pathID(action.slice(5))); dialog('details', structuredView(result), async () => {}, 'close'); }
  };
  if (queryTab === 'query') draw(); else await loadLists();
}
