import { test } from 'node:test';
import assert from 'node:assert/strict';

globalThis.location = { origin: 'http://localhost:8080', pathname: '/ui/' };
globalThis.document = { documentElement: {} };
const storage = new Map();
globalThis.localStorage = { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) };
const { parseJSON, json, scalar, request, settings, saveSettings, endpointURL } = await import('./static/api.js');

test('database integer values survive inspection and editing without rounding', () => {
  const input = '{"version":9223372036854775807,"fields":{"timestamp":1791685200123456789,"decimal":1.5,"string":"9223372036854775807"}}';
  const value = parseJSON(input);
  assert.equal(value.version.rawJSON, '9223372036854775807');
  assert.equal(value.fields.timestamp.rawJSON, '1791685200123456789');
  assert.equal(JSON.stringify(value), input);
  assert.equal(parseJSON(json(value)).fields.decimal, 1.5);
  assert.equal(JSON.stringify(parseJSON('{"n":9.223372036854776e18}')), '{"n":9.223372036854776e18}');
  assert.equal(scalar(value.version), '9223372036854775807');
  assert.equal(scalar({ rawJSON: 'a user field' }), '{"rawJSON":"a user field"}');
});

test('endpoint configuration rejects credential-bearing URLs and never persists tokens', () => {
  for (const value of ['javascript:alert(1)', 'https://user:secret@example.com', 'https://example.com?token=secret', '//user:secret@example.com']) assert.throws(() => endpointURL(value));
  saveSettings({ data: '/', admin: '/admin/', router: '', raft: '', clusterID: '', token: 'secret', routerToken: 'router-secret', raftToken: 'raft-secret' });
  assert.equal(settings.admin, 'http://localhost:8080/admin');
  assert.doesNotMatch(storage.get('graphdb.endpoints'), /secret/);
});

test('requests preserve endpoint authorization, tenant scope and uncertain mutation semantics', async () => {
  Object.assign(settings, { data: 'http://localhost:8080', admin: 'http://localhost:8080/admin', router: 'http://localhost:8080/router', raft: 'http://localhost:8080/private', token: 'data-token', routerToken: 'router-token', raftToken: 'raft-token', clusterID: 'group-a' });
  const calls = [];
  globalThis.fetch = async (url, init) => { calls.push({ url, ...init }); return new Response('{"status":"ok"}', { headers: { 'Content-Type': 'application/json' } }); };
  await request('admin', '/v1/tasks', { method: 'POST', tenantID: 'tenant-a', body: { type: 'compact' } });
  await request('raft', '/raft/status', { tenantID: '' });
  await request('router', '/v1/cluster', { tenantID: '' });
  assert.equal(calls[0].url, 'http://localhost:8080/admin/v1/tasks');
  assert.equal(calls[0].headers['X-Tenant-ID'], 'tenant-a');
  assert.equal(calls[0].headers.Authorization, 'Bearer data-token');
  assert.equal(calls[1].headers.Authorization, 'Bearer raft-token');
  assert.equal(calls[1].headers['X-Raft-Cluster'], 'group-a');
  assert.equal(calls[2].headers.Authorization, 'Bearer router-token');
  let attempts = 0;
  globalThis.fetch = async () => { attempts++; throw new TypeError('network lost after send'); };
  await assert.rejects(request('data', '/v1/commits', { method: 'POST', body: { mutations: {} }, tenantID: 'tenant-a' }), /server state|服务端状态/);
  assert.equal(attempts, 1);
});

test('snapshot NDJSON exports retain every line without whole-document JSON parsing', async () => {
  const ndjson = '{"version":2}\n{"entity":{"id":"host:a"}}\n{"entity":{"id":"host:b"}}\n';
  globalThis.fetch = async () => new Response(ndjson, { headers: { 'Content-Type': 'application/x-ndjson' } });
  const result = await request('data', '/v1/export/snapshot/stream', { raw: true, tenantID: 'tenant-a' });
  assert.equal(result.data, ndjson);
});

test('overview distinguishes empty measurements from missing diagnostics and retains exact version labels', async () => {
  const { setTenant } = await import('./static/api.js');
  const { setLanguage } = await import('./static/i18n.js');
  const { overviewPage } = await import('./static/overview.js');
  setTenant('demo'); setLanguage('en');
  let html;
  const reports = {
    '/v1/health': { status: 'degraded' }, '/v1/readiness': { status: 'ready' },
    '/v1/diagnostics': { status: 'ok', tasks: null, tasks_limit: 100, disk: { total_bytes: 100, available_bytes: 100, write_ready: true } },
    '/v1/tenant-usage': { categories: [], total_bytes: 0, object_count: 0 },
    '/v1/control/reader-freshness': parseJSON('{"fresh":true,"manifest_version":9223372036854775807,"visible_version":9223372036854775807,"version_lag":0}')
  };
  const ctx = { valid: () => true, header: () => {}, connection: () => {}, html: value => { html = value; }, get: async (_endpoint, path) => reports[path] };
  await overviewPage(ctx);
  assert.match(html, /aria-label="Health: Degraded"/);
  assert.match(html, />0%<\/text>/);
  assert.match(html, />0 B<\/text>/);
  assert.match(html, />0<\/text><text[^>]*>tasks<\/text>/);
  assert.match(html, /9223372036854775807/);
  assert.doesNotMatch(html, /NaN|Infinity/);
  ctx.get = async (_endpoint, path) => { if (path === '/v1/diagnostics') throw new Error('offline'); return reports[path]; };
  await overviewPage(ctx);
  assert.match(html, /aria-label="Local diagnostics: Unknown"/);
  assert.match(html, /Data unavailable/);
  assert.doesNotMatch(html, />0%<\/text>/);
  setTenant('');
});

test('structured records escape user fields and retain exact numeric values and nested details', async () => {
  const { structuredView, valueView } = await import('./static/display.js');
  const input = parseJSON('{"fields":{"<script>":"<img src=x onerror=alert(1)>","version":9223372036854775807,"enabled":false,"tags":["a","b"],"nested":{"key":"value"}},"rows":[{"id":"a","count":0},{"id":"b","count":3}]}');
  const view = structuredView(input);
  assert.doesNotMatch(view, /<script>|<img src=x/);
  assert.match(view, /&lt;script&gt;/);
  assert.match(view, /9223372036854775807/);
  assert.match(view, /<table>/);
  assert.match(view, /value-chips/);
  assert.match(view, /<details class="raw-data"[^>]*>/);
  assert.match(valueView(false), />No<\/span>/);
  assert.match(valueView(0), />0<\/span>/);
  assert.match(structuredView({ codec: 'arrow-only-metadata' }), /arrow-only-metadata/);
});

test('structured field edits preserve value types and large integers across advanced JSON round trips', async () => {
  const { parseField, objectEditor } = await import('./static/forms.js');
  assert.equal(JSON.stringify(parseField('number', '9223372036854775807')), '9223372036854775807');
  assert.equal(parseField('string', '9223372036854775807'), '9223372036854775807');
  assert.equal(parseField('boolean', 'false'), false);
  assert.equal(parseField('null', ''), null);
  assert.equal(parseField('number', ''), undefined);
  assert.throws(() => parseField('number', '"123"'));
  assert.throws(() => parseField('boolean', '1'));
  const payload = parseJSON('{"fields":{"counter":9223372036854775807},"unknown":{"future_option":true}}');
  const editor = objectEditor('payload', payload);
  assert.match(editor, /9223372036854775807/);
  assert.match(editor, /future_option/);
  assert.match(editor, /<details class="advanced-editor">/);
  assert.match(editor, /data-node="number"/);
});

test('adding a source starts with defaults instead of duplicating an existing source', async () => {
  const { objectEditor } = await import('./static/forms.js');
  const view = objectEditor('configuration', { sources: [{ name: 'production-source', priority: 10, description: 'Existing source' }] });
  const template = view.match(/<template>([\s\S]*?)<\/template>/)[1];
  const initialValues = [...template.matchAll(/data-value-input[^>]*value="([^"]*)"/g)].map(match => match[1]);
  assert.deepEqual(initialValues, ['', '0', '']);
});

test('collapsed details expand once with complete, escaped, lossless data', async () => {
  const { details, bindDetails } = await import('./static/display.js');
  const value = parseJSON('{"counter":9223372036854775807,"untrusted":"</script><img src=x onerror=alert(1)>","rows":[{"id":"a"},{"id":"b"}]}');
  const html = details('checkpoint', value);
  const payload = { textContent: html.match(/<script[^>]*>([\s\S]*?)<\/script>/)[1], removed: false, remove() { this.removed = true; }, insertAdjacentHTML(_position, content) { expanded.push(content); } };
  assert.deepEqual(parseJSON(payload.textContent), value);
  let listener, bindings = 0;
  const expanded = [], root = { dataset: {}, addEventListener(_event, handler) { bindings++; listener = handler; } };
  const target = { open: false, dataset: { detailView: 'structured' }, querySelector: () => payload.removed ? null : payload };
  bindDetails(root); bindDetails(root);
  listener({ target });
  assert.equal(expanded.length, 0);
  target.open = true; listener({ target }); listener({ target });
  assert.equal(bindings, 1);
  assert.equal(expanded.length, 1);
  assert.match(expanded[0], /9223372036854775807/);
  assert.match(expanded[0], /&lt;\/script&gt;&lt;img/);
  assert.doesNotMatch(html, /<img src=x/);
});

test('query lists fetch the active tab and refresh its current server data', async () => {
  const { queryPage } = await import('./static/data.js');
  const calls = [];
  const ctx = { root: { querySelector: () => null }, valid: () => true, html: () => {}, get: async (_endpoint, path) => { calls.push(path); return { queries: [] }; } };
  await queryPage(ctx);
  await ctx.action('tab:savedQueries');
  assert.deepEqual(calls.splice(0), ['/v1/query/templates']);
  await ctx.action('tab:runningQueries');
  assert.deepEqual(calls.splice(0), ['/v1/queries/running']);
  await ctx.action('list-refresh');
  assert.deepEqual(calls.splice(0), ['/v1/queries/running']);
  await ctx.action('tab:query');
});

test('task inspection uses one detail request and ignores a late prior selection', async () => {
  const { tasksPage } = await import('./static/operations.js');
  const calls = [], signal = new AbortController();
  const rows = ['a', 'b'].map(id => ({ id, type: 'compact', status: 'succeeded', phase: 'listed' }));
  let rendered = '', finishPrior, finishList, delayList = false;
  const ctx = { signal: signal.signal, root: { querySelector: () => ({}) }, valid: () => true, html: value => { rendered = value; }, get: async (_endpoint, path) => {
    calls.push(path);
    if (path.startsWith('/v1/tasks?')) return delayList ? new Promise(resolve => { finishList = resolve; }) : { tasks: rows };
    if (path === '/v1/tasks/a') return new Promise(resolve => { finishPrior = resolve; });
    return { ...rows[1], phase: 'latest-b' };
  } };
  await tasksPage(ctx); calls.length = 0;
  const prior = ctx.action('inspect:0');
  await ctx.action('inspect:1');
  assert.deepEqual(calls, ['/v1/tasks/a', '/v1/tasks/b']);
  finishPrior({ ...rows[0], phase: 'late-a' }); await prior;
  assert.match(rendered, /latest-b/);
  assert.doesNotMatch(rendered, /late-a/);
  delayList = true;
  const refresh = ctx.action('tab:tasks');
  await ctx.action('inspect:1');
  finishList({ tasks: [...rows].reverse() }); await refresh;
  calls.length = 0;
  const nextSelection = ctx.action('inspect:1');
  assert.deepEqual(calls, ['/v1/tasks/b']);
  await nextSelection;
  signal.abort();
});

test('tenant pages fetch bounded results, reset search cursors and ignore late pages and inspections', async () => {
  const { tenantsPage } = await import('./static/tenants.js');
  const { setTenant, tenant: initialTenant } = await import('./static/api.js');
  const calls = [], elements = { tenantSearch: { value: '09999' }, includeLegacy: { checked: false } };
  const form = { elements: { namedItem: name => elements[name] } }, limit = {};
  let rendered, finishPrior, finishDetail;
  const ctx = { valid: () => true, root: { querySelector: selector => selector === '#tenant-filters' ? form : limit }, html: value => { rendered = value; }, refresh: async () => {}, get: async (_endpoint, path, options) => {
    calls.push({ path, options });
    if (path.endsWith('/tenant-00000')) return new Promise(resolve => { finishDetail = resolve; });
    const query = new URL(path, 'http://localhost').searchParams;
    if (query.has('cursor')) return new Promise(resolve => { finishPrior = resolve; });
    return { tenants: [{ tenant_id: query.get('q') ? 'tenant-09999' : 'tenant-00000', status: 'active' }], total: query.get('q') ? 1 : 10000, next_cursor: query.get('q') ? '' : 'tenant-00024' };
  } };
  await tenantsPage(ctx);
  assert.equal(new URL(calls[0].path, 'http://localhost').searchParams.get('limit'), '25');
  assert.equal(calls[0].options.tenantID, '');
  assert.match(rendered, /10000/);
  const detail = ctx.action('inspect:0');
  const prior = ctx.action('next');
  await form.onsubmit({ preventDefault() {} });
  finishPrior({ tenants: [{ tenant_id: 'wrong-tenant' }], total: 10000 }); await prior;
  finishDetail({ tenant_id: 'tenant-00000' }); await detail;
  assert.match(rendered, /tenant-09999/);
  assert.doesNotMatch(rendered, /wrong-tenant/);
  const search = new URL(calls.at(-1).path, 'http://localhost').searchParams;
  assert.equal(search.get('q'), '09999'); assert.equal(search.has('cursor'), false); assert.equal(search.has('include_legacy'), false);
  await ctx.action('select:0');
  assert.equal((await import('./static/api.js')).tenant, 'tenant-09999');
  setTenant(initialTenant);
});
