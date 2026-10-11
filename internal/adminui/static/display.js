import { t, language } from './i18n.js';
import { json, parseJSON, scalar } from './api.js';

export const e = value => scalar(value).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
export const isRecord = value => value !== null && typeof value === 'object' && !Array.isArray(value) && !JSON.isRawJSON?.(value);
export const valueType = value => JSON.isRawJSON?.(value) ? 'number' : value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
const columnOrder = ['id', 'name', 'tenant_id', 'kind', 'type', 'field', 'status', 'phase', 'version', 'relation_type', 'source', 'priority', 'value', 'count', 'row_count', 'entity_count', 'edge_count', 'role', 'key', 'format'];
const storageKeys = new Set(['layout_version', 'codec', 'content_hash', 'schema_hash', 'data_hash', 'snapshot_key', 'snapshot_catalog_key', 'head_commit_id', 'commit_keys', 'filesystem_id']);
export const label = key => t('field_' + key) !== 'field_' + key ? t('field_' + key) : t(key) !== key ? t(key) : key.replace(/_/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2');
export const pre = value => `<pre class="code result-json">${e(typeof value === 'string' ? value : json(value))}</pre>`;
// JSON lives in an inert payload until expansion. Escape '<' so user strings
// cannot terminate the script element and introduce markup.
const detailPayload = value => `<script type="application/json" data-detail-payload>${JSON.stringify(value)?.replace(/</g, '\\u003c') ?? 'null'}</script>`;
export const rawDetails = value => `<details class="raw-data" data-detail-view="raw"><summary>${e(t('rawJSON'))}</summary>${detailPayload(value)}</details>`;
export const badge = value => `<span class="status ${['ok', 'ready', 'fresh', 'active', 'succeeded', 'failed', 'degraded', 'running', 'queued', 'disabled', 'deleted', 'canceled'].includes(value) ? value : ''}">${e(t(value || 'unknown'))}</span>`;
const dateFormats = new Map();

export function dateView(value) {
  if (!value || String(value).startsWith('0001-')) return '<span class="muted">—</span>';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return e(value);
  if (!dateFormats.has(language)) dateFormats.set(language, new Intl.DateTimeFormat(language, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }));
  return `<time datetime="${e(value)}" title="${e(value)}">${e(dateFormats.get(language).format(date))}</time>`;
}

export function table(columns, rows, selected) {
  if (!rows.length) return `<div class="empty"><div class="empty-symbol">∅</div><h3>${e(t('empty'))}</h3><p>${e(t('emptyDesc'))}</p></div>`;
  return `<div class="table-scroll"><table><thead><tr>${columns.map(([key]) => `<th scope="col">${e(key.startsWith('@') ? key.slice(1) : label(key))}</th>`).join('')}</tr></thead><tbody>${rows.map((row, i) => `<tr ${row === selected ? 'class="selected"' : ''}>${columns.map(([, render]) => `<td>${render(row, i)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}

function scalarView(value, key) {
  if (value == null) return '<span class="muted">—</span>';
  if (typeof value === 'boolean') return `<span class="boolean-value ${value ? 'yes' : ''}">${e(t(value ? 'yes' : 'no'))}</span>`;
  if (['status', 'state'].includes(key)) return badge(value);
  if (/(?:_at|_run|_success|_drill)$/.test(key) && typeof value === 'string' && /^\d{4}-\d\d-\d\dT/.test(value)) return dateView(value);
  return `<span class="${valueType(value) === 'number' ? 'numeric-value' : 'text-value'}">${e(value === '' ? '—' : value)}</span>`;
}

export function fieldSummary(value = {}) {
  if (!Object.keys(value).length) return '<span class="muted">—</span>';
  return `<div class="field-summary">${Object.entries(value).slice(0, 3).map(([key, item]) => `<div><span>${e(key)}</span><strong>${isRecord(item) ? typeof item.type === 'string' ? e(item.type) : `${Object.keys(item).length} ${e(t('properties'))}` : Array.isArray(item) ? `${item.length} ${e(t('items'))}` : e(item)}</strong></div>`).join('')}${Object.keys(value).length > 3 ? `<span class="muted">+${Object.keys(value).length - 3} ${e(t('properties'))}</span>` : ''}</div>`;
}

export function valueView(value, key = '', depth = 0, literal = false) {
  if (depth > 5) return `<details><summary>${e(label(key))}</summary>${pre(value)}</details>`;
  if (Array.isArray(value)) {
    if (!value.length) return `<p class="empty-inline">${e(t('noItems'))}</p>`;
    if (value.every(item => !isRecord(item) && !Array.isArray(item))) return `<div class="value-chips">${value.map(item => `<span>${scalarView(item, key)}</span>`).join('')}</div>`;
    if (value.every(isRecord)) {
      const columns = [...new Set(value.flatMap(item => Object.keys(item)))].sort((a, b) => {
        const rank = name => columnOrder.includes(name) ? columnOrder.indexOf(name) : value.some(item => isRecord(item[name]) || Array.isArray(item[name])) ? 200 : storageKeys.has(name) ? 150 : 100;
        return rank(a) - rank(b);
      });
      const visible = columns.slice(0, 5);
      const cell = (row, name) => isRecord(row[name]) || Array.isArray(row[name]) ? `<details><summary>${e(t('details'))}</summary>${valueView(row[name], name, depth + 1)}</details>` : valueView(row[name], name, depth + 1);
      return table(visible.map(name => ['@' + label(name), row => cell(row, name)]).concat(columns.length > visible.length ? [['details', row => `<details><summary>${e(t('details'))}</summary>${valueView(Object.fromEntries(Object.entries(row).filter(([name]) => !visible.includes(name))), '', depth + 1)}</details>`]] : []), value);
    }
    return `<div class="value-array">${value.map((item, i) => `<section><h4>${e(t('item'))} ${i + 1}</h4>${valueView(item, '', depth + 1)}</section>`).join('')}</div>`;
  }
  if (!isRecord(value)) return scalarView(value, key);
  const all = Object.entries(value), storage = all.filter(([name]) => !literal && depth === 0 && storageKeys.has(name)), entries = all.filter(([name]) => !storage.some(([key]) => key === name)), plain = entries.filter(([, item]) => !isRecord(item) && !Array.isArray(item)), groups = entries.filter(([, item]) => isRecord(item) || Array.isArray(item));
  if (!all.length) return `<p class="empty-inline">${e(t('noProperties'))}</p>`;
  return `${plain.length ? `<dl class="property-list">${plain.map(([name, item]) => `<div><dt>${e(literal ? name : label(name))}</dt><dd>${scalarView(item, name)}</dd>${literal ? `<span class="value-type">${e(valueType(item))}</span>` : ''}</div>`).join('')}</dl>` : ''}${groups.map(([name, item]) => `<section class="property-group"><h3>${e(literal ? name : label(name))}${Array.isArray(item) ? ` <span class="group-count">${item.length}</span>` : ''}</h3>${valueView(item, name, depth + 1, ['fields', 'labels', 'metadata', 'aliases'].includes(name))}</section>`).join('')}${storage.length ? `<details class="structured-details"><summary>${e(t('storageMetadata'))}</summary>${valueView(Object.fromEntries(storage), '', depth + 1)}</details>` : ''}`;
}

export const structuredView = (value, raw = true) => `<div class="structured-view">${valueView(value)}${raw && typeof value === 'object' && value !== null ? rawDetails(value) : ''}</div>`;
export const details = (title, value, open = false) => `<details class="structured-details" ${open ? 'open' : 'data-detail-view="structured"'}><summary>${e(t(title))}</summary>${open ? structuredView(value) : detailPayload(value)}</details>`;

export function bindDetails(root) {
  if (root.dataset.detailsBound) return;
  root.dataset.detailsBound = 'true';
  root.addEventListener('toggle', event => {
    const target = event.target;
    if (!target.open || !target.dataset.detailView) return;
    const payload = target.querySelector(':scope > script[data-detail-payload]');
    if (!payload) return;
    try {
      const value = parseJSON(payload.textContent);
      payload.insertAdjacentHTML('afterend', target.dataset.detailView === 'raw' ? pre(value) : structuredView(value));
      payload.remove();
    } catch (error) { payload.insertAdjacentHTML('afterend', `<p class="form-error" role="alert">${e(error.message)}</p>`); }
  }, true);
}

export function taskView(task) {
  const total = Number(task.progress_total), completed = Number(task.progress_completed || 0);
  const result = task.result || {}, summary = Object.fromEntries(Object.entries(result).filter(([key, value]) => !isRecord(value) && !Array.isArray(value) && !storageKeys.has(key)));
  for (const key of ['indexes', 'entity_pages', 'edge_shards']) if (Array.isArray(result[key])) summary[key + '_count'] = result[key].length;
  return `<div class="task-heading"><h2>${e(t(task.type))}</h2>${badge(task.status)}</div>${total > 0 && Number.isFinite(total) ? `<div class="task-progress"><span>${e(t('progress'))}</span><strong>${e(task.progress_completed || 0)} / ${e(task.progress_total)}</strong><progress max="${total}" value="${Math.max(0, Math.min(completed, total))}" aria-label="${e(t('progress'))}"></progress></div>` : ''}${valueView({ id: task.id, tenant_id: task.tenant_id, phase: task.phase })}<section class="property-group"><h3>${e(t('timeline'))}</h3>${valueView({ started_at: task.started_at, updated_at: task.updated_at, finished_at: task.finished_at })}</section>${task.error ? `<p class="form-error">${e(task.error)}</p>` : ''}<section class="property-group"><h3>${e(t('parameters'))}</h3>${valueView(task.params || {})}</section><section class="property-group"><h3>${e(t('result'))}</h3>${valueView(summary)}${Object.keys(result).length ? details('moreDetails', result) : ''}</section>${task.checkpoint ? details('checkpoint', task.checkpoint) : ''}${rawDetails(task)}`;
}
