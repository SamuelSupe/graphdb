import { t } from './i18n.js';
import { json, parseJSON } from './api.js';
import { e, isRecord, valueType, label } from './display.js';

const mapKeys = new Set(['fields', 'labels', 'metadata', 'aliases', 'params', 'backup', 'backpressure', 'quota', 'maintenance', 'indexes']);
const configKeys = { backup: ['enabled', 'interval_seconds', 'keep_count', 'max_age_seconds', 'retry_initial_seconds', 'retry_max_seconds', 'restore_drill_interval_seconds'], backpressure: ['object_latency_threshold_ms', 'cas_conflict_window_ms', 'cas_conflict_threshold', 'max_commit_tail', 'retry_after_ms'], quota: ['max_entities_per_tenant', 'max_edges_per_tenant'], maintenance: ['auto_compact', 'compact_commit_tail_threshold', 'compact_object_count_threshold', 'compact_bytes_threshold', 'small_file_object_threshold', 'small_file_bytes_threshold', 'entity_page_split_threshold', 'entity_page_merge_threshold', 'edge_shard_split_threshold', 'edge_shard_merge_threshold', 'gc_interval_seconds', 'keep_snapshots', 'deadletter_max_age_seconds', 'task_max_age_seconds', 'cleanup_index_orphans'], indexes: ['auto_rebuild', 'rebuild_on_stale'] };
const booleanKeys = new Set(['enabled', 'auto_compact', 'auto_rebuild', 'rebuild_on_stale', 'cleanup_index_orphans']);
const arraySamples = { sources: { name: '', priority: 0, description: '' }, field_aliases: { source: '', kind: '', aliases: {} }, field_priorities: { source: '', kind: '', fields: {} }, upsert_entities: { id: '', kind: '', fields: {} }, upsert_edges: { type: '', from: '', to: '', fields: {} }, identity_keys: { name: '', fields: [''], strategy: '', confidence_threshold: 0 } };
const types = ['string', 'number', 'boolean', 'null', 'object', 'array'];
const defaults = { string: '', number: 0, boolean: false, null: null, object: {}, array: [] };

function emptyItem(value) {
  if (isRecord(value)) return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, emptyItem(item)]));
  return defaults[valueType(value)];
}

export function parseField(type, value) {
  if (type === 'string') return value;
  if (type === 'null') return null;
  if (value === '' && type === 'number') return undefined;
  const result = parseJSON(value);
  if (valueType(result) !== type) throw new Error(t('invalidFieldValue'));
  return result;
}

function primitive(value, name, key) {
  const type = valueType(value);
  const extra = type === 'null' || ['expected_version', 'idempotency_key'].includes(key) ? 'readonly' : '';
  const inputLabel = e(key ? label(key) : t('fieldValue'));
  const input = type === 'boolean' ? `<select data-value-input name="${e(name)}" aria-label="${inputLabel}"><option value="true" ${value ? 'selected' : ''}>${e(t('yes'))}</option><option value="false" ${!value ? 'selected' : ''}>${e(t('no'))}</option></select>` : `<input data-value-input name="${e(name)}" aria-label="${inputLabel}" value="${e(value ?? '')}" ${type === 'number' ? 'inputmode="decimal"' : ''} ${extra}>`;
  return `<div data-node="${type}">${input}</div>`;
}

function entry(key, value, name, dynamic = false, group = '') {
  const hints = configKeys[group];
  const choices = hints && !hints.includes(key) && key ? [key, ...hints] : hints;
  const keyControl = hints ? `<select data-key data-config-group="${e(group)}" aria-label="${e(t('setting'))}" required><option value="">${e(t('selectSetting'))}</option>${choices.map(item => `<option value="${e(item)}" ${key === item ? 'selected' : ''}>${e(label(item))}</option>`).join('')}</select>` : `<input data-key aria-label="${e(t('fieldName'))}" placeholder="${e(t('fieldName'))}" value="${e(key)}" required>`;
  const typeControl = `<select data-type aria-label="${e(t('valueType'))}">${(group === 'labels' || group === 'aliases' ? ['string'] : types).map(type => `<option value="${type}" ${valueType(value) === type ? 'selected' : ''}>${e(type)}</option>`).join('')}</select>`;
  return `<div class="editor-entry ${dynamic ? 'map-entry' : ''}" data-entry>${dynamic ? `<div class="map-entry-header">${keyControl}${typeControl}<button type="button" class="small remove-field" data-form-op="remove">${e(t('remove'))}</button></div>` : `<input data-key type="hidden" value="${e(key)}"><span class="editor-label">${e(label(key))}</span>`}${node(value, name, key)}</div>`;
}

function node(value, name, key = '') {
  if (Array.isArray(value)) {
    const sample = arraySamples[key] ?? emptyItem(value[0] ?? '');
    return `<div data-node="array" class="array-editor"><div class="array-entries">${value.map((item, index) => `<div class="editor-item"><div class="editor-item-title"><span>${e(t('item'))} ${index + 1}</span><button type="button" class="small" data-form-op="remove">${e(t('remove'))}</button></div>${node(item, name + ':' + index)}</div>`).join('')}</div><p class="hint array-empty">${e(t('noItems'))}</p><button type="button" class="small" data-form-op="array-add">${e(t('addItem'))}</button><template>${node(sample, name + ':new')}</template></div>`;
  }
  if (!isRecord(value)) return primitive(value, name, key);
  const dynamic = mapKeys.has(key);
  const schemaFields = key === 'fields' && (name.startsWith('definition') || name.includes('upsert_entity_types') || Object.values(value).some(item => isRecord(item) && typeof item.type === 'string'));
  const fieldDefault = schemaFields ? { type: 'string', required: false, indexed: false, unique: false } : name.includes('field_priorities') ? 0 : '';
  const systemKeys = new Set(['field_sources', 'existence_source', 'field_write_modes', 'version', 'created_at', 'updated_at']);
  const system = child => /upsert_(entities|edges):\d+$/.test(name) && systemKeys.has(child);
  const render = ([child, item]) => entry(child, item, name + ':' + encodeURIComponent(child), dynamic, key);
  const entries = Object.entries(value), metadata = entries.filter(([child]) => system(child));
  return `<div data-node="object" class="object-fields ${dynamic ? 'map-editor' : ''}"><div class="editor-entries">${configKeys[key] && !entries.length ? `<p class="hint">${e(t('inheritConfig'))}</p>` : ''}${entries.filter(([child]) => !system(child)).map(render).join('')}</div>${metadata.length ? `<details class="editor-system"><summary>${e(t('recordInformation'))}</summary><div class="editor-entries">${metadata.map(render).join('')}</div></details>` : ''}${dynamic ? `<button type="button" class="small" data-form-op="map-add">${e(t('addField'))}</button><template>${entry('', fieldDefault, name + ':new', true, key)}</template>` : ''}</div>`;
}

export function objectEditor(name, value, title = 'request') {
  return `<div class="object-editor" data-editor="${e(name)}"><h3>${e(t(title))}</h3><textarea name="${e(name)}" class="object-source" hidden>${e(json(value))}</textarea><div class="object-body">${node(value, name, name === 'params' ? 'params' : '')}</div><details class="advanced-editor"><summary>${e(t('advancedJSON'))}</summary><p class="hint">${e(t('advancedJSONHint'))}</p><textarea class="code object-raw" aria-label="${e(t(title || 'request') + ' · ' + t('advancedJSON'))}" name="${e(name)}-raw" rows="14" spellcheck="false">${e(json(value))}</textarea></details><p class="form-error editor-error" role="alert"></p></div>`;
}

function readNode(target) {
  const type = target.dataset.node;
  if (type === 'array') return [...target.querySelector('.array-entries').children].map(item => readNode(item.querySelector('[data-node]')));
  if (type === 'object') {
    const rows = [...target.querySelectorAll(':scope > .editor-entries > [data-entry], :scope > .editor-system > .editor-entries > [data-entry]')];
    const pairs = rows.map(item => {
      const key = item.querySelector('[data-key]').value;
      if (!key) throw new Error(t('requiredFields'));
      return [key, readNode(item.querySelector('[data-node]'))];
    });
    if (new Set(pairs.map(([key]) => key)).size !== pairs.length) throw new Error(t('duplicateField'));
    return Object.fromEntries(pairs.filter(([, value]) => value !== undefined));
  }
  return parseField(type, target.querySelector('[data-value-input]').value);
}

function readWidget(widget) {
  const advanced = widget.querySelector('.advanced-editor');
  const value = advanced.open ? parseJSON(widget.querySelector('.object-raw').value) : readNode(widget.querySelector('.object-body > [data-node]'));
  if (!isRecord(value)) throw new Error(t('jsonObject'));
  widget.querySelector('.object-source').value = json(value);
  return value;
}

export function readEditor(form, name) {
  const widget = [...form.querySelectorAll('.object-editor')].find(item => item.dataset.editor === name);
  return widget ? readWidget(widget) : null;
}

export function captureEditors(root) {
  for (const widget of root.querySelectorAll('.object-editor')) readWidget(widget);
}

export function bindObjectEditors(root, restore = false) {
  for (const widget of root.querySelectorAll('.object-editor')) {
    const name = widget.dataset.editor, source = widget.querySelector('.object-source'), body = widget.querySelector('.object-body'), raw = widget.querySelector('.object-raw'), advanced = widget.querySelector('.advanced-editor'), error = widget.querySelector('.editor-error');
    if (restore) { body.innerHTML = node(parseJSON(source.value), name, name === 'params' ? 'params' : ''); raw.value = source.value; }
    if (widget.dataset.bound) continue;
    widget.dataset.bound = 'true';
    const sync = (showError = true) => { try { readWidget(widget); error.textContent = ''; } catch (cause) { error.textContent = showError ? cause.message : ''; } };
    let validationTimer;
    const validateInput = () => {
      clearTimeout(validationTimer);
      validationTimer = setTimeout(() => { if (widget.isConnected) sync(); }, 150);
    };
    body.oninput = validateInput;
    raw.oninput = validateInput;
    body.onchange = event => {
      if (event.target.matches('[data-key]') && configKeys[event.target.dataset.configGroup]?.includes(event.target.value)) {
        const row = event.target.closest('[data-entry]'), input = row.querySelector('[data-value-input]');
        if (input?.value === '' && row.querySelector('[data-type]').value === 'string') {
          const type = booleanKeys.has(event.target.value) ? 'boolean' : 'number';
          row.querySelector('[data-type]').value = type;
          row.querySelector('[data-node]').outerHTML = node(defaults[type], name + ':new', event.target.value);
        }
      }
      if (event.target.matches('[data-type]')) {
        const row = event.target.closest('[data-entry]'), type = event.target.value;
        row.querySelector('[data-node]').outerHTML = node(defaults[type], name + ':changed', row.querySelector('[data-key]').value);
      }
      sync();
    };
    body.onclick = event => {
      const button = event.target.closest('[data-form-op]');
      if (!button) return;
      if (button.dataset.formOp === 'remove') {
        const item = button.closest('[data-entry], .editor-item'), list = item.parentElement;
        item.remove();
        if (list.classList.contains('array-entries')) [...list.children].forEach((row, index) => { row.querySelector('.editor-item-title > span').textContent = t('item') + ' ' + (index + 1); });
      }
      else {
        const container = button.closest('[data-node]'), template = container.querySelector(':scope > template');
        const list = container.querySelector(':scope > .editor-entries, :scope > .array-entries');
        if (button.dataset.formOp === 'map-add') list.insertAdjacentHTML('beforeend', template.innerHTML);
        else list.insertAdjacentHTML('beforeend', `<div class="editor-item"><div class="editor-item-title"><span>${e(t('item'))} ${list.children.length + 1}</span><button type="button" class="small" data-form-op="remove">${e(t('remove'))}</button></div>${template.innerHTML}</div>`);
        list.lastElementChild.querySelector('input:not([type="hidden"]), select')?.focus();
      }
      sync(button.dataset.formOp === 'remove');
    };
    let wasOpen = false;
    advanced.ontoggle = () => {
      if (advanced.open === wasOpen) return;
      try {
        if (advanced.open) { source.value = json(readNode(body.querySelector('[data-node]'))); raw.value = source.value; }
        else { const value = parseJSON(raw.value); if (!isRecord(value)) throw new Error(t('jsonObject')); source.value = json(value); body.innerHTML = node(value, name, name === 'params' ? 'params' : ''); }
        wasOpen = advanced.open;
        for (const input of body.querySelectorAll('input, select, button')) input.disabled = wasOpen;
        error.textContent = '';
      } catch (cause) { error.textContent = cause.message; advanced.open = wasOpen; }
    };
  }
}
