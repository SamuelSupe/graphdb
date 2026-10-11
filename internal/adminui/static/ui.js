import { t } from './i18n.js';
import { json, parseJSON, request, settings, tenant } from './api.js';
import { e, structuredView, bindDetails } from './display.js';
import { objectEditor, bindObjectEditors, readEditor } from './forms.js';
export { e, badge, table, pre, details, rawDetails, structuredView, valueView, fieldSummary, dateView, taskView } from './display.js';

export const button = (label, action, kind = '', extra = '') => `<button type="button" class="${kind}" data-action="${e(action)}" ${extra}>${e(t(label))}</button>`;
export const field = (key, value = '', type = 'text', extra = '') => `<label class="field"><span>${e(t(key))}</span><input name="${e(key)}" type="${type}" value="${e(value)}" ${extra}></label>`;
export const select = (key, values, current = '', extra = '') => `<label class="field"><span>${e(t(key))}</span><select name="${e(key)}" ${extra}>${values.map(value => { const [id, label] = Array.isArray(value) ? value : [value, t(value)]; return `<option value="${e(id)}" ${id === current ? 'selected' : ''}>${e(label)}</option>`; }).join('')}</select></label>`;
export const editor = (name, value, label = 'request', rows = 14) => `<label class="field editor-field"><span>${e(t(label))}</span><textarea class="code" name="${e(name)}" rows="${rows}" spellcheck="false" autocapitalize="off" autocomplete="off">${e(typeof value === 'string' ? value : json(value))}</textarea></label>`;
export const checkbox = (key, checked = false) => `<label class="check"><input type="checkbox" name="${e(key)}" ${checked ? 'checked' : ''}>${e(t(key))}</label>`;
export const tabs = (values, current) => `<div class="tabs" role="tablist">${values.map(value => `<button type="button" role="tab" aria-selected="${value === current}" tabindex="${value === current ? '0' : '-1'}" data-action="tab:${e(value)}">${e(t(value))}</button>`).join('')}</div>`;
export const notice = text => `<p class="notice">${e(text)}</p>`;
export function link(value, action) { return `<button type="button" class="text-button" data-action="${e(action)}">${e(value)}</button>`; }
export function jsonInput(form, name) {
  try { return parseJSON(form.elements.namedItem(name).value); } catch (error) { throw new Error(t('invalidJSON') + ' ' + error.message); }
}
export function objectInput(form, name) {
  const value = readEditor(form, name) ?? jsonInput(form, name);
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(t('jsonObject'));
  return value;
}
export function toast(message, error = false) {
  const target = document.querySelector('#toast');
  target.textContent = message;
  target.className = error ? 'visible error' : 'visible';
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { target.className = ''; }, error ? 9000 : 4500);
}
export function download(value, filename, type = 'application/json') {
  const url = URL.createObjectURL(new Blob([typeof value === 'string' ? value : json(value)], { type }));
  const anchor = document.createElement('a');
  anchor.href = url; anchor.download = filename; anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function dialog(title, content, onSubmit, submit = 'save', destructive = false) {
  const target = document.querySelector('#dialog');
  bindDetails(target);
  target.onclick = null;
  target.setAttribute('aria-labelledby', 'dialog-heading');
  target.innerHTML = `<form><div class="dialog-title"><h2 id="dialog-heading">${e(t(title))}</h2><button type="button" data-close aria-label="${e(t('close'))}">×</button></div><div class="dialog-content">${content}<p class="form-error" role="alert"></p></div><footer>${submit === 'close' ? '' : button('cancel', 'close')}<button type="submit" class="${destructive ? 'danger' : 'primary'}">${e(t(submit))}</button></footer></form>`;
  const form = target.querySelector('form');
  bindObjectEditors(form);
  let working = false;
  const close = () => { if (!working) target.close(); };
  target.querySelector('[data-close]').onclick = close;
  const cancelButton = target.querySelector('[data-action="close"]');
  if (cancelButton) cancelButton.onclick = close;
  target.oncancel = event => { if (working) event.preventDefault(); };
  form.onsubmit = async event => {
    event.preventDefault();
    if (working) return;
    working = true;
    const submitButton = form.querySelector('[type="submit"]');
    submitButton.disabled = true;
    form.querySelector('.form-error').textContent = '';
    try { await onSubmit(form); target.close(); } catch (error) { form.querySelector('.form-error').textContent = error.message; }
    finally { working = false; submitButton.disabled = false; }
  };
  target.showModal();
}

export function confirmRequest(ctx, label, endpoint, path, body, options = {}) {
  const tenantID = options.tenantID ?? tenant;
  const resource = options.resource || tenantID;
  const method = options.method || 'POST';
  const destination = settings[endpoint] + path;
  return new Promise(resolve => {
    const content = notice(t(options.destructive ? 'destructive' : 'confirmDesc')) + `<dl class="facts"><dt>${e(t('tenant'))}</dt><dd>${e(tenantID)}</dd><dt>${e(t('destination'))}</dt><dd class="code">${e(method + ' ' + destination)}</dd></dl>` + (body === undefined ? '' : options.readOnly ? structuredView(body) : objectEditor('body', body)) + (options.destructive ? field('typeConfirm', '', 'text', 'required autocomplete="off"') + `<p class="hint code">${e(resource)}</p>` : '');
    dialog(label, content, async form => {
      if (options.destructive && form.elements.namedItem('typeConfirm').value !== resource) throw new Error(t('confirmMismatch'));
      const result = await request(endpoint, path, { method, body: body === undefined ? undefined : options.readOnly ? body : objectInput(form, 'body'), tenantID, headers: options.headers });
      toast(t(result.status === 202 ? 'accepted' : 'success'));
      await options.after?.(result.data);
      ctx.result?.(result.data);
      resolve(result);
    }, 'execute', options.destructive);
    document.querySelector('#dialog').addEventListener('close', () => resolve(null), { once: true });
  });
}

export function bindActions(root, handler) {
  bindDetails(root);
  root.onkeydown = event => {
    const tab = event.target.closest('[role="tab"]');
    if (!tab || !['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const items = [...tab.parentElement.querySelectorAll('[role="tab"]')], index = items.indexOf(tab);
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1 : (index + (event.key === 'ArrowRight' ? 1 : -1) + items.length) % items.length;
    items[next].click();
  };
  root.onclick = async event => {
    const target = event.target.closest('[data-action]');
    if (!target || !root.contains(target) || target.disabled) return;
    const action = target.dataset.action;
    const pending = target.dataset.pending;
    if (pending) return;
    target.dataset.pending = 'true';
    try { await handler(action, target); } catch (error) { if (error.name !== 'AbortError') toast(error.message, true); }
    finally {
      delete target.dataset.pending;
      if (target.matches('[role="tab"]')) [...root.querySelectorAll('[role="tab"][aria-selected="true"]')].find(tab => tab.dataset.action === action)?.focus();
    }
  };
}
