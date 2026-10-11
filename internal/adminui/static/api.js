import { readPreference, savePreference, t } from './i18n.js';

const gatewayPrefix = location.pathname.endsWith('/admin/ui/') ? '/admin' : '';
const defaults = { data: location.origin, admin: location.origin + gatewayPrefix, router: '', raft: '', clusterID: '' };
let stored;
try { stored = JSON.parse(readPreference('graphdb.endpoints', '{}')); } catch { stored = {}; }
export const settings = { ...defaults, ...stored, token: '', routerToken: '', raftToken: '' };
export let tenant = readPreference('graphdb.tenant', '');
export function setTenant(value) { tenant = value; savePreference('graphdb.tenant', value); }

export function endpointURL(value, optional = false) {
  if (!value && optional) return '';
  if (!value || (!value.startsWith('/') && !/^https?:\/\//i.test(value))) throw new Error(t('invalidURL'));
  const url = new URL(value, location.origin);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error(t('invalidURL'));
  return url.href.replace(/\/$/, '');
}
export function saveSettings(values) {
  const endpoints = { data: endpointURL(values.data), admin: endpointURL(values.admin), router: endpointURL(values.router, true), raft: endpointURL(values.raft, true), clusterID: values.clusterID };
  Object.assign(settings, endpoints, { token: values.token.trim(), routerToken: values.routerToken.trim(), raftToken: values.raftToken.trim() });
  savePreference('graphdb.endpoints', JSON.stringify(endpoints));
}

// Database integers can exceed JavaScript's safe range. Keep their original
// numeric tokens when inspecting or editing a response instead of rounding.
export function parseJSON(text) {
  return JSON.parse(text, (_key, value, context) => {
    if (typeof value === 'number' && Math.abs(value) > Number.MAX_SAFE_INTEGER) {
      if (!JSON.rawJSON || !context?.source) throw new Error(t('browserJSON'));
      return JSON.rawJSON(context.source);
    }
    return value;
  });
}
export const json = value => JSON.stringify(value, null, 2);
export const scalar = value => JSON.isRawJSON?.(value) ? value.rawJSON : (typeof value === 'object' && value !== null ? JSON.stringify(value) : String(value ?? '—'));
export const pathID = value => encodeURIComponent(value);
export const params = values => {
  const query = new URLSearchParams(Object.entries(values).filter(([, value]) => value !== '' && value !== undefined));
  return query.size ? '?' + query.toString() : '';
};

export async function request(endpoint, path, options = {}) {
  const { method = 'GET', body, signal, headers = {}, raw = false, tenantID = tenant } = options;
  const base = settings[endpoint];
  if (!base) throw new Error(t('clusterHint'));
  const token = endpoint === 'raft' ? settings.raftToken : endpoint === 'router' ? settings.routerToken : settings.token;
  const requestHeaders = { Accept: raw ? '*/*' : 'application/json', ...headers };
  if (tenantID) requestHeaders['X-Tenant-ID'] = tenantID;
  if (token) requestHeaders.Authorization = 'Bearer ' + token;
  if (endpoint === 'raft' && settings.clusterID) requestHeaders['X-Raft-Cluster'] = settings.clusterID;
  if (body !== undefined && !requestHeaders['Content-Type']) requestHeaders['Content-Type'] = 'application/json';
  const timeout = AbortSignal.timeout(method === 'GET' ? 60000 : 600000);
  const combined = signal ? AbortSignal.any([signal, timeout]) : timeout;
  let response;
  try {
    response = await fetch(base.replace(/\/$/, '') + path, { method, headers: requestHeaders, body: body === undefined ? undefined : typeof body === 'string' || body instanceof Blob ? body : json(body), signal: combined, credentials: 'same-origin', redirect: 'error' });
  } catch (error) {
    if (signal?.aborted) throw error;
    throw new Error(t('unknownResult'), { cause: error });
  }
  const text = await response.text();
  let data = text;
  const contentType = response.headers.get('Content-Type')?.split(';')[0].toLowerCase();
  if (text && (!raw || !response.ok) && (contentType === 'application/json' || contentType?.endsWith('+json'))) data = parseJSON(text);
  if (!response.ok) {
    const error = new Error(`${response.status} · ${data?.message || data?.error || text || response.statusText}`);
    error.status = response.status;
    error.code = data?.code;
    throw error;
  }
  return { data: raw ? text : data, status: response.status, headers: response.headers };
}
