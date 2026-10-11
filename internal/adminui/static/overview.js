import { t, language } from './i18n.js';
import { tenant, scalar } from './api.js';
import { e, button, structuredView, dialog, download } from './ui.js';

const colors = { blue: '#2563eb', teal: '#0d9488', violet: '#8b5cf6', amber: '#d97706', red: '#dc2626', gray: '#94a3b8', green: '#16a34a' };
const number = value => value == null || value === '' || typeof value === 'boolean' ? null : Number.isFinite(Number(scalar(value))) && Number(scalar(value)) >= 0 ? Number(scalar(value)) : null;
const count = value => value == null ? '—' : e(scalar(value));
const bytes = value => {
  const n = number(value);
  if (n === null) return '—';
  const unit = n ? Math.max(0, Math.min(4, Math.floor(Math.log(n) / Math.log(1024)))) : 0;
  return `${new Intl.NumberFormat(language, { maximumFractionDigits: unit ? 1 : 0 }).format(n / 1024 ** unit)} ${['B', 'KiB', 'MiB', 'GiB', 'TiB'][unit]}`;
};
const dot = color => `<svg width="8" height="8" aria-hidden="true"><circle cx="4" cy="4" r="4" fill="${color}"/></svg>`;
const legend = items => `<div class="chart-legend">${items.map(item => `<span>${dot(item.color)}${e(item.label)} <strong>${e(item.display ?? item.value)}</strong></span>`).join('')}</div>`;
const unavailable = (hint = 'chartUnavailable') => `<div class="chart-unavailable"><svg viewBox="0 0 160 90" aria-hidden="true"><path d="M10 75H150M10 10V75" stroke="#cbd5e1" fill="none"/><path d="M25 65L55 40L90 52L130 20" stroke="#cbd5e1" stroke-width="3" stroke-dasharray="5 5" fill="none"/></svg><p>${e(t(hint))}</p></div>`;

function panel(title, source, body, extra = '', subtitle = '') {
  return `<section class="overview-panel ${extra}" aria-label="${e(t(title))}"><header><div><h2>${e(t(title))}</h2>${subtitle ? `<p>${e(subtitle)}</p>` : ''}</div>${button('inspect', 'inspect:' + source, 'small')}</header>${body}</section>`;
}

function donut(title, items, value, label) {
  const total = items.reduce((sum, item) => sum + item.value, 0);
  let offset = 0;
  const arcs = items.filter(item => item.value > 0).map(item => {
    const size = item.value / total * 100;
    const arc = `<circle cx="100" cy="100" r="72" pathLength="100" fill="none" stroke="${item.color}" stroke-width="17" stroke-dasharray="${size} ${100 - size}" stroke-dashoffset="${-offset}" transform="rotate(-90 100 100)"><title>${e(item.label)}: ${e(item.display ?? item.value)}</title></circle>`;
    offset += size;
    return arc;
  }).join('');
  return `<div class="donut-chart"><svg viewBox="0 0 200 200" role="img" aria-label="${e(t(title))}"><title>${e(items.map(item => `${item.label}: ${item.display ?? item.value}`).join(', '))}</title><circle cx="100" cy="100" r="72" fill="none" stroke="#edf1f7" stroke-width="17"/>${arcs}<text x="100" y="98" text-anchor="middle" class="chart-value">${e(value)}</text><text x="100" y="122" text-anchor="middle" class="chart-label">${e(label)}</text></svg></div>${legend(items)}`;
}

function bars(title, items, format, axis = true) {
  const max = Math.max(...items.map(item => number(item.value) ?? 0), 1);
  const description = items.map(item => `${item.label}: ${format(item.value)}`).join(', ');
  return `<figure class="bar-chart" role="img" aria-label="${e(t(title) + ': ' + description)}">${items.map(item => {
    const n = number(item.value);
    return `<div class="chart-bar"><span>${e(item.label)}</span><svg viewBox="0 0 100 17" preserveAspectRatio="none" aria-hidden="true"><rect width="100" height="17" rx="1" fill="#f1f5fa"/>${n === null ? '' : `<rect width="${Math.min(n / max, 1) * 100}" height="17" rx="1" fill="${item.color}"/>`}</svg><strong>${e(format(item.value))}</strong></div>`;
  }).join('')}${axis ? `<div class="chart-scale"><span></span><div><span>0</span><span>${e(format(max / 2))}</span><span>${e(format(max))}</span></div><span></span></div>` : ''}</figure>`;
}

function healthChart(reports) {
  return `<div class="health-chart">${['health', 'readiness', 'diagnostics'].map(name => {
    const report = reports[name], value = report?.value;
    const status = report?.status !== 'fulfilled' ? report?.reason?.status === 503 ? 'degraded' : 'unknown' : value?.ready === false ? 'degraded' : value?.status || 'unknown';
    const healthy = ['ok', 'ready'].includes(status), color = healthy ? colors.green : status === 'unknown' ? colors.gray : colors.amber;
    return `<button class="health-gauge" type="button" data-action="inspect:${name}" aria-label="${e(t(name) + ': ' + t(status))}"><svg viewBox="0 0 100 100" role="img" aria-label="${e(t(status))}"><circle cx="50" cy="50" r="35" fill="#fff" stroke="#edf1f7" stroke-width="8"/><circle cx="50" cy="50" r="35" fill="none" stroke="${color}" stroke-width="8" ${healthy ? '' : 'stroke-dasharray="5 5"'}/><text x="50" y="60" text-anchor="middle" font-size="28" fill="${color}">${healthy ? '✓' : status === 'unknown' ? '?' : '!'}</text></svg><span>${e(t(name))}</span><strong>${e(t(status))}</strong></button>`;
  }).join('')}</div>`;
}

function diskChart(diagnostics) {
  const disk = diagnostics?.disk, total = number(disk?.total_bytes), available = number(disk?.available_bytes);
  if (!total || available === null || available > total) return unavailable();
  const used = total - available, percent = new Intl.NumberFormat(language, { maximumFractionDigits: 1 }).format(used / total * 100);
  return donut('diskCapacity', [{ label: t('diskUsed'), value: used, display: bytes(used), color: disk.write_ready === false ? colors.red : colors.blue }, { label: t('diskAvailable'), value: available, display: bytes(available), color: '#dbe7fc' }], percent + '%', t('diskUsed')) + `<p class="chart-note">${e(t('diskTotal'))} ${e(bytes(total))} · ${e(t(disk.write_ready === false ? 'diskBlocked' : 'diskData'))}</p>`;
}

function storageChart(usage) {
  if (!usage || !Array.isArray(usage.categories)) return unavailable(tenant ? 'chartUnavailable' : 'tenantRequired');
  if (!usage.categories.length) return donut('storageComposition', [], bytes(usage.total_bytes), t('usage'));
  if (usage.categories.some(item => number(item.bytes) === null)) return unavailable();
  const sorted = [...usage.categories].sort((a, b) => number(b.bytes) - number(a.bytes));
  const items = sorted.slice(0, 6).map((item, i) => ({ label: t('storage_' + item.name) === 'storage_' + item.name ? item.name : t('storage_' + item.name), value: item.bytes, color: [colors.blue, colors.teal, colors.violet, '#38bdf8', colors.amber, '#64748b'][i] }));
  if (sorted.length > 6) items.push({ label: t('otherCategories'), value: sorted.slice(6).reduce((sum, item) => sum + number(item.bytes), 0), color: colors.gray });
  return `<div class="chart-headline"><strong>${e(bytes(usage.total_bytes))}</strong><span>${count(usage.object_count)} ${e(t('objects'))}${usage.stale ? ' · ' + e(t('stale')) : ''}</span></div>${bars('storageComposition', items, bytes)}`;
}

function admissionChart(diagnostics) {
  const admission = diagnostics?.admission;
  if (!admission) return unavailable();
  return `<svg class="admission-chart" viewBox="0 0 360 260" role="img" aria-label="${e(t('admissionLoad'))}">${['query', 'read', 'write'].map((key, i) => {
    const row = admission[key], active = number(row?.active), limit = number(row?.global_limit), y = i * 78 + 28;
    const valid = active !== null && limit > 0 && row.enabled !== false;
    return `<g><title>${e(t('admission_' + key))}: ${count(row?.active)} / ${count(row?.global_limit)}, ${e(t('waiting'))}: ${count(row?.waiting)}</title><text x="0" y="${y}" class="chart-label">${e(t('admission_' + key))}</text><text x="355" y="${y}" text-anchor="end" class="chart-label chart-number">${valid ? `${count(row.active)} / ${count(row.global_limit)}` : e(t(row?.enabled === false ? 'disabled' : 'chartUnavailable'))}</text><rect x="0" y="${y + 12}" width="355" height="14" rx="4" fill="#edf1f7"/>${valid ? `<rect x="0" y="${y + 12}" width="${Math.min(active / limit, 1) * 355}" height="14" rx="4" fill="${colors.blue}"/>` : ''}<text x="0" y="${y + 47}" class="chart-axis">${e(t('waiting'))} ${count(row?.waiting)}</text></g>`;
  }).join('')}</svg><p class="chart-note">${e(t('admissionHint'))}</p>`;
}

function tasksChart(diagnostics) {
  if (!tenant) return unavailable('tenantRequired');
  if (!diagnostics || diagnostics.tasks_error || !Object.hasOwn(diagnostics, 'tasks') || (diagnostics.tasks !== null && !Array.isArray(diagnostics.tasks))) return unavailable();
  const tasks = diagnostics.tasks ?? [];
  const statuses = ['succeeded', 'running', 'queued', 'failed', 'canceled', 'unknown'];
  const items = statuses.map((key, i) => ({ label: t(key), value: tasks.filter(task => (statuses.includes(task.status) ? task.status : 'unknown') === key).length, color: [colors.teal, colors.blue, colors.amber, colors.red, colors.gray, colors.violet][i] })).filter(item => item.value);
  return donut('taskDistribution', items, tasks.length, t('taskCount')) + `<p class="chart-note">${e(t('taskSample'))} ${count(diagnostics.tasks_limit)}</p>`;
}

function versionsChart(freshness, usage) {
  if (!freshness) return unavailable(tenant ? 'chartUnavailable' : 'tenantRequired');
  const items = [{ label: t('publishedVersion'), value: freshness.manifest_version, color: colors.blue }, { label: t('visibleVersion'), value: freshness.visible_version, color: colors.teal }, { label: t('snapshotVersion'), value: usage?.snapshot_version, color: colors.violet }];
  return bars('versionPosition', items, value => value == null ? '—' : scalar(value), false) + `<p class="chart-note">${e(t(freshness.fresh === true ? 'fresh' : freshness.fresh === false ? 'stale' : 'unknown'))} · ${e(t('versionLag'))} ${count(freshness.version_lag)}</p>`;
}

export async function overviewPage(ctx) {
  const checks = [['health', '/v1/health'], ['readiness', '/v1/readiness'], ['diagnostics', '/v1/diagnostics']];
  if (tenant) checks.push(['usage', '/v1/tenant-usage'], ['freshness', '/v1/control/reader-freshness']);
  const results = await Promise.allSettled(checks.map(([, path]) => ctx.get('admin', path)));
  if (!ctx.valid()) return;
  const reports = Object.fromEntries(checks.map(([name], i) => [name, results[i]]));
  const value = name => reports[name]?.status === 'fulfilled' ? reports[name].value : null;
  const diagnostics = value('diagnostics'), usage = value('usage'), freshness = value('freshness');
  ctx.connection(reports.health.status === 'fulfilled' ? 'connected' : 'disconnected');
  ctx.header(button('download', 'download'));
  ctx.html(`<div class="overview-caption"><span>${e(t('snapshotCharts'))}</span><span>${e(new Date().toLocaleTimeString(language))}</span></div><div class="overview-grid">${panel('systemStatus', 'diagnostics', healthChart(reports))}${panel('diskCapacity', 'diagnostics', diskChart(diagnostics))}${panel('taskDistribution', 'diagnostics', tasksChart(diagnostics))}${panel('storageComposition', 'usage', storageChart(usage), 'overview-wide', tenant)}${panel('admissionLoad', 'diagnostics', admissionChart(diagnostics))}${panel('versionPosition', 'freshness', versionsChart(freshness, usage), 'overview-full')}</div><div class="overview-tools">${button('metrics', 'metrics')}${button('trafficGate', 'traffic')}${button('writerLease', 'writer')}<p class="hint">${e(t('localHint'))}</p></div>`);
  ctx.action = async action => {
    if (action.startsWith('inspect:')) {
      const name = action.slice(8), report = reports[name];
      dialog(name, structuredView(report?.status === 'fulfilled' ? report.value : report?.reason?.message || t('tenantRequired')), async () => {}, 'close');
    } else if (['metrics', 'traffic', 'writer'].includes(action)) {
      const [path, label] = { metrics: ['/metrics', 'metrics'], traffic: ['/v1/control/reader-traffic-gate', 'trafficGate'], writer: ['/v1/control/writer-lease', 'writerLease'] }[action];
      const result = await ctx.get('admin', path, { raw: action === 'metrics' });
      if (ctx.valid()) dialog(label, structuredView(result), async () => {}, 'close');
    } else if (action === 'download') download(Object.fromEntries(checks.map(([name]) => [name, reports[name].status === 'fulfilled' ? reports[name].value : { error: reports[name].reason.message }])), 'graphdb-diagnostics.json');
  };
}
