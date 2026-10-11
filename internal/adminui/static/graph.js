import { e, notice } from './ui.js';
import { t } from './i18n.js';

export function graphView(entities, edges) {
  const nodes = new Map(entities.map(entity => [entity.id, entity]));
  for (const edge of edges) {
    if (!nodes.has(edge.from)) nodes.set(edge.from, { id: edge.from });
    if (!nodes.has(edge.to)) nodes.set(edge.to, { id: edge.to });
  }
  const visible = [...nodes.values()].slice(0, 100);
  const positions = new Map(visible.map((node, i) => {
    const angle = i / visible.length * Math.PI * 2;
    const ring = visible.length > 20 && i % 2 ? 100 : 160;
    return [node.id, [350 + Math.cos(angle) * ring * 1.6, 205 + Math.sin(angle) * ring]];
  }));
  return notice(t('graphHint')) + `<svg class="graph-canvas" viewBox="0 0 700 420" role="img" aria-label="${e(t('graph'))}"><defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M0 0L10 5L0 10Z" fill="#94a9c9"/></marker></defs>${edges.slice(0, 200).map(edge => {
    const a = positions.get(edge.from), b = positions.get(edge.to);
    return a && b ? `<line x1="${a[0]}" y1="${a[1]}" x2="${b[0]}" y2="${b[1]}" stroke="#a8bad4" stroke-width="1.5" marker-end="url(#arrow)"/><text x="${(a[0] + b[0]) / 2}" y="${(a[1] + b[1]) / 2 - 6}" text-anchor="middle">${e(edge.type)}</text>` : '';
  }).join('')}${visible.map(node => { const p = positions.get(node.id); return `<g class="graph-node" tabindex="0" role="button" aria-label="${e(node.id)}" data-action="node:${e(node.id)}"><circle cx="${p[0]}" cy="${p[1]}" r="9" fill="#2563eb" stroke="#fff" stroke-width="3"/><text x="${p[0]}" y="${p[1] + 25}" text-anchor="middle">${e(node.id.length > 28 ? node.id.slice(0, 26) + '…' : node.id)}</text></g>`; }).join('')}</svg>`;
}

export function resultGraph(data) {
  const entities = [], edges = [];
  for (const item of data.results || []) {
    if (item.entity) entities.push(item.entity);
    if (item.edge) edges.push(item.edge);
    if (item.path) { entities.push(...(item.path.entities || [])); edges.push(...(item.path.edges || [])); }
  }
  return graphView(entities, edges);
}
