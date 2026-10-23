// Widgets de gráfico del copiloto (partials/widget_chart.html):
// <canvas data-js-widget-chart='{"type":..,"title":..,"payload":{labels,values}}'>.
// Los widgets nuevos llegan en un swap OOB (#agent-grid en partials/agent_chat_turn.html):
// htmx 1.9 emite htmx:afterSwap también sobre cada elemento OOB, así que core/content.js
// los recibe como `root`.
import { onContent } from '../core/content.js';

const PALETTE_DARK = ['#60a5fa', '#f87171', '#34d399', '#fbbf24', '#a78bfa', '#22d3ee', '#94a3b8'];
const PALETTE_LIGHT = ['#0b2d42', '#cf1f27', '#2a6f97', '#4c9a8c', '#d97706', '#7c3aed', '#0891b2', '#64748b'];

// Dibuja el porcentaje sobre cada porción de pie/doughnut.
const percentPlugin = {
  id: 'percentLabels',
  afterDatasetsDraw(chart) {
    const t = chart.config.type;
    if (t !== 'pie' && t !== 'doughnut') return;
    const ctx = chart.ctx;
    const meta = chart.getDatasetMeta(0);
    const data = chart.data.datasets[0].data || [];
    const total = data.reduce((a, b) => a + (Number(b) || 0), 0);
    if (total <= 0) return;
    ctx.save();
    ctx.font = 'bold 12px sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    meta.data.forEach((arc, i) => {
      const pct = Math.round((Number(data[i]) || 0) / total * 100);
      const mid = (arc.startAngle + arc.endAngle) / 2;
      const radius = t === 'pie' ? arc.outerRadius * 0.6 : (arc.outerRadius + arc.innerRadius) / 2;
      const x = Math.cos(mid) * radius + arc.x;
      const y = Math.sin(mid) * radius + arc.y;
      const txt = pct + '%';
      ctx.strokeStyle = 'rgba(15, 23, 42, 0.6)';
      ctx.lineWidth = 2;
      ctx.strokeText(txt, x, y);
      ctx.fillStyle = '#ffffff';
      ctx.fillText(txt, x, y);
    });
    ctx.restore();
  },
};

function chartConfig(spec) {
  const type = spec.type;
  const payload = spec.payload || {};
  const labels = payload.labels || [];
  const values = payload.values || [];
  // Multi-serie (p. ej. ventas por ubicación y mes): payload.datasets = [{label, data}].
  // Solo en ejes cartesianos; pie/doughnut/radar siguen con una serie.
  const multi = (type === 'bar' || type === 'line')
    && Array.isArray(payload.datasets) && payload.datasets.length > 0;

  // Paleta armónica con la identidad de la app, adaptada al tema (claro/oscuro).
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const palette = isDark ? PALETTE_DARK : PALETTE_LIGHT;
  const accent = isDark ? '#93c5fd' : '#0b2d42';
  const accentFill = isDark ? 'rgba(147, 197, 253, 0.12)' : 'rgba(11, 45, 66, 0.08)';

  let backgroundColor;
  let borderColor;
  if (type === 'pie' || type === 'doughnut') {
    backgroundColor = labels.map((_, i) => palette[i % palette.length]);
    borderColor = '#ffffff';
  } else if (type === 'bar') {
    backgroundColor = labels.map((_, i) => palette[i % palette.length]);
    borderColor = backgroundColor;
  } else { // line / radar
    backgroundColor = accentFill;
    borderColor = accent;
  }

  const opts = { responsive: true, maintainAspectRatio: false };
  if (type === 'bar' || type === 'line') {
    opts.scales = {
      y: { beginAtZero: true, grid: { color: 'rgba(15, 23, 42, 0.06)' } },
      x: { grid: { display: false } },
    };
    opts.plugins = { legend: { display: multi } };
  } else if (type === 'pie' || type === 'doughnut') {
    opts.plugins = { legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } } };
  }

  const datasets = multi
    ? payload.datasets.map((ds, i) => {
      const color = palette[i % palette.length];
      return {
        label: ds.label, data: ds.data || [], backgroundColor: color,
        borderColor: color, borderWidth: 1, tension: 0.3, fill: false,
      };
    })
    : [{
      label: spec.title,
      data: values,
      backgroundColor,
      borderColor,
      borderWidth: 1,
      tension: 0.3,
    }];

  return {
    type,
    data: {
      labels,
      datasets,
    },
    options: opts,
    plugins: [percentPlugin],
  };
}

function renderWidgetCharts(root) {
  if (!window.Chart || !root || typeof root.querySelectorAll !== 'function') return;
  const canvases = Array.from(root.querySelectorAll('canvas[data-js-widget-chart]'));
  if (root.matches && root.matches('canvas[data-js-widget-chart]')) canvases.push(root);
  canvases.forEach((el) => {
    let spec;
    try {
      spec = JSON.parse(el.getAttribute('data-js-widget-chart') || '{}');
    } catch (e) {
      return;
    }
    // Destruir la instancia previa del canvas al re-renderizar.
    const prev = window.Chart.getChart(el);
    if (prev) prev.destroy();
    new window.Chart(el, chartConfig(spec));
  });
}

export function init() {
  onContent(renderWidgetCharts);
}
