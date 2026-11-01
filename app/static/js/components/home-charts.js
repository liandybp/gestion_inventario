// Gráficos de Inicio (partials/home_charts.html): barras mensuales, línea de ventas diarias y
// pastel de ventas por artículo. Los datos llegan en <script type="application/json">.
// Chart.js es un script clásico del <head>: ya está cargado cuando se ejecutan los módulos.
import { onContent } from '../core/content.js';
import { cssList, cssVar, THEME_EVENT } from './chart-palette.js';

const charts = { monthly: null, dailyLine: null, pie: null };

function find(container, selector) {
  return container.querySelector(selector) || document.querySelector(selector);
}

function readPayload(container, selector) {
  const dataEl = find(container, selector);
  try {
    return JSON.parse((dataEl && dataEl.textContent) ? dataEl.textContent : '{}');
  } catch (e) {
    return null;
  }
}

function list(payload, key) {
  return Array.isArray(payload[key]) ? payload[key] : [];
}

function replaceChart(key, canvas, config) {
  if (charts[key]) {
    try { charts[key].destroy(); } catch (e) {}
  }
  charts[key] = new window.Chart(canvas.getContext('2d'), config);
}

function renderMonthlyChart(container) {
  const el = container.querySelector('#monthly-chart');
  if (!el || !window.Chart) return;
  const payload = readPayload(container, '#monthly-chart-data');
  if (!payload) return;
  replaceChart('monthly', el, {
    type: 'bar',
    data: {
      labels: list(payload, 'labels'),
      datasets: [
        { label: 'Ventas', data: list(payload, 'sales'), backgroundColor: cssVar('--home-chart-sales') },
        { label: 'Compras', data: list(payload, 'purchases'), backgroundColor: cssVar('--home-chart-purchases') },
        { label: 'Utilidad', data: list(payload, 'profit'), backgroundColor: cssVar('--home-chart-profit') },
      ],
    },
    options: {
      responsive: true,
      plugins: { legend: { position: 'bottom' } },
      scales: { x: { stacked: false }, y: { beginAtZero: true } },
    },
  });
}

function renderDailyLineChart(container) {
  const el = container.querySelector('#monthly-sales-daily-line');
  if (!el || !window.Chart) return;
  const payload = readPayload(container, '#monthly-sales-daily-line-data');
  if (!payload) return;
  replaceChart('dailyLine', el, {
    type: 'line',
    data: {
      labels: list(payload, 'labels'),
      datasets: [
        {
          label: 'Ventas por día (€)',
          data: list(payload, 'sales'),
          borderColor: cssVar('--home-chart-sales'),
          backgroundColor: cssVar('--home-chart-line-fill'),
          fill: true,
          tension: 0.25,
          pointRadius: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
        y: { beginAtZero: true },
      },
    },
  });
}

// Tooltip del pastel construido con textContent: la etiqueta es "SKU - nombre de producto"
// (dato de usuario). Antes se metía en innerHTML sin escapar (XSS al pasar el ratón).
function fillPieTooltip(box, label, qty, value, total, pct) {
  const lines = [
    [label, true],
    [`Cantidad vendida: ${qty.toFixed(2)}`, false],
    [`Venta del artículo: ${value.toFixed(2)} €`, false],
    [`Venta total mes: ${total.toFixed(2)} €`, false],
    [`Porcentaje: ${pct.toFixed(1)}%`, false],
  ];
  box.replaceChildren(...lines.map(([text, title], i) => {
    const div = document.createElement('div');
    div.textContent = text;
    if (title) {
      div.style.fontWeight = '700';
      div.style.marginBottom = '6px';
    } else {
      div.className = 'muted';
      if (i < lines.length - 1) div.style.marginBottom = '4px';
    }
    return div;
  }));
}

function renderPieChart(container) {
  const el = container.querySelector('#monthly-sales-pie');
  if (!el || !window.Chart) return;
  const payload = readPayload(container, '#monthly-sales-pie-data');
  if (!payload) return;

  const wrap = find(container, '#monthly-sales-pie-wrap');
  const tooltipBox = find(container, '#monthly-sales-pie-tooltip');
  const lineSvg = find(container, '#monthly-sales-pie-line');
  const lineEl = find(container, '#monthly-sales-pie-line-el');

  const labels = list(payload, 'labels');
  const values = list(payload, 'values');
  const qtys = list(payload, 'qtys');
  const total = values.reduce((a, b) => a + (Number(b) || 0), 0);

  const external = (context) => {
    const chart = context.chart;
    const tt = context.tooltip;
    if (!wrap || !tooltipBox || !lineSvg || !lineEl) return;
    if (!tt || tt.opacity === 0) {
      lineSvg.style.display = 'none';
      return;
    }
    const idx = (tt.dataPoints && tt.dataPoints.length) ? tt.dataPoints[0].dataIndex : 0;
    const v = Number(values[idx] || 0);
    fillPieTooltip(tooltipBox, String(labels[idx] || ''), Number(qtys[idx] || 0), v, total,
      total ? (v / total * 100) : 0);

    const wrapRect = wrap.getBoundingClientRect();
    const canvasRect = el.getBoundingClientRect();
    const boxRect = tooltipBox.getBoundingClientRect();
    lineEl.setAttribute('x1', String((canvasRect.left - wrapRect.left) + (tt.caretX || 0)));
    lineEl.setAttribute('y1', String((canvasRect.top - wrapRect.top) + (tt.caretY || 0)));
    lineEl.setAttribute('x2', String(boxRect.left - wrapRect.left));
    lineEl.setAttribute('y2', String((boxRect.top - wrapRect.top) + (boxRect.height / 2)));
    lineSvg.style.display = 'block';
    chart.draw();
  };

  replaceChart('pie', el, {
    type: 'pie',
    data: { labels, datasets: [{ data: values, backgroundColor: cssList('home-pie') }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false, external } },
    },
  });
}

function enhance(root, fromSwap) {
  const container = root || document;
  renderMonthlyChart(container);
  renderPieChart(container);
  renderDailyLineChart(container);
  if (!fromSwap) return;
  [charts.dailyLine, charts.pie, charts.monthly].forEach((chart) => {
    try { if (chart && chart.resize) chart.resize(); } catch (e) {}
  });
}

export function init() {
  onContent(enhance);
  // Cambio de tema sin recarga: redibujar con las variables del tema nuevo.
  document.addEventListener(THEME_EVENT, () => enhance(document, false));
}
