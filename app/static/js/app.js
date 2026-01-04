(function () {
  function setActiveTab(tabEl) {
    document.querySelectorAll('.tab').forEach((t) => t.setAttribute('aria-selected', 'false'));
    if (tabEl) tabEl.setAttribute('aria-selected', 'true');
  }

  function onTabClick(evt) {
    const el = evt.target.closest('.tab');
    if (!el) return;
    setActiveTab(el);
  }

  function clearTarget(selector) {
    if (!selector) return;
    const el = document.querySelector(selector);
    if (el) el.innerHTML = '';
  }

  function onClearClick(evt) {
    const btn = evt.target.closest('[data-clear-target]');
    if (!btn) return;
    evt.preventDefault();
    clearTarget(btn.getAttribute('data-clear-target'));
  }

  function renderBarcodes(root) {
    if (!window.JsBarcode) return;
    const container = root || document;
    const svgs = container.querySelectorAll('svg.barcode[data-code]:not([data-rendered="1"])');
    svgs.forEach((svg) => {
      const code = svg.getAttribute('data-code') || '';
      if (!code) return;
      try {
        JsBarcode(svg, code, { format: 'CODE128', displayValue: false, height: 28, margin: 0, width: 1 });
        svg.setAttribute('data-rendered', '1');
      } catch (e) {}
    });
  }

  function renderMonthlyChart(root) {
    const container = root || document;
    const el = container.querySelector('#monthly-chart');
    if (!el) return;

    const debugEl = container.querySelector('#monthly-chart-debug') || document.querySelector('#monthly-chart-debug');
    const dataEl = container.querySelector('#monthly-chart-data') || document.querySelector('#monthly-chart-data');
    const setDebug = (msg) => {
      if (debugEl) debugEl.textContent = msg || '';
    };

    const attempt = (n) => {
      if (!window.Chart) {
        setDebug('Cargando gráfica...');
        if (n > 0) setTimeout(() => attempt(n - 1), 200);
        if (n === 0) setDebug('No se pudo cargar Chart.js (revisa conexión/CDN).');
        return;
      }

      let payload = null;
      try {
        payload = JSON.parse((dataEl && dataEl.textContent) ? dataEl.textContent : '{}');
      } catch (e) {
        setDebug('Error leyendo datos de gráfica.');
        return;
      }

      const labels = Array.isArray(payload.labels) ? payload.labels : [];
      const sales = Array.isArray(payload.sales) ? payload.sales : [];
      const purchases = Array.isArray(payload.purchases) ? payload.purchases : [];
      const profit = Array.isArray(payload.profit) ? payload.profit : [];

      if (window.__monthlyChart) {
        try { window.__monthlyChart.destroy(); } catch (e) {}
      }

      window.__monthlyChart = new Chart(el.getContext('2d'), {
        type: 'bar',
        data: {
          labels,
          datasets: [
            { label: 'Ventas', data: sales, backgroundColor: 'rgba(11,45,66,0.85)' },
            { label: 'Compras', data: purchases, backgroundColor: 'rgba(203, 58, 66, 0.75)' },
            { label: 'Utilidad', data: profit, backgroundColor: 'rgba(16, 185, 129, 0.75)' },
          ],
        },
        options: {
          responsive: true,
          plugins: { legend: { position: 'bottom' } },
          scales: { x: { stacked: false }, y: { beginAtZero: true } },
        },
      });

      setDebug('');
    };

    attempt(15);
  }

  function renderMonthlySalesDailyLineChart(root) {
    const container = root || document;
    const el = container.querySelector('#monthly-sales-daily-line');
    if (!el) return;

    const debugEl = container.querySelector('#monthly-sales-daily-line-debug') || document.querySelector('#monthly-sales-daily-line-debug');
    const dataEl = container.querySelector('#monthly-sales-daily-line-data') || document.querySelector('#monthly-sales-daily-line-data');
    const setDebug = (msg) => {
      if (debugEl) debugEl.textContent = msg || '';
    };

    const attempt = (n) => {
      if (!window.Chart) {
        setDebug('Cargando gráfica...');
        if (n > 0) setTimeout(() => attempt(n - 1), 200);
        if (n === 0) setDebug('No se pudo cargar Chart.js (revisa conexión/CDN).');
        return;
      }

      let payload = null;
      try {
        payload = JSON.parse((dataEl && dataEl.textContent) ? dataEl.textContent : '{}');
      } catch (e) {
        setDebug('Error leyendo datos de gráfica.');
        return;
      }

      const labels = Array.isArray(payload.labels) ? payload.labels : [];
      const sales = Array.isArray(payload.sales) ? payload.sales : [];

      if (window.__monthlySalesDailyLine) {
        try { window.__monthlySalesDailyLine.destroy(); } catch (e) {}
      }

      window.__monthlySalesDailyLine = new Chart(el.getContext('2d'), {
        type: 'line',
        data: {
          labels,
          datasets: [
            {
              label: 'Ventas por día (€)',
              data: sales,
              borderColor: 'rgba(11,45,66,0.85)',
              backgroundColor: 'rgba(11,45,66,0.10)',
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

      setDebug('');
    };

    attempt(15);
  }

  function renderMonthlySalesPieChart(root) {
    const container = root || document;
    const el = container.querySelector('#monthly-sales-pie');
    if (!el) return;

    const wrap = container.querySelector('#monthly-sales-pie-wrap') || document.querySelector('#monthly-sales-pie-wrap');
    const tooltipBox = container.querySelector('#monthly-sales-pie-tooltip') || document.querySelector('#monthly-sales-pie-tooltip');
    const lineSvg = container.querySelector('#monthly-sales-pie-line') || document.querySelector('#monthly-sales-pie-line');
    const lineEl = container.querySelector('#monthly-sales-pie-line-el') || document.querySelector('#monthly-sales-pie-line-el');

    const debugEl = container.querySelector('#monthly-sales-pie-debug') || document.querySelector('#monthly-sales-pie-debug');
    const dataEl = container.querySelector('#monthly-sales-pie-data') || document.querySelector('#monthly-sales-pie-data');
    const setDebug = (msg) => {
      if (debugEl) debugEl.textContent = msg || '';
    };

    const attempt = (n) => {
      if (!window.Chart) {
        setDebug('Cargando gráfica...');
        if (n > 0) setTimeout(() => attempt(n - 1), 200);
        if (n === 0) setDebug('No se pudo cargar Chart.js (revisa conexión/CDN).');
        return;
      }

      let payload = null;
      try {
        payload = JSON.parse((dataEl && dataEl.textContent) ? dataEl.textContent : '{}');
      } catch (e) {
        setDebug('Error leyendo datos de gráfica.');
        return;
      }

      const labels = Array.isArray(payload.labels) ? payload.labels : [];
      const values = Array.isArray(payload.values) ? payload.values : [];
      const qtys = Array.isArray(payload.qtys) ? payload.qtys : [];
      const total = values.reduce((a, b) => a + (Number(b) || 0), 0);

      if (window.__monthlySalesPie) {
        try { window.__monthlySalesPie.destroy(); } catch (e) {}
      }

      window.__monthlySalesPie = new Chart(el.getContext('2d'), {
        type: 'pie',
        data: {
          labels,
          datasets: [
            {
              data: values,
              backgroundColor: [
                'rgba(11,45,66,0.85)',
                'rgba(203, 58, 66, 0.75)',
                'rgba(16, 185, 129, 0.75)',
                'rgba(59, 130, 246, 0.75)',
                'rgba(245, 158, 11, 0.75)',
                'rgba(99, 102, 241, 0.75)',
                'rgba(236, 72, 153, 0.75)',
                'rgba(34, 197, 94, 0.65)',
                'rgba(148, 163, 184, 0.75)',
                'rgba(15, 118, 110, 0.70)',
                'rgba(107, 114, 128, 0.55)',
              ],
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              enabled: false,
              external: function (context) {
                const chart = context.chart;
                const tt = context.tooltip;

                if (!wrap || !tooltipBox || !lineSvg || !lineEl) return;

                if (!tt || tt.opacity === 0) {
                  lineSvg.style.display = 'none';
                  return;
                }

                const idx = (tt.dataPoints && tt.dataPoints.length) ? tt.dataPoints[0].dataIndex : 0;
                const label = labels[idx] || '';
                const qty = Number(qtys[idx] || 0);
                const v = Number(values[idx] || 0);
                const pct = total ? (v / total * 100) : 0;

                tooltipBox.innerHTML = `
                  <div style="font-weight:700; margin-bottom:6px;">${label}</div>
                  <div class="muted" style="margin-bottom:4px;">Cantidad vendida: ${qty.toFixed(2)}</div>
                  <div class="muted" style="margin-bottom:4px;">Venta del artículo: ${v.toFixed(2)} €</div>
                  <div class="muted" style="margin-bottom:4px;">Venta total mes: ${total.toFixed(2)} €</div>
                  <div class="muted">Porcentaje: ${pct.toFixed(1)}%</div>
                `;

                const wrapRect = wrap.getBoundingClientRect();
                const canvasRect = el.getBoundingClientRect();
                const boxRect = tooltipBox.getBoundingClientRect();

                const x1 = (canvasRect.left - wrapRect.left) + (tt.caretX || 0);
                const y1 = (canvasRect.top - wrapRect.top) + (tt.caretY || 0);
                const x2 = (boxRect.left - wrapRect.left);
                const y2 = (boxRect.top - wrapRect.top) + (boxRect.height / 2);

                lineEl.setAttribute('x1', String(x1));
                lineEl.setAttribute('y1', String(y1));
                lineEl.setAttribute('x2', String(x2));
                lineEl.setAttribute('y2', String(y2));
                lineSvg.style.display = 'block';

                chart.draw();
              },
            },
          },
        },
      });

      setDebug('');
    };

    attempt(15);
  }

  function runEnhancers(root) {
    renderBarcodes(root);
    renderMonthlyChart(root);
    renderMonthlySalesPieChart(root);
    renderMonthlySalesDailyLineChart(root);
  }

  document.addEventListener('click', onTabClick);
  document.addEventListener('click', onClearClick);

  document.addEventListener('htmx:afterSwap', (evt) => {
    runEnhancers(evt.target);
  });

  document.addEventListener('DOMContentLoaded', () => {
    runEnhancers(document);
  });
})();
