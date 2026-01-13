(function () {
  const ACTIVE_TAB_KEY = 'active_tab_v1';

  function saveActiveTab(tabEl) {
    if (!tabEl) return;
    const tabId = tabEl.id || '';
    const url = tabEl.getAttribute('hx-get') || '';
    if (!url) return;
    try {
      localStorage.setItem(ACTIVE_TAB_KEY, JSON.stringify({ id: tabId, url }));
    } catch (e) {}
  }

  function restoreActiveTab() {
    let saved = null;
    try {
      saved = JSON.parse(localStorage.getItem(ACTIVE_TAB_KEY) || 'null');
    } catch (e) {
      saved = null;
    }
    if (!saved || !saved.url) return;

    const tabContent = document.getElementById('tab-content');
    if (!tabContent) return;

    const savedBtn = saved.id ? document.getElementById(saved.id) : null;
    const btn = savedBtn && savedBtn.classList.contains('sidebar-item') ? savedBtn : null;
    const url = (btn && btn.getAttribute('hx-get')) || saved.url;
    if (!url) return;

    tabContent.setAttribute('hx-get', url);
    if (btn) setActiveTab(btn);
  }

  function setActiveTab(tabEl) {
    document.querySelectorAll('.sidebar-item').forEach((t) => t.setAttribute('aria-selected', 'false'));
    if (tabEl) tabEl.setAttribute('aria-selected', 'true');
  }

  function onTabClick(evt) {
    const el = evt.target.closest('.sidebar-item');
    if (!el) return;
    setActiveTab(el);
    saveActiveTab(el);
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

  function saveReusableNote(note) {
    if (!note || !note.trim()) return;
    const trimmed = note.trim();
    try {
      let notes = JSON.parse(localStorage.getItem('reusable_notes') || '[]');
      if (!notes.includes(trimmed)) {
        notes.unshift(trimmed);
        notes = notes.slice(0, 20);
        localStorage.setItem('reusable_notes', JSON.stringify(notes));
      }
    } catch (e) {}
  }

  function getReusableNotes() {
    try {
      return JSON.parse(localStorage.getItem('reusable_notes') || '[]');
    } catch (e) {
      return [];
    }
  }

  function enhanceNoteInputs(root) {
    const noteInputs = (root || document).querySelectorAll('input[name="note"]');
    noteInputs.forEach((input) => {
      if (input.hasAttribute('data-note-enhanced')) return;
      input.setAttribute('data-note-enhanced', 'true');
      
      let datalist = input.list;
      if (!datalist) {
        const listId = 'reusable-notes-list-' + Math.random().toString(36).substr(2, 9);
        datalist = document.createElement('datalist');
        datalist.id = listId;
        input.setAttribute('list', listId);
        input.parentElement.appendChild(datalist);
      }
      
      const notes = getReusableNotes();
      datalist.innerHTML = '';
      notes.forEach((note) => {
        const option = document.createElement('option');
        option.value = note;
        datalist.appendChild(option);
      });
      
      const form = input.closest('form');
      if (form && !form.hasAttribute('data-note-save-listener')) {
        form.setAttribute('data-note-save-listener', 'true');
        form.addEventListener('submit', () => {
          const noteVal = input.value;
          if (noteVal) saveReusableNote(noteVal);
        });
      }
    });
  }

  function debugReturnLots(root) {
    let returnSkuInput = null;
    if (root && typeof root.querySelector === 'function') {
      returnSkuInput = root.querySelector('#return-sku-input');
    }
    if (!returnSkuInput) {
      returnSkuInput = document.getElementById('return-sku-input');
    }
    if (returnSkuInput && !returnSkuInput.hasAttribute('data-debug-listener')) {
      returnSkuInput.setAttribute('data-debug-listener', 'true');
      returnSkuInput.addEventListener('change', () => {
        // debug hook (no-op)
      });
    }
  }

  function onTransferProductChange(evt) {
    const input = evt.target;
    if (!input || input.name !== 'product') return;
    
    const row = input.closest('tr');
    if (!row) return;
    
    const qtyInput = row.querySelector('input[name="quantity"]');
    if (!qtyInput) return;
    
    let sku = (input.value || '').trim();
    if (sku.includes(' - ')) {
      sku = sku.split(' - ')[0].trim();
    }
    if (!sku) return;

    const form = input.closest('form');
    let fromCode = '';
    if (form) {
      const fromSel = form.querySelector('select[name="from_location_code"]');
      if (fromSel) {
        fromCode = (fromSel.value || '').trim();
      }
    }

    let url = `/ui/transfers/stock/${encodeURIComponent(sku)}`;
    if (fromCode) {
      url = url + `?location_code=${encodeURIComponent(fromCode)}`;
    }

    fetch(url)
      .then(res => res.text())
      .then(stock => {
        qtyInput.value = stock || '0';
      })
      .catch(() => {
        qtyInput.value = '0';
      });
  }

  function onTransferAddLine(evt) {
    const btn = evt.target.closest('[data-transfer-add-line]');
    if (!btn) return;
    evt.preventDefault();

    const container = btn.closest('form') || document;
    const tbody = container.querySelector('#transfer-lines');
    const tmpl = container.querySelector('#transfer-line-template');
    if (!tbody || !tmpl) return;
    const row = tmpl.content.firstElementChild;
    if (!row) return;
    const clone = row.cloneNode(true);
    tbody.appendChild(clone);
    const firstInput = clone.querySelector('input[name="product"]');
    if (firstInput) {
      try { firstInput.focus(); } catch (e) {}
    }
  }

  function onTransferRemoveLine(evt) {
    const btn = evt.target.closest('[data-transfer-remove-line]');
    if (!btn) return;
    evt.preventDefault();

    const row = btn.closest('tr');
    if (!row) return;
    const tbody = row.parentElement;
    if (!tbody) return;

    const rows = tbody.querySelectorAll('tr');
    if (rows.length <= 1) {
      const prod = row.querySelector('input[name="product"]');
      const qty = row.querySelector('input[name="quantity"]');
      if (prod) prod.value = '';
      if (qty) qty.value = '';
      return;
    }
    row.remove();
  }

  function onTransferToggleEdit(evt) {
    const btn = evt.target.closest('[data-transfer-toggle-edit]');
    if (!btn) return;
    evt.preventDefault();

    const row = btn.closest('tr');
    if (!row) return;

    const fields = row.querySelectorAll('input[name="product"], input[name="quantity"]');
    const anyEnabled = Array.from(fields).some((el) => !el.disabled);
    if (anyEnabled) {
      fields.forEach((el) => { el.disabled = true; });
      btn.textContent = 'Editar';
    } else {
      fields.forEach((el) => { el.disabled = false; });
      btn.textContent = 'Bloquear';
      const first = row.querySelector('input[name="product"], input[name="quantity"]');
      if (first) {
        try { first.focus(); } catch (e) {}
      }
    }
  }

  function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) {
      modal.remove();
    }
  }

  function onModalClose(evt) {
    const closeBtn = evt.target.closest('[data-modal-close]');
    if (!closeBtn) return;
    evt.preventDefault();
    const modalId = closeBtn.getAttribute('data-modal-close');
    closeModal(modalId);
  }

  function onModalOverlayClick(evt) {
    if (evt.target.classList.contains('modal-overlay')) {
      const modalId = evt.target.closest('[id]')?.id;
      if (modalId) closeModal(modalId);
    }
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
    enhanceNoteInputs(root);
    debugReturnLots(root);
  }

  restoreActiveTab();

  document.addEventListener('click', onTabClick);
  document.addEventListener('click', onClearClick);
  document.addEventListener('click', onModalClose);
  document.addEventListener('click', onModalOverlayClick);
  document.addEventListener('click', onTransferAddLine);
  document.addEventListener('click', onTransferRemoveLine);
  document.addEventListener('click', onTransferToggleEdit);
  document.addEventListener('change', onTransferProductChange);

  document.addEventListener('htmx:afterSwap', (evt) => {
    const target = evt.target;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        runEnhancers(target);
        try { if (window.__monthlySalesDailyLine && window.__monthlySalesDailyLine.resize) window.__monthlySalesDailyLine.resize(); } catch (e) {}
        try { if (window.__monthlySalesPie && window.__monthlySalesPie.resize) window.__monthlySalesPie.resize(); } catch (e) {}
        try { if (window.__monthlyChart && window.__monthlyChart.resize) window.__monthlyChart.resize(); } catch (e) {}
      });
    });
  });

  document.addEventListener('htmx:afterRequest', (evt) => {
    const elt = evt.detail && evt.detail.elt;
    const xhr = evt.detail && evt.detail.xhr;
    if (!elt || !xhr) return;

    const modal = elt.closest('.modal-overlay');
    if (!modal) return;

    const keepOpen = String(xhr.getResponseHeader('X-Modal-Keep') || '') === '1';
    if (keepOpen) return;

    if (xhr.status >= 200 && xhr.status < 300) {
      try {
        modal.remove();
      } catch (e) {}
    }
  });

  document.addEventListener('DOMContentLoaded', () => {
    runEnhancers(document);
    
    // Mobile menu toggle
    const menuToggle = document.getElementById('mobile-menu-toggle');
    const menuOverlay = document.getElementById('mobile-menu-overlay');
    const sidebar = document.getElementById('sidebar');
    
    if (menuToggle && menuOverlay && sidebar) {
      const toggleMenu = () => {
        sidebar.classList.toggle('active');
        menuOverlay.classList.toggle('active');
      };
      
      menuToggle.addEventListener('click', toggleMenu);
      menuOverlay.addEventListener('click', toggleMenu);
      
      // Close menu when clicking a sidebar item
      sidebar.addEventListener('click', (e) => {
        if (e.target.closest('.sidebar-item')) {
          sidebar.classList.remove('active');
          menuOverlay.classList.remove('active');
        }
      });
    }
  });
})();
