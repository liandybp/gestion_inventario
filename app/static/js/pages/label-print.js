// Etiqueta individual (label_single_print.html): N copias de nombre + código de barras
// CODE128 del SKU. SKU y nombre llegan en data-js-label-sku / data-js-label-name de #sheet.
const sheet = document.getElementById('sheet');
const copiesInput = document.getElementById('copies');
const sku = (sheet && sheet.dataset.jsLabelSku) || '';
const name = (sheet && sheet.dataset.jsLabelName) || '';

function makeLabelEl() {
  const page = document.createElement('div');
  page.className = 'page';
  const wrap = document.createElement('div');
  wrap.className = 'label';
  const nameEl = document.createElement('div');
  nameEl.className = 'name';
  nameEl.textContent = name;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('class', 'barcode');
  svg.setAttribute('data-code', sku);
  wrap.appendChild(nameEl);
  wrap.appendChild(svg);
  page.appendChild(wrap);
  return page;
}

function render() {
  const n = Math.max(1, parseInt(copiesInput.value || '1', 10) || 1);
  sheet.innerHTML = '';
  for (let i = 0; i < n; i++) {
    sheet.appendChild(makeLabelEl());
  }
  if (window.JsBarcode) {
    sheet.querySelectorAll('svg.barcode[data-code]').forEach((svg) => {
      try {
        window.JsBarcode(svg, sku, {
          format: 'CODE128',
          displayValue: true,
          fontSize: 12,
          textMargin: 0,
          height: 40,
          margin: 0,
          width: 1.2,
        });
      } catch (e) {}
    });
  }
}

if (sheet && copiesInput) {
  copiesInput.addEventListener('change', render);
  render();

  const params = new URLSearchParams(window.location.search || '');
  if ((params.get('auto_print') || '') === '1') {
    setTimeout(() => {
      try { window.print(); } catch (e) {}
    }, 200);
  }

  document.getElementById('btn-print').addEventListener('click', () => {
    render();
    setTimeout(() => {
      try { window.print(); } catch (e) {}
    }, 100);
  });
}
