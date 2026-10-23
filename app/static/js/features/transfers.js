// Traspasos: líneas dinámicas del formulario, stock disponible al elegir producto y el modal
// de traspaso masivo (filtro, selección, recarga de la pestaña tras el éxito).
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
    .then((res) => {
      if (!res.ok || res.redirected) throw new Error(`${res.status}`);
      return res.text();
    })
    .then((stock) => {
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

function toggleBulkRow(cb, checked) {
  const row = cb.closest('tr');
  if (!row) return;
  const qty = row.querySelector('input[name="quantity"]');
  const prod = row.querySelector('input[name="product"]');
  if (qty) qty.disabled = !checked;
  if (prod) prod.disabled = !checked;
}

function onTransferBulkCheckboxChange(evt) {
  const cb = evt.target.closest('.transfer-bulk-check');
  if (!cb) return;
  toggleBulkRow(cb, cb.checked);
}

function onTransferBulkSelectAll(evt) {
  const master = evt.target.closest('#transfer-bulk-select-all');
  if (!master) return;
  const modal = master.closest('.modal-overlay');
  if (!modal) return;
  modal.querySelectorAll('.transfer-bulk-check').forEach((cb) => {
    const row = cb.closest('tr');
    if (row && row.style.display === 'none') return;
    cb.checked = master.checked;
    toggleBulkRow(cb, master.checked);
  });
}

function onTransferBulkSearch(evt) {
  const input = evt.target.closest('#transfer-bulk-search');
  if (!input) return;
  const q = (input.value || '').toLowerCase();
  const modal = input.closest('.modal-overlay');
  if (!modal) return;
  modal.querySelectorAll('.transfer-bulk-row').forEach((row) => {
    const hay = (row.getAttribute('data-search') || '').indexOf(q) !== -1;
    row.style.display = hay ? '' : 'none';
  });
}

export function init() {
  document.addEventListener('click', onTransferAddLine);
  document.addEventListener('click', onTransferRemoveLine);
  document.addEventListener('click', onTransferToggleEdit);
  document.addEventListener('change', onTransferProductChange);
  document.addEventListener('change', onTransferBulkCheckboxChange);
  document.addEventListener('change', onTransferBulkSelectAll);
  document.addEventListener('input', onTransferBulkSearch);
  document.addEventListener('transferBulkSuccess', () => {
    try {
      window.htmx.ajax('GET', '/ui/tabs/transfers', { target: '#tab-content', swap: 'innerHTML' });
    } catch (e) {}
  });
}
