// Modales: cierre por botón [data-modal-close], por clic en el overlay (salvo
// data-modal-static="1") y automático tras una petición correcta desde dentro del modal.
// También [data-clear-target]: vacía el elemento indicado.
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
  closeModal(closeBtn.getAttribute('data-modal-close'));
}

function onModalOverlayClick(evt) {
  if (evt.target.classList.contains('modal-overlay')) {
    if (evt.target.getAttribute('data-modal-static') === '1') return;
    const modalId = evt.target.closest('[id]')?.id;
    if (modalId) closeModal(modalId);
  }
}

function onClearClick(evt) {
  const btn = evt.target.closest('[data-clear-target]');
  if (!btn) return;
  evt.preventDefault();
  const selector = btn.getAttribute('data-clear-target');
  if (!selector) return;
  const el = document.querySelector(selector);
  if (el) el.innerHTML = '';
}

function onAfterRequest(evt) {
  const elt = evt.detail && evt.detail.elt;
  const xhr = evt.detail && evt.detail.xhr;
  if (!elt || !xhr) return;

  const modal = elt.closest('.modal-overlay');
  if (!modal) return;

  const keepOpen = String(xhr.getResponseHeader('X-Modal-Keep') || '') === '1';
  if (keepOpen) return;

  // Error 4xx en HTML ya swapeado (ver core/swap.js) en un panel FUERA del modal
  // (p. ej. product_edit_form → #product-panel): cerrar el modal para que se vea.
  const target = evt.detail && evt.detail.target;
  const isHtml = String(xhr.getResponseHeader('Content-Type') || '').includes('text/html');
  const errorShownBehind = xhr.status >= 400 && xhr.status < 500 && isHtml && target && !modal.contains(target);

  if ((xhr.status >= 200 && xhr.status < 300) || errorShownBehind) {
    try {
      modal.remove();
    } catch (e) {}
  }
}

export function init() {
  document.addEventListener('click', onClearClick);
  document.addEventListener('click', onModalClose);
  document.addEventListener('click', onModalOverlayClick);
  document.addEventListener('htmx:afterRequest', onAfterRequest);
}
