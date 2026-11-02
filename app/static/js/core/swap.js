// Comportamiento de los swaps de htmx: mostrar los errores 4xx en HTML y conservar el
// scroll al recargar los paneles de compra y venta.
let pendingScrollRestore = null;

function shouldPreserveScrollForElt(elt) {
  if (!elt || typeof elt.closest !== 'function') return false;
  return Boolean(elt.closest('#purchase-panel') || elt.closest('#sale-panel'));
}

export function init() {
  document.addEventListener('htmx:beforeRequest', (evt) => {
    const elt = evt.detail && evt.detail.elt;
    if (!shouldPreserveScrollForElt(elt)) return;
    pendingScrollRestore = window.scrollY;
  });

  // Los routers UI devuelven su panel con el mensaje de error y el status de la
  // HTTPException (400/404/409/422...). htmx no swapea respuestas >= 400 por
  // defecto, así que sin esto el error no se veía. Solo HTML: las 422 de
  // validación de FastAPI son JSON y el 403 de CSRF es texto plano. El chat del
  // copiloto ya inserta sus errores a mano.
  document.addEventListener('htmx:beforeSwap', (evt) => {
    const xhr = evt.detail && evt.detail.xhr;
    if (!xhr || xhr.status < 400 || xhr.status >= 500) return;
    const path = String((evt.detail.requestConfig && evt.detail.requestConfig.path) || '');
    if (path.startsWith('/ui/agent/chat')) return;
    const ct = String(xhr.getResponseHeader('Content-Type') || '');
    if (!ct.includes('text/html')) return;
    evt.detail.shouldSwap = true;
    evt.detail.isError = false;
  });

  document.addEventListener('htmx:afterSwap', (evt) => {
    const target = evt.target;
    if (pendingScrollRestore !== null && target && (target.id === 'purchase-panel' || target.id === 'sale-panel')) {
      const y = pendingScrollRestore;
      pendingScrollRestore = null;
      requestAnimationFrame(() => {
        try { window.scrollTo({ top: y, left: 0, behavior: 'auto' }); } catch (e) { window.scrollTo(0, y); }
      });
    }
  });
}
