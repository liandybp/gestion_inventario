// Sugerencias dinámicas de productos: los datalist iniciales solo traen los 200 primeros
// productos; al teclear en un input con data-product-suggest se piden coincidencias al
// servidor. No usa hx-get porque varios de estos inputs ya lo usan para cargar los valores
// por defecto del artículo. Ventas y Traspasos NO llevan data-product-suggest: sus datalist
// filtran por stock en la ubicación.
const suggestTimers = new WeakMap();
const suggestInitial = new WeakMap(); // datalist -> contenido inicial (lo comparten varias líneas)

function onInput(evt) {
  const input = evt.target;
  if (!(input instanceof HTMLInputElement) || !input.dataset.productSuggest || !input.list) return;
  const list = input.list;
  if (!suggestInitial.has(list)) suggestInitial.set(list, list.innerHTML);
  clearTimeout(suggestTimers.get(input));
  suggestTimers.set(input, setTimeout(async () => {
    const q = input.value.trim();
    if (q.length < 2) {
      // Campo vaciado: vuelven las sugerencias iniciales.
      if (list.innerHTML !== suggestInitial.get(list)) list.innerHTML = suggestInitial.get(list);
      return;
    }
    // El valor ya es una opción (el usuario eligió una sugerencia o escaneó un código): no consultar.
    if (Array.from(list.options).some((o) => o.value === input.value)) return;
    const url = '/ui/product-suggest?q=' + encodeURIComponent(q) + '&fmt=' + encodeURIComponent(input.dataset.productSuggest);
    try {
      const resp = await fetch(url, { credentials: 'same-origin' });
      // Sesión caducada (redirige al login), error, o el usuario siguió tecleando: descartar.
      if (!resp.ok || resp.redirected || input.value.trim() !== q) return;
      const html = await resp.text();
      // Sin coincidencias: se deja la lista como está (el navegador ya filtra lo que no coincide).
      if (html.trim()) list.innerHTML = html;
    } catch (e) {}
  }, 250));
}

export function init() {
  document.addEventListener('input', onInput);
}
