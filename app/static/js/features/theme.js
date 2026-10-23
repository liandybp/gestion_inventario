// Botón de tema claro/oscuro ([data-js-theme-toggle]). Recarga la página para que los
// gráficos se redibujen con la paleta del tema (la paleta CSS llega en la fase 3).
function onClick(evt) {
  if (!evt.target.closest('[data-js-theme-toggle]')) return;
  const root = document.documentElement;
  const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  try { localStorage.setItem('gi-theme', next); } catch (e) {}
  location.reload();
}

export function init() {
  document.addEventListener('click', onClick);
}
