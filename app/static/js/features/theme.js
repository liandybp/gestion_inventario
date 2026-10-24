// Botón de tema claro/oscuro ([data-js-theme-toggle]). Sin recarga: el CSS cambia solo con
// data-theme y los gráficos se redibujan al recibir gi:themechange (components/chart-palette.js).
import { THEME_EVENT } from '../components/chart-palette.js';

function onClick(evt) {
  if (!evt.target.closest('[data-js-theme-toggle]')) return;
  const root = document.documentElement;
  const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  root.setAttribute('data-theme', next);
  try { localStorage.setItem('gi-theme', next); } catch (e) {}
  document.dispatchEvent(new CustomEvent(THEME_EVENT, { detail: { theme: next } }));
}

export function init() {
  document.addEventListener('click', onClick);
}
