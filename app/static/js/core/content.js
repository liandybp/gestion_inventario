// Registro de "enhancers": funciones que se aplican al contenido nuevo.
// Se ejecutan sobre `document` en DOMContentLoaded y sobre cada elemento que swapea htmx
// (evt.target de htmx:afterSwap: el target principal y también cada elemento OOB), tras dos
// frames, para que el layout del contenido nuevo ya esté calculado.
const enhancers = [];

export function onContent(fn) {
  enhancers.push(fn);
}

function run(root, fromSwap) {
  enhancers.forEach((fn) => fn(root, fromSwap));
}

export function init() {
  document.addEventListener('DOMContentLoaded', () => run(document, false));
  document.addEventListener('htmx:afterSwap', (evt) => {
    const target = evt.target;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => run(target, true));
    });
  });
}
