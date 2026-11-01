// Páginas de impresión (traspaso, reposición): botón [data-js-print].
document.addEventListener('click', (evt) => {
  if (evt.target.closest('[data-js-print]')) window.print();
});
