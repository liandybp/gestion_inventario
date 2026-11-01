// Comportamientos de formulario declarados con data-js-* (sustituyen a los onclick=/onchange=
// /onkeydown= inline, que el CSP sin 'unsafe-inline' bloquea):
//   data-js-select-all="<name>"   casilla que marca/desmarca los input[name=<name>] de su form
//   data-js-autosubmit            al cambiar, envía su formulario (form.submit(), sin htmx)
//   data-js-mirror-to="<id>"      al cambiar, copia su valor al elemento con ese id
//   data-js-enter-focus="<name>"  Enter no envía: pasa el foco al input[name=<name>] del form
function onClick(evt) {
  const master = evt.target.closest('[data-js-select-all]');
  if (!master) return;
  const form = master.closest('form');
  if (!form) return;
  const name = master.getAttribute('data-js-select-all');
  form.querySelectorAll(`input[name="${name}"]`).forEach((cb) => { cb.checked = master.checked; });
}

function onChange(evt) {
  const el = evt.target;
  if (!(el instanceof Element)) return;
  if (el.hasAttribute('data-js-autosubmit') && el.form) {
    el.form.submit();
    return;
  }
  const mirrorId = el.getAttribute('data-js-mirror-to');
  if (mirrorId) {
    const target = document.getElementById(mirrorId);
    if (target) target.value = el.value;
  }
}

function onKeydown(evt) {
  const el = evt.target;
  if (evt.key !== 'Enter' || !(el instanceof Element)) return;
  const name = el.getAttribute('data-js-enter-focus');
  if (!name || !el.form) return;
  evt.preventDefault();
  const next = el.form.querySelector(`input[name="${name}"]`);
  if (next) {
    next.focus();
    if (next.select) next.select();
  }
}

export function init() {
  document.addEventListener('click', onClick);
  document.addEventListener('change', onChange);
  document.addEventListener('keydown', onKeydown);
}
