// Actividad del usuario para el timeout de inactividad del servidor: los polls de htmx en
// segundo plano no deben mantener viva la sesión, así que cada petición lleva la hora de la
// última interacción real en la cabecera X-User-Activity.
let lastUserActivity = Date.now();
let lastUserActivityWrite = 0;

function markUserActivity() {
  const now = Date.now();
  if ((now - lastUserActivityWrite) < 1000) return;
  lastUserActivityWrite = now;
  lastUserActivity = now;
}

export function init() {
  ['click', 'keydown', 'scroll', 'touchstart', 'mousemove'].forEach((type) => {
    document.addEventListener(type, markUserActivity, { passive: true, capture: true });
  });
  document.addEventListener('htmx:configRequest', (evt) => {
    try {
      if (evt.detail && evt.detail.headers) {
        evt.detail.headers['X-User-Activity'] = String(lastUserActivity || '');
      }
    } catch (e) {}
  });
}
