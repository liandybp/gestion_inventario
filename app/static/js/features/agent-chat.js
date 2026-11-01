// Panel de chat del copiloto (dashboard.html, solo admin/owner y modo no desktop):
// abrir/cerrar ([data-js-agent-toggle]), tamaño ([data-js-agent-size]), envío optimista
// y visibilidad de la burbuja solo en la pestaña "Dashboard Dinámico".
const SIZES = ['small', 'medium', 'large'];

function togglePanel() {
  const panel = document.getElementById('agent-panel');
  if (panel) panel.classList.toggle('hidden');
}

function cyclePanelSize() {
  const panel = document.getElementById('agent-panel');
  if (!panel) return;
  let idx = SIZES.indexOf(panel.dataset.size || 'medium');
  idx = (idx + 1) % SIZES.length;
  panel.dataset.size = SIZES[idx];
  panel.classList.remove('small', 'medium', 'large');
  panel.classList.add(SIZES[idx]);
}

// Chat optimista: el mensaje del usuario aparece al instante + indicador "escribiendo…".
function appendBubble(role, text) {
  const thread = document.getElementById('agent-chat-thread');
  if (!thread) return;
  const d = document.createElement('div');
  d.className = 'msg msg-' + role;
  d.textContent = text;
  thread.appendChild(d);
  thread.scrollTop = thread.scrollHeight;
}

function appendTyping() {
  const thread = document.getElementById('agent-chat-thread');
  if (!thread || document.getElementById('agent-typing')) return;
  const d = document.createElement('div');
  d.id = 'agent-typing';
  d.className = 'msg msg-assistant agent-typing';
  d.innerHTML = '<span class="dot"></span><span class="dot"></span><span class="dot"></span>';
  thread.appendChild(d);
  thread.scrollTop = thread.scrollHeight;
}

function initForm() {
  // Enviar el chat: el form usa hx-post (htmx garantiza el envío). Este listener hace el
  // append optimista + indicador, y limpia el input en un microtask para no interferir con
  // la lectura del form por htmx.
  const form = document.getElementById('agent-chat-form');
  if (!form) return;
  const input = form.querySelector('textarea[name=message]');
  form.addEventListener('submit', () => {
    const msg = (input && input.value || '').trim();
    if (!msg) return;
    appendBubble('user', msg); // el mensaje va al historial YA
    appendTyping(); // indicador "escribiendo…"
    setTimeout(() => { if (input) input.value = ''; }, 0);
  }, true); // capture: corre antes de que htmx lea el formulario
  // Enter envía; Shift+Enter inserta salto de línea (textarea multilínea).
  if (input) {
    input.addEventListener('keydown', (evt) => {
      if (evt.key === 'Enter' && !evt.shiftKey) {
        evt.preventDefault();
        form.requestSubmit();
      }
    });
  }
}

export function init() {
  if (!document.getElementById('agent-panel')) return;
  initForm();

  document.addEventListener('click', (evt) => {
    if (evt.target.closest('[data-js-agent-toggle]')) togglePanel();
    else if (evt.target.closest('[data-js-agent-size]')) cyclePanelSize();
  });

  // Al terminar la respuesta: quitar el indicador; si fue un error (4xx), htmx no swapea
  // >=400, así que insertamos el reply de error a mano.
  document.body.addEventListener('htmx:afterRequest', (evt) => {
    const cfg = evt.detail.requestConfig || {};
    if (cfg.path !== '/ui/agent/chat') return;
    const typing = document.getElementById('agent-typing');
    if (typing) typing.remove();
    const xhr = evt.detail.xhr;
    if (xhr && xhr.status >= 400 && xhr.responseText) {
      const thread = document.getElementById('agent-chat-thread');
      if (thread) thread.insertAdjacentHTML('beforeend', xhr.responseText);
    }
  });

  // Mostrar la burbuja SOLO en el Dashboard Dinámico. Solo se actualiza al cambiar de
  // pestaña (swap en #tab-content); la respuesta del chat (swap en #agent-chat-thread)
  // NO debe ocultar la burbuja.
  document.body.addEventListener('htmx:afterSwap', (evt) => {
    const target = evt.detail.target;
    if (target && target.id === 'tab-content') {
      const path = (evt.detail.requestConfig && evt.detail.requestConfig.path) || '';
      const onDashboard = (path === '/ui/tabs/agent');
      const fab = document.getElementById('agent-fab');
      if (fab) fab.style.display = onDashboard ? 'flex' : 'none';
      if (!onDashboard) {
        const panel = document.getElementById('agent-panel');
        if (panel) panel.classList.add('hidden');
      }
    }
    if (target && target.classList && target.classList.contains('agent-thread')) {
      target.scrollTop = target.scrollHeight;
    }
  });
}
