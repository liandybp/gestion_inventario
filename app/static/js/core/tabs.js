// Pestañas del sidebar: recordar la activa (localStorage), restaurarla al cargar,
// reintentar la carga inicial de #tab-content y el menú móvil.
const ACTIVE_TAB_KEY = 'active_tab_v1';

function setActiveTab(tabEl) {
  document.querySelectorAll('.sidebar-item').forEach((t) => t.setAttribute('aria-selected', 'false'));
  if (tabEl) tabEl.setAttribute('aria-selected', 'true');
}

function saveActiveTab(tabEl) {
  if (!tabEl) return;
  const tabId = tabEl.id || '';
  const url = tabEl.getAttribute('hx-get') || '';
  if (!url) return;
  try {
    localStorage.setItem(ACTIVE_TAB_KEY, JSON.stringify({ id: tabId, url }));
  } catch (e) {}
}

function restoreActiveTab() {
  let saved = null;
  try {
    saved = JSON.parse(localStorage.getItem(ACTIVE_TAB_KEY) || 'null');
  } catch (e) {
    saved = null;
  }
  if (!saved || !saved.url) return;

  const tabContent = document.getElementById('tab-content');
  if (!tabContent) return;

  const savedBtn = saved.id ? document.getElementById(saved.id) : null;
  let btn = savedBtn && savedBtn.classList.contains('sidebar-item') ? savedBtn : null;
  if (!btn && saved.url) {
    btn = document.querySelector(`.sidebar-item[hx-get="${saved.url}"]`);
  }
  if (!btn) return;

  const url = btn.getAttribute('hx-get') || '';
  if (!url) return;

  tabContent.setAttribute('hx-get', url);
  setActiveTab(btn);
}

function onTabClick(evt) {
  const el = evt.target.closest('.sidebar-item');
  if (!el) return;
  setActiveTab(el);
  saveActiveTab(el);
}

function retryInitialTabLoad() {
  try {
    const tabContent = document.getElementById('tab-content');
    if (tabContent && (!String(tabContent.innerHTML || '').trim())) {
      const url = tabContent.getAttribute('hx-get') || '';
      if (url && window.htmx && typeof window.htmx.trigger === 'function') {
        setTimeout(() => {
          if (!String(tabContent.innerHTML || '').trim()) {
            try { window.htmx.trigger(tabContent, 'load'); } catch (e) {}
          }
        }, 200);
      }
    }
  } catch (e) {}
}

function initMobileMenu() {
  const menuToggle = document.getElementById('mobile-menu-toggle');
  const menuOverlay = document.getElementById('mobile-menu-overlay');
  const sidebar = document.getElementById('sidebar');
  if (!menuToggle || !menuOverlay || !sidebar) return;

  const toggleMenu = () => {
    sidebar.classList.toggle('active');
    menuOverlay.classList.toggle('active');
  };
  menuToggle.addEventListener('click', toggleMenu);
  menuOverlay.addEventListener('click', toggleMenu);
  // Cerrar el menú al elegir una pestaña.
  sidebar.addEventListener('click', (e) => {
    if (e.target.closest('.sidebar-item')) {
      sidebar.classList.remove('active');
      menuOverlay.classList.remove('active');
    }
  });
}

export function init() {
  // Antes de DOMContentLoaded: htmx lee el hx-get de #tab-content al procesar el body.
  restoreActiveTab();
  document.addEventListener('click', onTabClick);
  document.addEventListener('DOMContentLoaded', () => {
    retryInitialTabLoad();
    initMobileMenu();
  });
}
