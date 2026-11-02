// Colores de los gráficos desde las variables CSS de app.css (:root y [data-theme="dark"]).
// Se leen en cada render: tras un cambio de tema (evento gi:themechange) ya traen la paleta nueva.
function rootStyle() {
  return getComputedStyle(document.documentElement);
}

export function cssVar(name) {
  return rootStyle().getPropertyValue(name).trim();
}

// --<prefix>-1, --<prefix>-2, … hasta la primera vacía.
export function cssList(prefix) {
  const style = rootStyle();
  const out = [];
  for (let i = 1; i <= 24; i++) {
    const value = style.getPropertyValue(`--${prefix}-${i}`).trim();
    if (!value) break;
    out.push(value);
  }
  return out;
}

// Texto y líneas de ejes/leyendas del tema actual; llamar antes de crear cada gráfico.
export function applyChartDefaults() {
  if (!window.Chart) return;
  window.Chart.defaults.color = cssVar('--chart-text');
  window.Chart.defaults.borderColor = cssVar('--chart-border');
}

export const THEME_EVENT = 'gi:themechange';
