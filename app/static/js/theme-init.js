// Aplica el tema guardado ANTES del primer pintado (evita el destello de color).
// Script clásico y síncrono en <head>: no puede ser un módulo (los módulos se difieren).
(function () {
  try {
    var t = localStorage.getItem("gi-theme") || "light";
    document.documentElement.setAttribute("data-theme", t);
  } catch (e) { document.documentElement.setAttribute("data-theme", "light"); }
})();
