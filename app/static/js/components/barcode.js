// Códigos de barras: <svg class="barcode" data-code="..."> se dibuja sin estilos inline (barcode-svg.js).
import { onContent } from '../core/content.js';
import { renderBarcodeSvg } from './barcode-svg.js';

function renderBarcodes(root) {
  if (!window.JsBarcode) return;
  const container = root || document;
  const svgs = container.querySelectorAll('svg.barcode[data-code]:not([data-rendered="1"])');
  svgs.forEach((svg) => {
    const code = svg.getAttribute('data-code') || '';
    if (!code) return;
    try {
      renderBarcodeSvg(svg, code, { format: 'CODE128', displayValue: false, height: 28, margin: 0, width: 1 });
      svg.setAttribute('data-rendered', '1');
    } catch (e) {}
  });
}

export function init() {
  onContent(renderBarcodes);
}
