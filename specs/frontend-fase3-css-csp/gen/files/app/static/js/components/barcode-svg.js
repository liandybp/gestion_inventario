// Dibuja un código de barras en un <svg> SIN atributos style (CSP style-src 'self').
// JsBarcode (global) solo calcula las barras en modo objeto; la geometría replica su renderer SVG
// 3.11.6 con las mismas operaciones, y los estilos van como atributos de presentación (fill, font-*).
const SVG_NS = 'http://www.w3.org/2000/svg';

const DEFAULTS = {
  width: 2, height: 100, format: 'auto', displayValue: true, fontOptions: '', font: 'monospace',
  textAlign: 'center', textPosition: 'bottom', textMargin: 2, fontSize: 20,
  background: '#ffffff', lineColor: '#000000', margin: 10,
};

function withMargins(o) {
  o.marginTop = o.marginTop || o.margin;
  o.marginBottom = o.marginBottom || o.margin;
  o.marginRight = o.marginRight || o.margin;
  o.marginLeft = o.marginLeft || o.margin;
  return o;
}

function measureText(text, o) {
  const ctx = document.createElement('canvas').getContext('2d');
  ctx.font = o.fontOptions + ' ' + o.fontSize + 'px ' + o.font;
  const m = ctx.measureText(text);
  return m ? m.width : 0;
}

function layout(enc, o) {
  const textWidth = o.displayValue ? measureText(enc.text, o) : 0;
  const barcodeWidth = enc.data.length * o.width;
  enc.width = Math.ceil(Math.max(textWidth, barcodeWidth));
  enc.height = o.height + (o.displayValue && enc.text.length > 0 ? o.fontSize + o.textMargin : 0)
    + o.marginTop + o.marginBottom;
  enc.barcodePadding = 0;
  if (o.displayValue && barcodeWidth < textWidth) {
    if (o.textAlign === 'center') enc.barcodePadding = Math.floor((textWidth - barcodeWidth) / 2);
    else if (o.textAlign === 'right') enc.barcodePadding = Math.floor(textWidth - barcodeWidth);
  }
}

function el(name, attrs, parent) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  parent.appendChild(node);
  return node;
}

function rect(x, y, w, h, parent) {
  return el('rect', { x, y, width: w, height: h }, parent);
}

export function renderBarcodeSvg(svg, code, options) {
  const base = withMargins(Object.assign({}, DEFAULTS, options));
  const target = {};
  window.JsBarcode(target, code, Object.assign({}, options));
  const encodings = target.encodings || [];
  const opts = encodings.map((enc) => withMargins(Object.assign({}, base, enc.options)));
  encodings.forEach((enc, i) => layout(enc, opts[i]));

  while (svg.firstChild) svg.removeChild(svg.firstChild);
  const total = encodings.reduce((acc, enc) => acc + enc.width, 0);
  const height = encodings.reduce((acc, enc) => Math.max(acc, enc.height), 0);
  const width = total + base.marginLeft + base.marginRight;
  svg.setAttribute('width', width + 'px');
  svg.setAttribute('height', height + 'px');
  svg.setAttribute('x', '0px');
  svg.setAttribute('y', '0px');
  svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
  svg.setAttribute('version', '1.1');
  if (base.background) rect(0, 0, width, height, svg).setAttribute('fill', base.background);

  let x = base.marginLeft;
  encodings.forEach((enc, i) => {
    const o = opts[i];
    const g = el('g', { transform: 'translate(' + x + ', ' + o.marginTop + ')', fill: o.lineColor }, svg);
    const yFrom = o.textPosition === 'top' ? o.fontSize + o.textMargin : 0;
    let run = 0;
    let pos = 0;
    for (let b = 0; b < enc.data.length; b++) {
      pos = b * o.width + enc.barcodePadding;
      if (enc.data[b] === '1') run++;
      else if (run > 0) { rect(pos - o.width * run, yFrom, o.width * run, o.height, g); run = 0; }
    }
    if (run > 0) rect(pos - o.width * (run - 1), yFrom, o.width * run, o.height, g);
    if (o.displayValue) {
      const y = o.textPosition === 'top' ? o.fontSize - o.textMargin : o.height + o.textMargin + o.fontSize;
      let tx;
      let anchor;
      if (o.textAlign === 'left' || enc.barcodePadding > 0) { tx = 0; anchor = 'start'; }
      else if (o.textAlign === 'right') { tx = enc.width - 1; anchor = 'end'; }
      else { tx = enc.width / 2; anchor = 'middle'; }
      const attrs = { 'text-anchor': anchor, x: tx, y, 'font-family': o.font, 'font-size': o.fontSize + 'px' };
      if (/bold/.test(o.fontOptions)) attrs['font-weight'] = 'bold';
      if (/italic/.test(o.fontOptions)) attrs['font-style'] = 'italic';
      el('text', attrs, g).appendChild(document.createTextNode(enc.text));
    }
    x += enc.width;
  });
}
