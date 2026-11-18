# Requirements — PDF de documentos de venta con el diseño de la plantilla "OFERTA"

Modelo: Opus 5.5 (`cc_claude_code`, Opus-1) · Fecha: 2026-11-18 · Slug: `pdf-documentos-venta-oferta`
Versión de partida: `0.5.1` → objetivo `0.5.2` (PATCH).

## Contexto

El PDF de factura (F) y presupuesto/oferta (P) se genera en `app/sales_document_pdf.py:14` con un diseño simple
(cabecera de texto, tabla con relleno azul oscuro). El usuario pide reproducir el diseño de la plantilla de referencia
`~/Descargas/COMERCIO GEELY CK B244742.pdf`, con sus colores y líneas, y añadir:

- teléfono (`54664732 / 50781349`) y email (`autosandero@gmail.com`) del emisor;
- fecha de oferta, fecha de vencimiento y número de oferta;
- bloque **FORMA DE PAGO** con los datos bancarios: Banco Metropolitano, Sucursal 248, No. Cuenta (CUP)
  `0598770024085419`;
- bloque **Recibido por / Cargo / Firma / Fecha**.

Decisiones del usuario (2026-11-18):

| Tema | Decisión |
|---|---|
| Fecha de vencimiento | Plazo por defecto en días por negocio; el formulario rellena la fecha y se puede cambiar por documento |
| Factura | El mismo diseño que la oferta, **sin** fecha de vencimiento. Cada bloque sale "en caso de que aplique" |
| Extras de la referencia | Marca/M, Matrícula, frase de plazo de ejecución y lema: **fuera**. Columna UNIDADES: dentro (el dato ya existe) |
| Email | `autosandero@gmail.com` |
| Moneda de las columnas de importe | La que se elige al construir el documento (selector "Moneda" del carrito, guardada en `currency_code`), con `GEN` → `CUP` (`IMPORTE CUP`, `IMPORTE USD`, `IMPORTE EUR`) |
| Precio unitario y subtotal (2026-11-18, 2ª ronda) | Se **mantienen**: columna de precio unitario en la tabla y fila Subtotal sobre el TOTAL |
| Iconos (2ª ronda) | Teléfono y email con icono, como en la referencia |
| Formato de fecha (2ª ronda) | `dd/mm/aaaa` |

Glosario (un término por concepto):

- **documento**: fila de `sales_documents` (tipo `F` = factura, `P` = presupuesto/oferta).
- **etiqueta del documento**: `invoice_label` / `quote_label` del negocio (hoy "Factura" / "Presupuesto"; el usuario
  puede poner "Oferta").
- **datos bancarios**: banco, sucursal, número de cuenta y moneda de la cuenta del negocio emisor.
- **plazo de validez**: número de días por defecto entre la fecha de emisión y la fecha de vencimiento de un `P`.
- **fecha de vencimiento**: columna nueva `sales_documents.valid_until` (fecha, sin hora).

## 1.1 Requisitos funcionales

### REQ-1: Datos bancarios del negocio
**Priority:** Must Have

WHEN un admin crea o edita un negocio en *Negocios*, THE SYSTEM SHALL permitir guardar banco, sucursal, número de
cuenta y moneda de la cuenta, y SHALL exponerlos en `IssuerConfig` (`bank_name`, `bank_branch`, `bank_account`,
`bank_account_currency`).

```gherkin
Scenario: El admin guarda los datos bancarios
  GIVEN un admin autenticado y el negocio "recambios"
  WHEN abre Negocios → Editar, escribe "Banco Metropolitano", "248", "0598770024085419", "CUP" y guarda
  THEN el modal de edición muestra esos cuatro valores al reabrirlo
  AND get_business_config(...).issuer.bank_account == "0598770024085419"

Scenario: Guardar los datos bancarios no borra el resto del emisor (negativo)
  GIVEN el negocio con TCP "Randy Manuel Delgado", NIT "01030573448" y teléfono "54664732 / 50781349"
  WHEN el admin edita solo el banco y guarda
  THEN el TCP, el NIT y el teléfono NO cambian
  AND los socios y el pendiente de dividendos NO cambian
```

### REQ-2: Plazo de validez por negocio
**Priority:** Must Have

WHEN un admin crea o edita un negocio, THE SYSTEM SHALL permitir guardar el plazo de validez en días (entero 0-365,
por defecto 15) y SHALL exponerlo en `SalesDocumentsConfig.quote_validity_days`.

```gherkin
Scenario: El admin cambia el plazo de validez
  GIVEN un admin autenticado
  WHEN edita el negocio y pone 30 en "Validez de presupuesto (días)"
  THEN el config del negocio tiene quote_validity_days == 30

Scenario: Un valor no válido no se guarda (negativo)
  GIVEN el negocio con quote_validity_days == 15
  WHEN el admin envía "abc" o "-3" o "400"
  THEN quote_validity_days sigue en 15 para "abc" y queda acotado a 0 para "-3" y a 365 para "400"
  AND el resto de la configuración del negocio NO cambia
```

### REQ-3: Fecha de vencimiento al emitir
**Priority:** Must Have

WHEN el usuario abre el formulario de emisión, THE SYSTEM SHALL mostrar un campo de fecha "Vencimiento (solo
presupuesto)" con el valor hoy (UTC) + plazo de validez. WHEN se emite un `P`, THE SYSTEM SHALL guardar esa fecha en
`valid_until`; si el campo llega vacío, SHALL guardar fecha de emisión + plazo de validez. WHEN se emite una `F`, THE
SYSTEM SHALL guardar `valid_until = NULL`.

```gherkin
Scenario: Presupuesto con vencimiento elegido
  GIVEN un carrito con una línea y el plazo de validez 15
  WHEN el usuario emite un P con vencimiento "2026-12-31"
  THEN el documento tiene valid_until == 2026-12-31

Scenario: Presupuesto con el campo vacío
  WHEN el usuario emite un P con el campo de vencimiento vacío
  THEN valid_until == fecha de emisión (UTC) + 15 días

Scenario: La factura no guarda vencimiento (negativo)
  WHEN el usuario emite una F con vencimiento "2026-12-31"
  THEN valid_until es NULL

Scenario: Fecha no válida (negativo)
  WHEN el usuario emite un P con vencimiento "31/12/2026" o anterior a la fecha de emisión
  THEN la respuesta es 422 HTML y el panel muestra el mensaje "Fecha de vencimiento inválida"
  AND NO se crea ningún documento
  AND el carrito NO se vacía
  AND el formulario conserva cliente, notas y la fecha escrita, para corregirla y volver a emitir

Scenario: La fecha elegida sobrevive a los cambios del carrito (negativo)
  GIVEN el usuario escribió vencimiento "2026-12-31" en el formulario de emisión
  WHEN añade, cambia, quita una línea o vacía el carrito
  THEN el campo de vencimiento sigue mostrando "2026-12-31"
```

### REQ-4: Vencimiento en la vista previa y en la edición
**Priority:** Should Have

WHEN el usuario pide la vista previa de un `P`, THE SYSTEM SHALL mostrar "Vencimiento: dd/mm/aaaa" y SHALL llevar la
fecha al formulario "Emitir ahora". WHEN el usuario edita un `P` ya emitido, THE SYSTEM SHALL permitir cambiar
`valid_until` con la misma validación de REQ-3.

```gherkin
Scenario: La vista previa conserva el vencimiento
  GIVEN un P en vista previa con vencimiento "2026-12-31"
  WHEN el usuario pulsa "Emitir ahora"
  THEN el documento emitido tiene valid_until == 2026-12-31

Scenario: La vista previa de una factura no muestra vencimiento (negativo)
  WHEN el usuario pide la vista previa de una F
  THEN el texto "Vencimiento" NO aparece en la vista previa

Scenario: Editar el vencimiento de un presupuesto
  GIVEN un P emitido con valid_until 2026-12-03
  WHEN el usuario lo edita y pone "2026-12-20"
  THEN valid_until == 2026-12-20
  AND las líneas y el total NO cambian

Scenario: Editar un presupuesto antiguo sin fecha no le inventa una (negativo)
  GIVEN un P con valid_until NULL
  WHEN el usuario cambia solo las notas y deja vacío el vencimiento
  THEN valid_until sigue NULL

Scenario: Fecha no válida al editar no toca nada (negativo)
  GIVEN un P con cliente "A", 2 líneas y total 10
  WHEN el usuario envía vencimiento "31/12/2026", cambia el cliente a "B" y marca una línea para borrar
  THEN la respuesta es 422 HTML con "Fecha de vencimiento inválida"
  AND el documento guardado sigue con cliente "A", 2 líneas, total 10 y el valid_until anterior
```

### REQ-5: Diseño del PDF según la plantilla de referencia
**Priority:** Must Have

WHEN se genera el PDF de un documento, THE SYSTEM SHALL dibujar, en este orden:

1. **Cabecera**: banda azul clara `RGB(220,230,241)` a todo el ancho. Dentro, a la izquierda: nombre del emisor en
   negrita, `TCP <titular>` y `NIT <nit>` (cada línea solo si el dato existe). A la derecha: dirección (si existe),
   el teléfono precedido de un **icono de teléfono** (si existe) y el email precedido de un **icono de sobre** (si
   existe). Los iconos son vectoriales, en el azul de la caja TOTAL.
2. **Barra de título**: `DESTINATARIO:` a la izquierda y la etiqueta del documento en mayúsculas a la derecha, en
   negrita, sobre una **doble línea** negra.
3. **Bloque cliente/documento**: a la izquierda `CLIENTE: <nombre>`, `ID: <identificación>` y la dirección del
   cliente (si existe). A la derecha, un bloque azul claro con filas etiqueta/valor:
   `No. <etiqueta>:` código; `Fecha de <etiqueta en minúsculas>:` fecha de emisión `dd/mm/aaaa`; y, solo en `P`,
   `Fecha de vencimiento:` `dd/mm/aaaa` (vacío si el documento no tiene `valid_until`).
4. **Subtotal y TOTAL**: bajo el bloque documento, una fila azul clara `Subtotal:` con el subtotal; debajo, el
   rectángulo azul `RGB(17,90,156)` con texto blanco en negrita `TOTAL:` y el total. Ambos con separador de miles
   (`490,875.00`).
5. **Tabla**: cabecera `CONCEPTO | UNIDADES | CANTIDAD | PRECIO <moneda> | IMPORTE <moneda>` en negrita sobre una doble
   línea; una fila por línea con la descripción, la unidad (vacía si no hay), la cantidad (hasta 4 decimales sin ceros
   finales, con separador de miles: `1`, `2.5`, `0.0001`, `1,234,567`), el precio unitario y el importe de la línea,
   ambos con separador de miles; doble línea al final de la tabla. Una fila nunca se parte entre dos páginas; si la tabla sigue en otra página, la cabecera de
   la tabla se repite.
6. **Notas** del documento, si existen.
7. **Pie** a dos columnas: a la izquierda `FORMA DE PAGO` (negrita), `Efectivo___`, `Cheque___`,
   `Transferencia Bancaria___` y, si hay datos bancarios, `<banco>, Sucursal <sucursal>` y
   `No. Cuenta (<moneda>): <cuenta>`; a la derecha `Recibido por:` con su línea, `Cargo: ___`, `Firma: ___`,
   `Fecha: ___`. El pie nunca se parte entre dos páginas.

Los textos largos (etiqueta de documento personalizada, dirección del emisor) no se salen de su caja: las etiquetas
del bloque documento reducen el tamaño de letra hasta caber (mínimo 6 pt) y la cabecera parte las líneas largas.

```gherkin
Scenario: PDF de un presupuesto completo
  GIVEN el negocio DyB Autopartes con TCP, NIT, teléfono "54664732 / 50781349", email "autosandero@gmail.com"
  AND datos bancarios "Banco Metropolitano" / "248" / "0598770024085419" / "CUP"
  AND un P en moneda GEN con 2 líneas (JGO 1 × 303875, U 1 × 187000) y valid_until 2026-12-03
  WHEN se descarga su PDF
  THEN el texto del PDF contiene "DyB Autopartes", "TCP Randy Manuel Delgado", "NIT 01030573448"
  AND contiene "54664732 / 50781349" y "autosandero@gmail.com"
  AND contiene "DESTINATARIO:", "No. Presupuesto:", "Fecha de vencimiento:" y "03/12/2026"
  AND contiene "Subtotal:", "TOTAL:" y "490,875.00"
  AND contiene "CONCEPTO", "UNIDADES", "CANTIDAD", "PRECIO CUP", "IMPORTE CUP", "JGO" y "303,875.00"
  AND la página tiene 2 imágenes vectoriales de icono (teléfono y email) en la cabecera
  AND contiene "FORMA DE PAGO", "Banco Metropolitano, Sucursal 248", "No. Cuenta (CUP): 0598770024085419"
  AND contiene "Recibido por:", "Cargo:", "Firma:"

Scenario: El PDF ya no tiene el diseño antiguo (negativo)
  WHEN se descarga el PDF de cualquier documento
  THEN el texto NO contiene "Código:", "ID fiscal", "Tel.:" ni "(Precios sin IVA)"

Scenario: Sin teléfono ni email no hay iconos sueltos (negativo)
  GIVEN un negocio sin teléfono ni email
  WHEN se descarga el PDF
  THEN la cabecera NO tiene iconos

Scenario: Sin datos bancarios no hay líneas de banco vacías (negativo)
  GIVEN un negocio sin datos bancarios
  WHEN se descarga el PDF
  THEN contiene "FORMA DE PAGO"
  AND NO contiene "Sucursal" ni "No. Cuenta"
```

### REQ-6: Etiqueta de moneda de la columna de importe
**Priority:** Must Have

WHEN se dibuja la cabecera de la tabla, THE SYSTEM SHALL usar `PRECIO <moneda>` e `IMPORTE <moneda>` con la moneda
que se eligió al construir el documento (`currency_code`) en mayúsculas, salvo `GEN`, que SHALL mostrarse como `CUP`.
Si el documento no tiene moneda, SHALL mostrar `PRECIO` e `IMPORTE`.

```gherkin
Scenario Outline: Etiqueta de moneda
  WHEN el documento tiene currency_code "<code>"
  THEN la cabecera dice "<label>"
  Examples:
    | code | label        |
    | GEN  | IMPORTE CUP  |
    | USD  | IMPORTE USD  |
    | eur  | IMPORTE EUR  |
    |      | IMPORTE      |

Scenario: La moneda la decide el documento, no el negocio
  GIVEN el negocio recambios con moneda por defecto USD
  WHEN el usuario elige "Moneda nacional (MN$)" en el carrito y emite
  THEN el PDF dice "PRECIO CUP" e "IMPORTE CUP"

Scenario: GEN nunca sale como texto en el PDF (negativo)
  WHEN el documento está en GEN
  THEN el texto del PDF NO contiene "GEN"
```

### REQ-7: La factura usa el mismo diseño, sin vencimiento
**Priority:** Must Have

WHEN se genera el PDF de una `F`, THE SYSTEM SHALL usar el mismo diseño de REQ-5 con la etiqueta de factura y SHALL
omitir la fila "Fecha de vencimiento".

```gherkin
Scenario: PDF de una factura
  GIVEN una F emitida
  WHEN se descarga su PDF
  THEN contiene "FACTURA", "No. Factura:", "Fecha de factura:", "FORMA DE PAGO" y "Recibido por:"
  AND NO contiene "Fecha de vencimiento"
```

### REQ-8: Documentos ya emitidos siguen generando PDF
**Priority:** Must Have

WHEN se pide el PDF de un documento emitido antes de este cambio (sin `valid_until`, líneas sin unidad), THE SYSTEM
SHALL generarlo con el diseño nuevo sin error.

```gherkin
Scenario: Presupuesto antiguo sin vencimiento
  GIVEN un P con valid_until NULL y líneas con unit_of_measure NULL
  WHEN se descarga su PDF
  THEN la respuesta es 200 application/pdf
  AND contiene "Fecha de vencimiento:" sin fecha detrás
  AND la columna UNIDADES de esas filas está vacía
```

### REQ-9: Longitud de los datos bancarios
**Priority:** Must Have

WHEN un admin guarda los datos bancarios, THE SYSTEM SHALL limitar banco a 128 caracteres, sucursal y cuenta a 64 y
moneda a 8 (atributo `maxlength` en el form y recorte en el servidor), para que SQLite y PostgreSQL se comporten igual.

```gherkin
Scenario: Una sucursal demasiado larga no rompe el guardado (negativo)
  WHEN el admin envía una sucursal de 65 caracteres saltándose el form
  THEN el negocio se guarda con los 64 primeros caracteres
  AND la respuesta NO es 500
```

(Antes REQ-9 era sembrar los datos de DyB en los `.conf`: pasa a fuera de alcance. Los `.conf` son legacy y solo se
leen en el backfill único de la v4; en producción y staging los datos se introducen en *Negocios → Editar*.)

### REQ-10: Tests e2e del flujo completo
**Priority:** Must Have

THE SYSTEM SHALL tener un test e2e Playwright que haga login, configure el negocio (teléfono, email, datos bancarios),
añada al carrito `E2E-SKU1` (1 × 303875) y `E2E-SKU2` (1 × 187000), elija moneda `GEN` y tipo `P`, escriba un
vencimiento, emita, descargue el PDF por `/ui/sales-doc/{id}/pdf` y compruebe su texto con `pdfplumber`
(`PRECIO CUP`, `IMPORTE CUP`, `ud`, `303,875.00`, `Subtotal:`, `490,875.00`, banco, vencimiento `31/12/2099`; sin
`GEN` ni `Tel.:`); y otro que emita
una `F` y compruebe que su PDF no tiene "Fecha de vencimiento".

## 1.2 MoSCoW

| Priority | REQs |
|---|---|
| Must Have | REQ-1, REQ-2, REQ-3, REQ-5, REQ-6, REQ-7, REQ-8, REQ-9, REQ-10 |
| Should Have | REQ-4 |
| Could Have | — |
| Won't Have | Ver 1.4 |

## 1.3 Requisitos no funcionales

| Category | Question | Answer |
|---|---|---|
| Performance | ¿Tiempo de generación? | Igual que hoy: un PDF de 1 página con fpdf2 se genera en milisegundos. Sin consultas nuevas: los datos salen del `UIContext` y del documento ya cargado |
| Security | ¿Datos sensibles nuevos? | El número de cuenta bancaria del **negocio** (no de clientes), que ya se imprime en documentos públicos. Solo lo edita quien hoy edita el negocio (sin cambio de permisos). Sin PII nueva |
| Scalability | ¿Volumen? | No aplica: decenas de documentos al mes |
| Observability | ¿Logs nuevos? | Fecha de vencimiento rechazada → `_log.info` con el valor recibido. Fallo de PDF ya se registra en `sales_doc_pdf` |
| Reliability | ¿Recuperación? | Migración idempotente al arrancar (`ADD COLUMN` solo si falta). Documentos antiguos siguen funcionando (REQ-8) |
| Compatibilidad | ¿Caracteres? | El PDF usa Helvetica (latin-1). Texto fuera de latin-1 se sustituye por `?` como hoy (`_safe_pdf_text`) |

## 1.4 Fuera de alcance

- Marca/M y Matrícula del vehículo, frase "Dichos trabajos… días/horas", lema bajo el nombre, número `LOT.`
- Icono de ubicación para la dirección (el usuario pidió iconos solo para teléfono y email).
- Opciones de moneda nuevas en el selector del carrito: siguen USD, EUR y GEN (moneda nacional).
- Guardar en el documento una copia de teléfono, email y datos bancarios: el PDF los lee del negocio en el momento
  de generarlo (como ya hace con el TCP).
- Zona horaria de Cuba en las fechas: la fecha de emisión y el "hoy" siguen en UTC, como el resto del sistema.
- Cambiar la etiqueta "Presupuesto" por "Oferta": el usuario lo hace en *Negocios → Editar*, sin código.
- Rellenar los datos de DyB en producción: lo hace el usuario en *Negocios → Editar* tras el despliegue.
- Sembrar teléfono, email y banco en los `.conf` legacy (antes REQ-9; hallazgo 11 de Codex).
- Convertir en HTML los 422 en JSON que ya existen (cliente o carrito vacíos): solo el error nuevo de vencimiento
  sale como HTML.
- Snapshots nuevos del modal de edición de un `P` y de la vista previa: lo cubren los unit de handlers y el e2e.

## 1.5 Dependencias

- `fpdf2==2.8.7` y `pdfplumber==0.11.10` ya están en `requirements.txt` (CI los instala).
- Migración de esquema idempotente en `app/migrations.py` (SQLite y PostgreSQL 16).
- Sin dependencias de otras ramas: parte de `develop` @ `ae8fda8`.

## 1.6 Preguntas abiertas

Ninguna. Las cinco decisiones de la tabla de contexto las tomó el usuario el 2026-11-18. Proceeding to Phase 2.
