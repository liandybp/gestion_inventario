`cc_design_check` terminó con código 0 y sin avisos. Contrasté el diseño con `develop` en `ae8fda8` y ejecuté sondas en memoria con fpdf2 2.8.7. No modifiqué archivos.

## 1. Contradicciones con el código

1. **El vencimiento elegido se pierde al tocar el carrito.** Diseño §§2.1, 2.2 y 2.4.3. **Severidad alta · confianza alta.** Añadir, actualizar, quitar o vaciar una línea reemplaza el panel. Aunque el formulario envía los campos de emisión con `hx-include`, los cuatro handlers reconstruyen el borrador sin `valid_until` y `_get_draft` devuelve una lista cerrada de campos. Elegir una fecha y luego modificar una línea vuelve a mostrar el valor por defecto.  
   **Propuesta:** transportar y recuperar `valid_until` en todos los reemplazos del panel y distinguir un campo ausente de uno que el usuario dejó vacío.  
   `app/templates/components/sales_doc/cart_add.html:6,39`, `app/templates/components/sales_doc/cart_table.html:42–51`, `app/routers/ui_sales_documents.py:115–154,512–552,668–705,749–783,812–845`.

2. **El 422 de vencimiento queda invisible en la UI.** Diseño §§2.2, 2.4.3 y 2.9. **Severidad media · confianza alta.** El `HTTPException` prescrito devuelve JSON. El manejador HTMX solo inserta errores 4xx con `Content-Type: text/html`. Una fecha anterior a la emisión supera la validación del `<input type="date">`, pero al enviarla el usuario no ve por qué falló la operación y puede seguir viendo una vista previa anterior.  
   **Propuesta:** devolver un fragmento HTML 422 para HTMX que muestre el detalle y conserve los valores, con `X-Modal-Keep` en edición si el destino queda fuera del modal.  
   `.venv/lib/python3.12/site-packages/fastapi/exception_handlers.py:11–17` (ruta relativa al entorno), `app/static/js/core/swap.js:17–30`, `app/static/js/core/modals.js:46–66`, `app/templates/components/sales_doc/issue_form.html:8,29–36`.

3. **Editar un presupuesto antiguo asigna una fecha que el usuario dejó vacía.** Diseño §§2.1d, 2.2 y 2.4.3. **Severidad alta · confianza alta.** La matriz exige conservar `NULL`, pero `current=None` significa tanto «emisión» como «edición de un documento sin fecha». La regla propuesta solo conserva `current` si tiene valor. Guardar un cambio de notas en un P histórico con el campo vacío calcula una fecha nueva y modifica su PDF.  
   **Propuesta:** distinguir emisión y edición mediante un parámetro explícito o un centinela y probar que editar un P histórico con fecha vacía conserva `NULL`.  
   `app/routers/ui_sales_documents.py:335–346,415–424`, `app/templates/features/sales_documents/sales_document_edit_form.html:6–12`.

4. **Los datos del e2e propuesto no producen el PDF que exige el propio test.** Diseño §2.8. **Severidad media · confianza alta.** El flujo añade una sola línea de `E2E-SKU1`, cuya unidad es `ud` y cuyo precio por defecto es 2. Las aserciones de REQ-5 incluyen `JGO`, un importe de `303,875.00`, un total de `490,875.00` y `IMPORTE CUP`. Además, el negocio de arranque usa USD y el flujo descrito nunca selecciona `GEN`.  
   **Propuesta:** preparar dos líneas con las unidades y los importes de REQ-5 y seleccionar `GEN` explícitamente antes de emitir.  
   `tests/e2e/conftest.py:47–67`, `app/business_config.recambios.conf:12–14`, `app/routers/ui_sales_documents.py:43–50,560–566,608–616`, `app/templates/components/sales_doc/cart_add.html:18–24`.

## 2. Supuestos sin sonda suficiente

5. **S6 mide un texto distinto del que tiene que caber en la celda.** Diseño §§2.1c y 2.4.1. **Severidad media · confianza alta.** La evidencia da 31,9 mm para «Fecha de vencimiento:» sin registrar el estilo. Medí 34,23 mm para esa etiqueta y 34,76 mm para «Fecha de presupuesto:» en Helvetica negrita de 9 pt, con 1 mm de margen interno por lado. La segunda supera los 34 mm útiles incluso con la etiqueta por defecto. La etiqueta editable «Presupuesto extraordinario» lleva la cadena a 56,99 mm. La misma geometría tampoco dice cómo ajustar direcciones largas a los 86 mm de la cabecera.  
   **Propuesta:** medir y ajustar el texto con la fuente, los márgenes y el ancho efectivos, y probar etiquetas y direcciones largas con sus cajas reales.  
   `app/templates/features/businesses/business_edit_form.html:174–184`, `app/models.py:156–158`, `app/routers/ui_sales_documents.py:923–928`, `app/sales_document_pdf.py:44–53`.

6. **S5 demuestra coincidencias de texto, pero no qué renderizados cambian.** Diseño §§2.1, 2.1c y 2.8. **Severidad baja · confianza alta.** La búsqueda devuelve nueve nombres, pero `modal_sales_document` se renderiza con un documento `F` y el campo nuevo solo aparece en `P`. `frag_salesdoc_customer` captura un fragmento independiente que solo devuelve ID y dirección. Limitar la revisión a esos nueve nombres puede dar por cubierto un modal de P que el arnés nunca renderiza.  
   **Propuesta:** comparar capturas reales y añadir casos de modal `P` y vista previa si se quiere verificar esos cambios con snapshots.  
   `tests/snapshots/ui_snapshot.py:269–274,348,439–443,495–523`, `app/routers/ui_sales_documents.py:505–509`.

7. **`VARCHAR(n)` no valida la longitud de forma uniforme.** Diseño §§2.3 y 2.5. **Severidad media · confianza alta.** `_s` elimina espacios, pero no limita caracteres. Una sucursal de 65 caracteres puede guardarse en SQLite y provocar un error al escribir en PostgreSQL. En edición, el handler tampoco tiene una rama de error para ese fallo del `commit`. S3 acredita la versión de PostgreSQL, pero no prueba el comportamiento del formulario ni la paridad entre motores.  
   **Propuesta:** validar las cuatro longitudes en servidor antes de modificar el negocio y devolver un error de formulario que conserve los valores introducidos.  
   `app/routers/ui_businesses.py:68–80,384–386,405–431,462`, `app/migrations.py:290–294,521–523`.

## 3. Ciclo de vida

8. **Un salto dentro de una fila puede separar descripción, cantidad e importe.** Diseño §2.4.1. **Severidad alta · confianza alta.** El diseño combina `multi_cell`, celdas contiguas de la misma altura y salto automático, pero solo define qué hacer con el pie. Reproduje una descripción de dos líneas a `y=280`: `multi_cell` pasó de página 1, `y=280`, a página 2, `y=20`. Si se reutiliza el cálculo de altura y reposicionamiento del generador actual, la altura sale negativa y las demás columnas se dibujan en otra posición o página. Un pie que no quepa completo también puede dividir sus dos columnas.  
   **Propuesta:** medir la fila y el pie antes de dibujarlos, decidir el salto para cada bloque y repetir la cabecera de tabla en la página nueva.  
   `app/sales_document_pdf.py:40–42,95–112`.

9. **Un presupuesto previsualizado puede quedar bloqueado al cruzar medianoche UTC.** Diseño §§2.1d, 2.2 y 2.9. **Severidad media · confianza alta.** Con plazo 0, una vista previa abierta antes de medianoche lleva la fecha de ese día en un campo oculto. Al pulsar «Emitir ahora» al día siguiente, `issue` toma un `now` nuevo y rechaza esa fecha. El botón sigue enviando el mismo valor y la vista previa carece de un campo editable.  
   **Propuesta:** conservar la distinción entre fecha explícita y fecha calculada, y dar una salida recuperable si el vencimiento de la vista previa ha quedado obsoleto.  
   `app/templates/features/sales_documents/sales_document_preview.html:44–52`, `app/routers/ui_sales_documents.py:900–913,950–958`, `app/static/js/core/modals.js:36–44`.

## 4. Cobertura de requisitos

10. **REQ-4 carece de un escenario negativo para la edición que introduce.** Requisitos §1.1, REQ-4, y diseño §§2.1d y 2.8. **Severidad media · confianza alta.** La única negativa explícita de REQ-4 comprueba la vista previa de una factura. El resumen de pruebas promete «sin documento y con carrito intacto», que cubre emisión, pero no demuestra que rechazar una fecha al editar deje intactos el cliente, las líneas y el total de un documento existente. Esto importa porque el handler actual modifica o elimina líneas y recalcula el total en el mismo POST.  
    **Propuesta:** enviar una fecha inválida junto con cambios de cliente y una línea marcada para borrar, y comprobar el 422 y el estado persistido completo.  
    `specs/pdf-documentos-venta-oferta/requirements.md:108–130`, `app/routers/ui_sales_documents.py:342–384,415–424`.

11. **REQ-9 no queda cubierto para una instalación nueva que use solo el `.conf` base.** Requisitos §1.1, REQ-9, y diseño §§2.1 y 2.7. **Severidad media · confianza alta.** El diseño añade datos a ambos archivos y asignaciones al backfill, pero el backfill descubre códigos mediante `business_config.*.conf`. Si solo está disponible `business_config.conf`, el seed sí crea `recambios` y puede leer la configuración base como fallback, pero `ensure_business` solo guarda código y nombre. Después, el backfill no descubre ese negocio por el archivo base.  
    **Propuesta:** definir explícitamente cómo se aplica el archivo base al negocio inicial y probar un arranque limpio con cada variante de configuración.  
    `app/business_config.py:185–219`, `app/migrations.py:587–608,678–696,879–899`.

## 5. Otros bugs

12. **La cantidad impresa puede ser cero para una línea con importe positivo.** Diseño §§2.1c y 2.4.2. **Severidad media · confianza alta.** El formulario y el modelo admiten cuatro decimales. Con cantidad `0.0001` y precio `100000`, la línea vale `10.00`, pero el formato propuesto muestra cantidad `0.00`. El PDF nuevo pierde precisión que el usuario puede introducir y la BD conserva.  
    **Propuesta:** mostrar hasta cuatro decimales y retirar únicamente los ceros finales, manteniendo el separador de miles.  
    `app/templates/components/sales_doc/cart_add.html:16–17`, `app/templates/features/sales_documents/sales_document_edit_form.html:30–32`, `app/models.py:182–186`, `app/routers/ui_sales_documents.py:930–946`.

## Ficheros leídos

- Completos: `specs/pdf-documentos-venta-oferta/requirements.md`, `specs/pdf-documentos-venta-oferta/design.md` y las seis sondas de `specs/pdf-documentos-venta-oferta/design/sondas/`: `S1-fpdf-pdfplumber.txt`, `S2-sqlite-date-column.txt`, `S3-dev-db-pg.txt`, `S4-reference-colors.txt`, `S5-snapshots-afectados.txt` y `S6-json-date-y-formatos.txt`.
- Código de aplicación, completo o en los tramos pertinentes: `app/sales_document_pdf.py`, `app/models.py`, `app/migrations.py`, `app/business_config.py`, `app/business_config.conf`, `app/business_config.recambios.conf`, `app/repositories/business_repository.py`, `app/routers/ui_sales_documents.py`, `app/routers/ui_businesses.py`, `app/routers/ui_common.py`, `app/sync/client.py`, `app/sync/service.py`, `app/static/js/core/swap.js` y `app/static/js/core/modals.js`.
- Plantillas: `app/templates/components/sales_doc/{cart_add,cart_table,issue_form,recent_documents}.html`, `app/templates/features/sales_documents/{sales_document_panel,sales_document_preview,sales_document_edit_form}.html` y `app/templates/features/businesses/{business_edit_form,tab_businesses}.html`.
- Pruebas y herramientas: `tests/snapshots/ui_snapshot.py`, `tests/unit/test_ui_businesses.py`, `tests/e2e/conftest.py`, `tests/e2e/test_sale_payment_method_e2e.py`, `.gitea/workflows/deploy.yml`, `requirements.txt`, `/home/liandy/.local/bin/cc_design_check` y los módulos instalados `fastapi/exception_handlers.py` y `fpdf/fpdf.py`.

HALLAZGOS: 12