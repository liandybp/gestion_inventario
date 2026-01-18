# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## Unreleased

## [0.2.1] - 2024-06-XX
### Cambios
- Se corrigieron errores menores en la gestión de inventario.
- Se mejoró la exportación de datos a Excel, ahora incluye nuevas columnas y formato ajustado.
- Se optimizó el rendimiento de la carga de archivos grandes.
- Se actualizó la documentación de uso en el README.
- Se agregaron pruebas unitarias para las funciones principales.
- Se refactorizó el código para mejorar la legibilidad y el mantenimiento.
- Se solucionó un problema con la validación de datos de entrada.
- Se mejoró la interfaz de usuario para una experiencia más intuitiva.

## 0.1.8 (2026-01-07)

- feat: inventario multi-ubicación (CENTRAL + POS) con selectores en Inventario/Ventas/Documentos
- feat: transferencias CENTRAL→POS multi-SKU (atómicas, preservando FIFO/costos)
- feat(ui): envíos: edición y eliminación de envíos desde el historial (Edit/Del)
- feat: envíos: vincular entradas a la salida mediante out_id para edición/borrado consistentes
- feat: devoluciones a proveedor **por lote** (selección de lote, preserva costo) + filtros en Historial

- fix(ui): ventas: datalist/selector de artículo filtrado por stock disponible en el POS seleccionado
- fix: ventas: mensaje de stock insuficiente más explícito (incluye ubicación y disponible vs solicitado)

- fix(ui): transfers/envíos: refresco inmediato del tab tras crear envío y limpieza del formulario en éxito
- fix(ui): transfers/envíos: historial ordenado y tablas más compactas (mejor UX en móvil)
- fix(ui): transfers/envíos: prevención de doble submit (deshabilita botón durante la request)
- fix: transfers/envíos: mensajes más claros cuando no hay stock en el almacén CENTRAL (incluye disponible vs solicitado)
- fix(ui): inventario: preservar ubicación seleccionada al editar producto
- fix(ui): inventario: deshabilitar eliminar producto si tiene movimientos/lotes (con tooltip explicativo)

- d957087 (2026-01-05) feat: update inventory handling to set received date for initial inventory adjustments and improve checkbox styling
- 71f270c (2026-01-05) feat: enhance stock list with minimum purchase cost and default sale price
- 0a1d240 (2026-01-05) feat: add initial inventory checkbox and adjust stock handling logic in product edit form

## 0.1.6 (2026-01-05)

- ad9b0e5 (2026-01-05) chore(release): bump version 0.1.5 → 0.1.6
- 156efa7 (2026-01-05) feat: add desired stock input and inventory adjustment logic in product edit form

## 0.1.5 (2026-01-05)

- f18cbd6 (2026-01-05) chore(release): bump version 0.1.4 → 0.1.5
- 57719b8 (2026-01-05) feat: add home charts feature with sales metrics and filtering options
- fbb9b63 (2026-01-05) feat: implement modal dialogs for editing customer, expense, product, and purchase details
- c62e0a0 (2026-01-04) fix: update SKU and name queries to use case-insensitive matching

## 0.1.4 (2026-01-04)

- 5c5c38e (2026-01-04) chore(release): bump version 0.1.3 → 0.1.4
- d67f3bb (2026-01-04) feat: add product import functionality from PDF invoice
- 3979403 (2026-01-04) fix: update asset versioning for app.css and app.js

## 0.1.3 (2026-01-04)

- e65d36a (2026-01-04) chore(release): bump version 0.1.2 → 0.1.3
- fffc647 (2026-01-04) fix: lint
- 3ba7671 (2026-01-04) fix: improve HTML structure and formatting for extraction table
- 0fd2cc0 (2026-01-04) fix: improve HTML structure and formatting for extraction table
- 35b339a (2026-01-04) chore(release): bump version 0.1.1 → 0.1.2
- 07fd93f (2026-01-04) feat: implement mobile menu and sidebar navigation, add month/year filters for purchases and sales

## 0.1.2 (2026-01-04)

- e0b90c5 (2026-01-04) chore(release): bump version 0.1.1 → 0.1.2
- a412f6a (2026-01-04) refactor: update HTML structure and styling for consistency across forms and components

## 0.1.1 (2026-01-04)

- 06cf618 (2025-12-14) fead: implement initial FastAPI application with inventory and product management features
- 2d27bb0 (2025-12-14) refactor: enhance inventory management with lot tracking and allocation features
- 59b7333 (2025-12-14) feat: add UI for product management and stock overview
- 616c084 (2025-12-14) feat: enhance dashboard with product, purchase, and sale panels
- a341bb2 (2025-12-14) feat: add unit of measure and default cost/price fields to product management
- 074778f (2025-12-14) feat: add expense tracking and profit reporting features with UI updates
- cc3064b (2025-12-14) fix: implement expense editing functionality with UI updates
- 681b883 (2025-12-14) fix: enhance monthly overview with JSON data for chart rendering and debug information
- 39820ed (2025-12-14) feat: add dividends tracking and extraction management features with UI updates
- 81028bd (2025-12-15) feat: implement delete functionality for extractions, expenses, purchases, and sales with UI updates
- 226928b (2025-12-15) feat: add image upload functionality for products and enhance product forms with image previews
- d415ec5 (2025-12-15) feat: add label printing functionality for purchases with UI enhancements
- 223fc21 (2025-12-16) feat: implement user authentication and logging with role management and session handling
- 9e4da79 (2025-12-16) feat: add user authentication and session management with UI updates
- dfb6d94 (2025-12-16) feat: add movement history tab with filtering options for inventory tracking
- 45bb759 (2025-12-16) feat: refactor inventory and product services for improved database access and add session secret utility
- bb648fb (2025-12-16) feat: configure PostgreSQL support and enhance database initialization logic
- 6be0001 (2025-12-27) feat: enhance user role management and improve inventory movement logging
- 428ce4a (2025-12-27) feat: enhance invoice processing
- 4a6ca9f (2025-12-27) feat: add sales and inventory value calculations, enhance dashboard with yearly sales data and charts
- bd3c642 (2025-12-27) feat: add activity tab for user session and event logging, enhance dashboard layout and styling
- ab1978e (2025-12-27) feat: implement business configuration management and sales document handling
- 4b48304 (2025-12-27) feat: add customer management features including CRUD operations and UI updates
- ef20b3c (2025-12-31) feat: add README files
- f405d72 (2026-01-01) Adding new docker-compose for homelab deployment
- 551fc67 (2026-01-01) Adding new docker-compose for homelab deployment
- 1558597 (2026-01-01) Changed docekrfile
- 54ef3da (2026-01-01) Changed dockerfile
- 95b6cad (2026-01-01) Changed dockerfile and main
- 65c64e1 (2026-01-04) feat: add opening_pending configuration and update inventory service to handle pending amounts
- 0ab5a7d (2026-01-04) chore: release tooling (gitignore, docs, bump2version)
- 3f5ceb3 (2026-01-04) chore(release): bump version 0.1.0 → 0.1.1
