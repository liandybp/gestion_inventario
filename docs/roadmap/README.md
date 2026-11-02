# Roadmap — Gestión de Inventario

Documentos de análisis y planificación de evoluciones futuras. **Son insumos**
para generar los PAE (vía `SDD-Agent`); no implementan código por sí mismos.

## Índice

| Documento | Fecha | Contenido |
|-----------|-------|-----------|
| [2026-08-17-oportunidades-evolucion.md](2026-08-17-oportunidades-evolucion.md) | 2026-08-17 | Catálogo de 7 oportunidades de valor (cobros, caducidad/FEFO, proveedores, notificaciones, barcode, export CSV, conteo físico) con matriz de priorización y fichas de diseño. |

## Prioridad actual (resumen)

- **P0** — #1 Cobros / cuentas por cobrar · #2 Caducidad de lotes + FEFO
- **P1** — #3 Proveedores + cuentas por pagar · #4 Notificaciones proactivas
- **P2** — #5 Escaneo barcode (EAN) · #6 Export CSV/Excel
- **P3** — #7 Conteo físico / arqueo

Ver el documento para el detalle, huecos verificados en el código y criterios de aceptación.

## Relación con otras carpetas de `docs/`

- `docs/plans/` — designs de features aprobadas (output de brainstorming / entrada a SDD-Agent).
- `docs/auditorias/` — auditorías técnicas con scorecard.
- `docs/code-reviews/` — revisiones tácticas previas a commit.
- `specs/` — PAE ya generados (requirements/design/tasks) para OpenCode.
