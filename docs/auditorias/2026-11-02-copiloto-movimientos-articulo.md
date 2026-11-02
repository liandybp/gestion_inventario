# Auditoría de Ingeniería — Copiloto: tool `product_movements` (movimientos de un artículo)

- **Fecha:** 2026-11-02
- **Alcance:** `develop...feature/copiloto-movimientos-articulo` (base `9655c8c`): lotes A-C de `cc_local_pae`
  (14 envelopes) y la corrección de fechas del prompt (`8264085`).
- **Score:** **4.90 / 5.00 🟢**. Sin hallazgos abiertos de código.
- **Responsable:** Claude Code (Opus 5.5), revisión 5b del loop v4.
- **Specs:** `specs/copiloto-movimientos-articulo/` (requirements, design, tasks T1-T14, PAE con la nota de la revisión).

## 1. Contexto y método

| Herramienta | Uso |
|---|---|
| Serena | Firmas reales antes de aprobar el PAE: `InventoryRepository.movement_history`, `MovementHistoryRow`, `_fill_reordered_audit_usernames` (devuelve `MovementHistoryRow`, así que el acceso por nombre es válido), `ProductService.__init__`, `transfer_codes_from_out_note`. |
| BD de dev (solo lectura) | Signos por tipo, 1 salida → N entradas por lote (1.126 entradas para 1.042 salidas), 97 entradas sin `out_id`, 1/1.294 descuadres movimientos/lotes. |
| `cc_local_pae --audit` | Fidelidad de los lotes A, B y C (primer PAE con las comprobaciones nuevas): OK en los tres tras corregir un falso positivo de la herramienta (§4, H-1006). |
| Ejecución real | `pytest tests/unit` 539 · `ui_snapshot.py check` 91 · `pytest tests/e2e -m e2e` 95 (incluido el e2e nuevo) · `pyflakes` sin nombres indefinidos nuevos · eval contra el LLM real (`qwen3_8b_instruct`) en la rama y en `develop`. |

## 2. Resultado funcional

| REQ | Estado | Evidencia |
|---|---|---|
| REQ-1/2/3 movimientos, saldo, traspaso en una fila | ✅ | `test_reporting_product_movements.py` (filas, etiquetas y saldos concretos) y e2e sembrado: tabla con "Saldo inicial", fila "… → …" y saldo final 9.0 en el navegador. |
| REQ-4 filtro por ubicación | ✅ | Tests de la vista filtrada (salida "→ destino", entrada "← origen"). |
| REQ-5 SKU por código o nombre | ✅ | `ProductService.resolve_sku`; SKU inexistente → 404 (`test_agent_tools.py`). |
| REQ-6 período por defecto y límite 500 | ✅ | Test de los 3 meses por defecto y del límite con la constante parcheada. |
| REQ-7 nota de descuadre | ✅ | Test con el lote modificado; nota escapada en `widget_table.html` (test con `<script>`). |
| REQ-8 elección de la tool | ✅ | Eval: los 3 casos nuevos eligen `product_movements` con `sku` (y `location`). |
| REQ-9 docs | ✅ | 35 tools en los dos `CLAUDE.md`; CHANGELOG 0.5.0. |

**Eval de precisión** (28 casos):

| Variante | Tool | Type | Args |
|---|---|---|---|
| `develop` (sin la tool, 25 casos) | 96 % | 96 % | 76 % |
| Rama, antes de corregir fechas | 96 % | 100 % | 79 % |
| **Rama, con la corrección de fechas** | **96 %** | **100 %** | **100 %** |

El único fallo de tool (`low_stock`: "¿Qué productos necesito reponer ya?" → `replenishment_table`) está también en
`develop` y es una ambigüedad del caso: la regla del prompt admite las dos tools para "qué reponer".

## 3. Scorecard

| # | Dimensión | Peso | Score | Evidencia |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 5 | La tool resuelve argumentos; la lógica vive en `ReportingService.product_movements`. |
| 2 | Cohesión | 10 | 5 | Reutiliza `movement_history`, `transfer_codes_from_out_note`, `resolve_sku`, `_resolve_location_id`. |
| 3 | Capas | 10 | 5 | Import local para evitar el ciclo `reporting` ↔ `inventory`; sin acceso a privados (corregido en la revisión del PAE). |
| 4 | Seguridad | 15 | 5 | Solo lectura, todo por `business_id`; nota con autoescape y test hostil. |
| 5 | Robustez | 15 | 5 | Una nota ilegible no rompe la tabla (`HTTPException` capturada, etiqueta de reserva); los errores de argumentos van al aviso de fallo de la tool. |
| 6 | Ventanas | 10 | 5 | Saldo inicial `< start` y filas `>= start, < end`, con test de frontera; comparación con lotes solo si la ventana llega a hoy. |
| 7 | Observabilidad | 10 | 5 | Sin cambios; `_execute_node` registra los fallos. |
| 8 | Configuración | 5 | 5 | `PRODUCT_MOVEMENTS_MAX_ROWS`. |
| 9 | Pruebas | 10 | 4 | Unit con valores concretos, e2e real, 3 casos de eval. El eval no comprueba las fechas que genera el modelo (así pasó desapercibido H-1004). |
| 10 | Escalabilidad | 5 | 5 | ≤ 500 filas; máximo real 64 por artículo. |
| | **Total** | 100 | **490 → 4.90 🟢** | |

## 4. Hallazgos (todos cerrados)

| ID | Sev. | Origen | Hallazgo | Corrección |
|---|---|---|---|---|
| H-1001 | Medio | PAE | `--lot0-test-cmd` con `--no-cov` sin `pytest-cov` instalado: el Lote 0 paró con `exit 4` sin que fallara nada. | Opción quitada del PAE (`2311d1a`); lote 0 verificado a mano. |
| H-1002 | Medio | PAE | T3 capturaba `Exception` al leer la nota de salida y dejaba la ubicación en " → " si la nota no se leía. | Corregido antes del lanzamiento: `HTTPException` y etiqueta de reserva. |
| H-1003 | Bajo | PAE | T4 usaba `ProductService._products` (atributo privado). | Corregido antes del lanzamiento: `ProductRepository.get_by_sku`. |
| H-1004 | Alto | previo | A principios de mes el copiloto resolvía "este mes" como el mes anterior (todas las tools con fechas; 6/28 casos en el eval, igual en `develop`). | `8264085`: el prompt lleva los rangos de "este mes" y "mes pasado" ya calculados; 2 tests (inicio de mes y cambio de año). Eval args 79 % → 100 %. |
| H-1005 | Bajo | loop | `cc_local` no tiene permiso para editar `.claude/` (5 denegaciones en el lote C; también en la fase 5a). | Anotado: los envelopes que tocan `.claude/CLAUDE.md` los aplica Claude Code o se acepta la denegación. |
| H-1006 | Bajo | herramienta | Falso positivo de la comprobación de fidelidad nueva con varias parejas BEFORE/AFTER en un bloque y etiquetas con prefijo. | `cc_local_pae` analiza los bloques línea a línea; recalibrado con 16 lotes sin falsos positivos en los fieles. |

## 5. Lecciones

- La comprobación de fidelidad de `cc_local_pae` funcionó en su primer PAE (detectó lo que creyó un envelope sin
  aplicar) y su único falso positivo se corrigió en la herramienta, no desactivándola.
- El eval de selección de tools mide tool, tipo y args fijos, pero no las fechas relativas: un caso por mes
  "este mes"/"mes pasado" con fechas esperadas calculadas evitaría que H-1004 vuelva a pasar desapercibido.
  Implementado vía los tests de `build_system_prompt` (el rango que recibe el modelo); el eval real lo confirma.
