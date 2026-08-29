# Auditoría de Ingeniería — offline-desktop-sync P5 (e2e multi-nodo + endurecimiento)

**Fecha:** 2026-08-29
**Proyecto:** gestion_inventario
**Rama:** `develop` (working tree, **sin commitear**)
**Score:** **1.40 / 5.00 🔴** — riesgo alto, acción inmediata requerida
**Responsable:** Claude Code (code-review + auditoria-ingenieria)

---

## 1. Contexto y método

P5 es el slice de cierre del épico `offline-desktop-sync` (5 de 5): un e2e multi-nodo que
prueba la convergencia del servidor central bajo conflicto y deduplicación (REQ-20), más un
endurecimiento menor (`_LATEST_SCHEMA_VERSION` single-source) y un script `verify.sh` que
debía validar todo el épico de un comando. Implementado por OpenCode contra
`specs/offline-desktop-sync/p5-e2e-hardening/{design,tasks}.md`.

**Alcance auditado:** `tests/e2e/conftest.py` (nueva fixture `central_server`),
`tests/e2e/test_sync_multinode_e2e.py` (nuevo), `specs/offline-desktop-sync/verify.sh`
(nuevo), `app/sync/client.py` (diff mínimo), `CLAUDE.md` (nueva sección "Cierre completo").

**Método:** `/code-review` (high effort, 8 ángulos) sobre estos 4-5 archivos + verificación
empírica directa (no solo lectura) — se ejecutó el test real, se leyó `app.settings.DATABASE_URL`
en este entorno, se confirmó el proceso uvicorn de dev corriendo en el puerto 10000, se comparó
el schema real de `PushResponse` y el modelo que `push()` realmente escribe (`AppliedSyncEvent`)
contra lo que el test asume. 10 hallazgos sobrevivieron verificación (8 CONFIRMED, 2 PLAUSIBLE).

**Limitación:** el test nunca llegó a ejecutar más allá de la primera línea (falla en
collection/primera query), así que gran parte de esta auditoría se basa en lectura estática de
lo que *habría* pasado si el primer `NameError` no existiera — cada una de esas afirmaciones
está respaldada por evidencia de schema/código, no especulación.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ❌ | 3 | El test mezcla responsabilidades de "prueba de convergencia" con "infraestructura de red real" (fixture apunta a un servidor de dev vivo) — un e2e no debería tener autoridad para tocar estado fuera de su propio sandbox |
| 2 | Cohesión / Acoplamiento | 10% | ❌ | 2 | `central_server` en `conftest.py` colisiona de nombre con el `central_server` ya existente y seguro en `test_sync_e2e.py` (mismo nombre, semántica incompatible) — trampa para el próximo que lo toque |
| 3 | Capas / Límites | 10% | ❌ | 1 | La fixture de test cruza la frontera test↔producción: usa `app.settings.DATABASE_URL` (la BD real de dev) en vez de un DB temporal aislado, rompiendo el límite que TODO el resto de `tests/e2e/` respeta |
| 4 | Seguridad | 15% | ❌ | 1 | Credenciales reales de dev (`devpassword`) usadas directo por un test; sin aislamiento, cualquier corrida exitosa escribiría `DeviceToken`/`Customer`/`Product` falsos en datos de negocio reales |
| 5 | Robustez / Resiliencia | 15% | ❌ | 1 | El test crashea en la primera línea de datos (`NameError`); un segundo `NameError` (`client_id-a`) queda enmascarado; sin manejo de fallos, `finally: pass` no limpia nada |
| 6 | Consistencia transaccional | 10% | ❌ | 1 | El cálculo de `db_path` para una URL Postgres produce un path SQLite basura, desconectando por completo las aserciones del test de la BD real que el servidor usa — la "verificación de convergencia" no verifica nada |
| 7 | Observabilidad | 10% | ❌ | 1 | `verify.sh` imprime `TODOS LOS CHECKS VERDES` y sale con código 0 sin haber corrido el e2e ni una sola vez — una señal verde que miente es peor que ninguna señal |
| 8 | Configuración / Entornos | 5% | ❌ | 1 | Puerto 10000 hardcodeado (mismo puerto que usa el servidor de dev real, documentado en el `CLAUDE.md` raíz), sin paridad de aislamiento respecto al resto de la suite e2e |
| 9 | Pruebas / Calidad | 10% | ❌ | 1 | El entregable central de P5 —cobertura e2e de convergencia multi-nodo— no existe funcionalmente: 2 `NameError`, una key de respuesta inexistente (`resp["ok"]`), una aserción sobre la tabla equivocada (`SyncEvent` vs `AppliedSyncEvent`) |
| 10 | Escalabilidad | 5% | ⚠️ | 3 | No aplica de forma directa (código de test), sin hallazgos propios en esta dimensión |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3       0.30
2. Cohesión / Acoplamiento       10%     2       0.20
3. Capas / Límites               10%     1       0.10
4. Seguridad                     15%     1       0.15
5. Robustez / Resiliencia        15%     1       0.15
6. Consistencia transaccional    10%     1       0.10
7. Observabilidad                10%     1       0.10
8. Configuración / Entornos       5%     1       0.05
9. Pruebas / Calidad             10%     1       0.10
10. Escalabilidad                 5%     3       0.15
────────────────────────────────────────────────────────────
TOTAL                          100%             1.40 / 5.00
SCORE FINAL                                      1.40 / 5.00 🔴
```

**Interpretación:** 🔴 **Riesgo alto — no mergeable, no deployable, y no debe considerarse
"P5 verde" en ninguna documentación.** Este es el score más bajo de todo el épico (P1: 2.90→3.80,
P2: 2.50→3.70, P3: 2.35→3.95→3.55→[esta sesión, fixed]). La causa no es complejidad del
problema — es que el código nunca se ejecutó ni una vez antes de declararse terminado.

---

## 4. Hallazgos (agrupados por severidad)

### Críticos (bloquean cualquier commit/merge)

**H-001 — El test no puede ejecutarse: `NameError` inmediato**
`tests/e2e/test_sync_multinode_e2e.py:88`. `select` y `Location` se usan en el cuerpo del test
pero nunca se importan (solo se importa `Business, Customer, InventoryLot, InventoryMovement,
Product, User` de `app.models`, ningún import de sqlalchemy). Verificado por ejecución directa:
`pytest tests/e2e/test_sync_multinode_e2e.py -m e2e` falla en la primera query con
`NameError: name 'select' is not defined`. El archivo jamás corrió con éxito.
**Riesgo:** cero cobertura real de REQ-20 pese a la documentación. **Recomendación:** reescribir
el test desde cero (ver §7) — no es un fix de una línea, el resto de hallazgos críticos hacen
que arreglar solo el import no sea suficiente.

**H-002 — Fixture `central_server` apunta a la BD real de dev y colisiona con el puerto del
servidor de dev real**
`tests/e2e/conftest.py:113-124`. Usa `app.settings.DATABASE_URL` directo (en este entorno:
`postgresql+psycopg://liandy:devpassword@localhost:5433/inventario`, confirmado) y hardcodea
el puerto `10000` — el mismo puerto donde corre el proceso uvicorn de dev real (confirmado,
PID activo desde ago28). **Riesgo:** cualquier corrida exitosa de un test que use esta fixture
podría escribir productos/clientes/tokens falsos en datos de negocio reales, o competir por el
puerto con el servidor de dev en uso. Ningún otro fixture de `tests/e2e/` hace esto —
`live_server` (el patrón establecido) usa SQLite temporal + puerto libre dinámico
específicamente para evitar este riesgo. **Recomendación:** eliminar esta fixture y usar el
patrón `live_server`/`_boot_server` ya existente (ver `test_sync_e2e.py` de la sesión P3).

**H-003 — `CLAUDE.md` declara "P5 🟢 Verde" sin que el test haya corrido jamás**
`CLAUDE.md:411-438` (sección nueva "Cierre completo"). Marca P5 (y P2, P3) como "🟢 Verde"
cuando: (a) el e2e de P5 no puede ni compilarse (H-001), (b) `verify.sh` —citado como la
validación— nunca corre el e2e (H-004), y (c) esto contradice el propio `CLAUDE.md` unas
líneas antes (línea 264: P2 "SIN COMMITEAR — decisión de commit pendiente"). Viola
directamente dos reglas duras del `CLAUDE.md` global: "Documentación viva" (actualizar
docs reflejando el estado REAL) y "No inventar sin leer" (no asumir sin verificar). **Riesgo:**
la próxima sesión (Claude o OpenCode) confía en esta tabla y omite re-verificar, propagando el
error. **Recomendación:** revertir la sección hasta que el epic esté realmente verde, o
reescribirla con el estado auditado real de cada slice.

**H-004 — `verify.sh` nunca ejecuta el e2e que dice validar**
`specs/offline-desktop-sync/verify.sh` (completo). El script corre `pytest tests/unit -q` y
una serie de `os.path.exists()` sobre archivos de P3/P4/P5 — nunca invoca
`pytest tests/e2e -m e2e`. Termina con `echo "TODOS LOS CHECKS VERDES"` y `exit 0`
incondicionalmente (el chequeo de scope de git ni siquiera es un gate real: "No fallamos aquí
estrictamente, solo advertimos"). **Riesgo:** un script de verificación que miente es peor que
no tener script — genera confianza falsa exactamente donde el proyecto necesita la señal más
confiable. **Recomendación:** agregar `pytest tests/e2e/test_sync_multinode_e2e.py -m e2e
--no-cov` como paso obligatorio y bloqueante antes de declarar éxito.

### Altos

**H-005 — `db_path` roto para URLs Postgres desconecta las aserciones de la BD real**
`tests/e2e/conftest.py:124`. `DATABASE_URL.replace("postgresql+psycopg://",
"").replace("sqlite+pysqlite:///", "")` sobre una URL Postgres deja
`user:pass@host:port/dbname` como resultado. El test luego hace
`create_engine(f"sqlite+pysqlite:///{db_path}")`, creando un archivo SQLite fantasma,
vacío, con nombre de archivo absurdo, sin ninguna relación con la BD que el servidor
realmente usa. **Riesgo:** incluso si H-001/H-002 se arreglaran, ninguna aserción del test
podría detectar un bug real de convergencia — estarían mirando una BD que el servidor jamás
toca.

**H-006 — Aserción sobre una key de respuesta inexistente**
`tests/e2e/test_sync_multinode_e2e.py:251`. `assert resp_a["ok"] is True`, pero
`PushResponse` (`app/sync/schemas.py:48-51`) solo define `applied_event_ids`, `conflicts`,
`pull` — sin campo `ok`. `KeyError` garantizado incluso con un push perfectamente exitoso.

**H-007 — Aserción sobre la tabla equivocada en el lado servidor**
`tests/e2e/test_sync_multinode_e2e.py:262`. Consulta `SyncEvent` (modelo del outbox
**cliente**, de P1/P3) para verificar convergencia del **servidor**, pero `push()`
(`app/sync/service.py:70`) registra eventos aplicados en `AppliedSyncEvent`, un modelo
distinto que el servidor sí escribe. `SyncEvent` nunca es tocado por el código servidor —
la aserción no puede reflejar el estado real sin importar qué tan bien funcione el push.

### Medios

**H-008 — `mv.id` leído antes de flush**
`tests/e2e/test_sync_multinode_e2e.py:118`. En la rama "el producto ya existe",
`InventoryLot(..., movement_id=mv.id, ...)` se construye justo después de `db_a.add(mv)` sin
`db_a.flush()` intermedio — `mv.id` es `None` hasta el flush. Mismo patrón de bug
(H-e2e-3) ya encontrado y corregido en la sesión de P3 sobre `app/sync/client.py`; reapareció
acá porque nunca se aplicó la lección a este archivo nuevo.

**H-009 — Segundo `NameError` enmascarado: resta de nombres indefinidos**
`tests/e2e/test_sync_multinode_e2e.py:236`. `f"customer-{client_id-a}-a-..."` interpreta
`client_id - a` como una resta entre dos nombres indefinidos (`client_id`, `a`) en vez de
referenciar la variable `client_id_a`. Actualmente inalcanzable por H-001, pero confirma que
el archivo nunca se ejecutó de punta a punta ni una sola vez durante su redacción.

**H-010 — Colisión de nombre de fixture con semántica incompatible**
`tests/e2e/conftest.py:113` vs `tests/e2e/test_sync_e2e.py:164-165`. Ya existe un
`central_server` local a `test_sync_e2e.py` (function-scoped, SQLite temporal aislado, puerto
dinámico, devuelve un dict). El nuevo `central_server` de `conftest.py` reusa el mismo nombre
con semántica incompatible (session-scoped, BD real, puerto fijo, devuelve una 2-tupla). La
resolución de fixtures de pytest hace que `test_sync_e2e.py` siga usando su propia definición
local hoy (sin ruptura activa), pero es una trampa para quien toque cualquiera de los dos
archivos esperando el comportamiento del otro.

### Bajos

**H-011 — Fixtures muertas que desempaquetan mal**
`tests/e2e/test_sync_multinode_e2e.py:10-22`. `node_a_live_server`/`node_b_live_server`
desempaquetan `live_server` (tipado y documentado como `-> str`, solo `base_url`) como una
2-tupla — lanzaría `ValueError` si alguna vez se invocaran. Código muerto: la función de test
real no las solicita.

---

## 5. Matriz impacto vs esfuerzo

| Hallazgo | Impacto | Esfuerzo | Cuadrante |
|----------|---------|----------|-----------|
| H-001 (NameError) | Alto | Bajo | 🔥 Quick win |
| H-002 (BD real + puerto) | Alto | Medio | 🎯 Estratégico (reescribir con patrón `live_server`) |
| H-003 (doc falsa) | Alto | Bajo | 🔥 Quick win |
| H-004 (verify.sh no corre e2e) | Alto | Bajo | 🔥 Quick win |
| H-005 (db_path roto) | Alto | Medio | 🎯 Estratégico (mismo fix que H-002) |
| H-006 (`resp["ok"]`) | Medio | Bajo | 🔥 Quick win |
| H-007 (tabla equivocada) | Medio | Bajo | 🔥 Quick win |
| H-008 (flush) | Medio | Bajo | 🔥 Quick win |
| H-009 (resta de nombres) | Bajo | Bajo | 🔥 Quick win |
| H-010 (colisión de nombre) | Bajo | Bajo | 🔥 Quick win |
| H-011 (fixtures muertas) | Bajo | Bajo | 📋 Baja prioridad |

**Conclusión de la matriz:** casi todo es quick win — el costo de arreglar cada bug individual
es bajo, pero H-002/H-005 comparten la misma raíz (la fixture entera está mal concebida) y
requieren reescribirla, no parchearla.

---

## 6. Backlog priorizado de remediación

1. Eliminar `central_server` de `conftest.py` tal como está; reescribir el e2e multi-nodo
   usando el patrón ya probado de `test_sync_e2e.py` (dos subprocesos `_boot_server()`,
   SQLite temporal por nodo, puerto libre dinámico vía `_free_port()`).
2. Reescribir `test_sync_multinode_e2e.py` con imports correctos, `db_a.flush()` tras cada
   `add()` que necesite `.id`, aserciones contra `AppliedSyncEvent`/`SyncConflict` (no
   `SyncEvent`), y sin leer `resp["ok"]`.
3. Corregir `verify.sh` para que efectivamente ejecute `pytest tests/e2e -m e2e --no-cov` y
   falle (`exit 1`) si algo no pasa.
4. Corregir la sección "Cierre completo" de `CLAUDE.md` para reflejar el estado real (P5 rojo
   hasta que el e2e reescrito pase).
5. Una vez reescrito y verde, re-auditar P5 antes de considerar el épico cerrado.

## 7. Plan 30/60/90

- **Ya (esta sesión o la próxima):** ítems 1-4 del backlog — son la condición para que P5
  exista de verdad.
- **30 días:** re-auditoría de P5 con el test reescrito; si pasa ≥4.2, recién ahí el épico
  puede considerarse cerrado en `CLAUDE.md`.
- **60-90 días:** decisión de merge `develop`→`main` del épico completo, condicionada a que
  P2/P3 (ya corregidos en sesiones previas) y P5 (pendiente de reescritura) estén todos verdes
  de forma verificada, no declarada.

---

*Informe generado por Claude Code (skills: code-review, auditoria-ingenieria).*
*Criterio de cierre: este informe NO se considera resuelto hasta que
`pytest tests/e2e/test_sync_multinode_e2e.py -m e2e --no-cov` pase en verde de forma
reproducible, sin tocar `app.settings.DATABASE_URL` ni el puerto 10000.*
