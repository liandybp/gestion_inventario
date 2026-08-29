# Auditoría de Ingeniería — Épico completo `offline-desktop-sync` (P1-P5)

**Fecha:** 2026-08-29
**Proyecto:** gestion_inventario
**Rama:** `develop` — P1/P2/P3 commiteados, P4/P5 en working tree sin commitear
**Score:** **2.15 / 5.00 🔴** — riesgo alto, épico NO listo para cerrar
**Responsable:** Claude Code (code-review + auditoria-ingenieria + verificación empírica)

---

## 1. Contexto y método

Auditoría integral del épico completo (5 slices) pedida explícitamente por el usuario tras el
cierre de P5. A diferencia de las auditorías previas (una por slice), esta evalúa el épico
**como sistema integrado**: qué tan seguro es asumir que "offline-desktop-sync" funciona de
punta a punta, no solo que cada pieza individual pasa sus propios tests.

**Alcance:** los 5 slices completos — `app/sync/*`, `app/routers/sync.py`,
`app/routers/ui_sync.py`, `app/desktop/*`, `packaging/*`, `requirements-desktop.txt`,
`app/db.py` (wiring SQLCipher), toda la suite de tests relacionada (unit + e2e).

**Estado de auditorías previas reutilizado (sin drift, verificado por `git diff` vacío en sus
archivos):**
- P1 (Fundamentos): 3.80/5.00 🟡, commiteado (`2dbad8a`)
- P2 (Backend de sync): 3.70/5.00 🟡, commiteado (`e4e03a2`)
- P3 (Cliente de sync): cerrado esta sesión con 3 bugs reales adicionales encontrados y
  corregidos vía e2e (score previo 3.55 🟡, no re-escoreado tras el cierre pero funcional),
  commiteado (`41adf1d`, `13f7af1`, `88edf08`)

**Evidencia nueva de esta auditoría (P4 y P5, nunca auditados con este rigor hasta ahora,
verificados por ejecución/lectura directa de API real, no solo inspección superficial):**
- P5 fue reescrito por completo esta sesión (de 1.40/5.00 🔴 a funcional, ver
  `docs/auditorias/2026-08-29-p5-multinode-e2e-offline-sync-closeout.md`) — pendiente de
  re-score formal, tratado aquí como 🟡 funcional.
- **P4 nunca había sido auditado ni ejecutado.** Esta auditoría es la primera vez que alguien
  revisa su contenido con atención. El resultado es peor que el que tenía P5 antes de su
  reescritura.

---

## 2. Hallazgo central: P4 nunca se ejecutó, en ningún nivel

El patrón que definió la auditoría de P5 (código que parece razonable pero nunca corrió una
sola vez) se repite en P4 de forma aún más severa, porque P4 incluye artefactos
(`packaging/desktop.spec`, `packaging/installer.iss`) que **no pueden validarse en este
entorno de desarrollo** (Linux, sin PyInstaller ni Inno Setup instalados) — lo cual significa
que nadie los ejecutó nunca, ni en esta sesión ni en ninguna anterior, y nada en el pipeline
de CI/CD del proyecto los ejecutaría tampoco.

### H-P4-001 — `get_db_key()`: la función central de cifrado SQLCipher crashea garantizado
`app/desktop/keyring.py:140`. `salt = db_path_obj.resolve().as_bytes()` — `pathlib.Path` **no
tiene** un método `.as_bytes()` (verificado: `AttributeError: 'PosixPath' object has no
attribute 'as_bytes'`). Esta rama de código nunca se disparó en el único test que la ejercita
(`test_fallback_deterministic_from_db_path`) porque ese test tiene un bug de aislamiento
separado (ver H-P4-002) que hace que la condición previa (`db_path_obj.exists()`) sea siempre
`False` bajo la invocación estándar de la suite (`DATABASE_URL=sqlite+pysqlite:///:memory:`).
**Riesgo:** la funcionalidad central que justifica todo P4 (cifrado local de la BD del nodo
desktop vía SQLCipher) jamás ha generado una clave real — ni en un test, ni manualmente. Es
imposible que este código haya sido probado en un escenario real.

### H-P4-002 — El test que debería cubrir esto no es hermético
`tests/unit/test_desktop_keyring.py:58-66`. El test crea `/tmp/test_desktop_keyring.db` pero
nunca hace `monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")` — `get_db_key()` lee
`os.getenv("DATABASE_URL")` del entorno AMBIENTE del proceso, no de lo que el test acaba de
crear. Bajo la invocación estándar del proyecto (`DATABASE_URL=sqlite+pysqlite:///:memory:`),
el path nunca existe, la rama del salt determinista nunca se alcanza, y el test falla con
`assert None is not None` — sin siquiera llegar a exponer H-P4-001.

### H-P4-003 — `launcher.py`: el punto de entrada de toda la app desktop crashea al primer log
`app/desktop/launcher.py:53`. `_log = __import__("app.logger").get_logger(__name__)` —
`__import__("app.logger")` devuelve el paquete **`app`** (no `app.logger`, ese es el
comportamiento documentado de `__import__` sin `fromlist`), y `app` no tiene un atributo
`get_logger`. Verificado directamente: `hasattr(__import__("app.logger"), "get_logger")` →
`False`. **Riesgo:** la primera vez que `main()` intenta loguear (justo después de arrancar
uvicorn, antes de abrir la ventana pywebview) lanza `AttributeError` y la app desktop nunca
llega a mostrar una ventana. El launcher, el artefacto que literalmente define "app de
escritorio" en este épico, nunca ha corrido exitosamente ni una vez.

### H-P4-004 — `packaging/desktop.spec`: API de PyInstaller inventada, no compilaría
El archivo completo usa nombres de parámetros y atributos que no existen en la API real de
PyInstaller: `Analysis(..., pathext=[], options={"pyexe": {...}}, treedir=None)` (PyInstaller
usa `pathex`, `hiddenimports`, `hookspath`, `excludes` como kwargs directos — no existe un
dict `options={"pyexe": ...}`); `PYZ(a.purecludedata, a.zippedpackages)` (los atributos reales
de un objeto `Analysis` son `a.pure` y `a.zipped_data`); una segunda llamada a
`Analysis(a.graph, a.analyzer, intercepts=[], kas_ignore=None, switchmenu=None,
atable_magic=True, olimp=False)` que no corresponde a ninguna firma real de PyInstaller (esos
nombres de parámetro no existen en ningún lugar de su documentación); `EXE(b, ...,
coll='Windows', ..., atrf=False, one_file=False)` con parámetros igualmente inventados y el
objeto equivocado (`b`, la segunda `Analysis()` fabricada) pasado donde debería ir `p` (el
`PYZ` real). **Riesgo:** `pyinstaller --spec-file packaging/desktop.spec` fallaría con
`TypeError`/`AttributeError` en la primera línea que PyInstaller intente evaluar. No hay forma
de que este archivo haya sido ejecutado contra un PyInstaller real (no está instalado en este
entorno, y nada en el proyecto lo instala o lo prueba en CI).

### H-P4-005 — `packaging/installer.iss`: sintaxis de Inno Setup igualmente inválida
Mezcla convenciones de secciones incompatibles: `[Setup]` usa correctamente `Clave=Valor`,
pero le siguen `OutputDir={app}` y `SourceDir={app}\dist` — `{app}` es una constante que Inno
Setup resuelve en **tiempo de instalación** (la carpeta que elige el usuario), no en tiempo de
compilación del instalador, así que usarla para decirle al compilador dónde están los
archivos fuente es un contrasentido estructural. `Filesource={app}\dist\*;{app}` no es una
directiva válida de `[Setup]` en absoluto (el punto y coma además inicia un comentario en
Inno Setup, así que la mitad de la línea se ignoraría). La sección `[Shortcut]` no existe en
Inno Setup — la sección real es `[Icons]`, con directivas `Name:`/`Filename:`/`WorkingDir:`
(dos puntos, no `=`) — mientras que este archivo usa `Cmd=`/`WorkingDir=` con `=`, la sintaxis
de `[Setup]`, en la sección equivocada. `[Files]` usa `{cm}` como si fuera una constante de
directorio (no existe — `{cm:...}` en Inno Setup es una función de mensajes personalizados,
sintaxis completamente distinta). `[Run]` usa `File:` en vez de la directiva real `Filename:`.
**Riesgo:** este `.iss` no compilaría en Inno Setup real; nadie lo ha ejecutado nunca.

---

## 3. Checklist por dimensión (épico completo)

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ✅ | 4 | `app/sync/*` bien separado por responsabilidad (client/service/outbox/auth/schemas/tables); `app/desktop/*` también razonablemente separado en su forma, aunque su contenido esté roto |
| 2 | Cohesión / Acoplamiento | 10% | ❌ | 2 | `app/sync/tables.py` como fuente única (bien, agregado esta sesión); pero `app/desktop/keyring.py` importa `logging` de forma inline 7 veces con nombres numerados (`_log_module`...`_log_module7`) en vez de un logger de módulo — duplicación severa, inconsistente con el resto del proyecto |
| 3 | Capas / Límites | 10% | ⚠️ | 3 | `app/sync/*` respeta capas; `launcher.py` es razonable como punto de entrada de un binario standalone; `packaging/*` no es código en ejecución sino config de build, pero está tan roto que no cumple su función de límite de despliegue en absoluto |
| 4 | Seguridad | 15% | ❌ | 1 | **La función de derivación de clave SQLCipher crashea garantizado (H-P4-001)** — la funcionalidad central de seguridad de todo P4 (cifrado local del nodo desktop) nunca ha generado una clave real. `app/db.py` interpola la clave en un `PRAGMA key = '...'` vía f-string en vez de un parámetro bindeado (riesgo bajo dado que la clave no es input de usuario, pero es un patrón peligroso) |
| 5 | Robustez / Resiliencia | 15% | ❌ | 1 | Tercera instancia del mismo patrón en el épico (tras P3 y P5): código que crashea en su primer uso real. `launcher.py` usa `time.sleep(1)` fijo para esperar a uvicorn en vez de un poll de `/health` (mismo patrón ya endurecido en los fixtures de test) |
| 6 | Consistencia transaccional | 10% | ✅ | 3 | P1-P3 (outbox, apply_pull, hash chain) ya endurecidos esta sesión; el wiring de la clave SQLCipher en `app/db.py`'s `PRAGMA key` sigue el mismo patrón del listener de FK, estructuralmente razonable una vez que `get_db_key()` esté arreglado |
| 7 | Observabilidad | 10% | ⚠️ | 3 | Logging consistente en `app/sync/*` (con `exc_info=True` donde corresponde); `verify.sh` corregido esta sesión para ser un gate honesto; `app/desktop/keyring.py` es un anti-patrón de logging (7 loggers ad-hoc en vez de uno de módulo) |
| 8 | Configuración / Entornos | 5% | ❌ | 2 | `get_db_key()` lee `os.getenv("DATABASE_URL")` directo en vez de `app.settings.DATABASE_URL` — bypassa la configuración centralizada que el propio `CLAUDE.md` del proyecto exige (H-008 histórico); mismo patrón de riesgo que causó el fiasco de P5 (fixture apuntando a config real en vez de aislada) |
| 9 | Pruebas / Calidad | 10% | ❌ | 1 | P1-P3 bien cubiertos (277 unit + 22 e2e verdes); P4 tiene 5 tests para `keyring.py` (1 roto) y **cero** cobertura para `launcher.py` o los artefactos de `packaging/` — no hay forma de que hayan sido validados nunca, ni por test automatizado ni por ejecución manual, porque las herramientas que los ejecutarían (PyInstaller, Inno Setup) no están disponibles en este entorno y nada en el proyecto las invoca en CI |
| 10 | Escalabilidad | 5% | ⚠️ | 3 | No aplica de forma directa a un empaquetado de escritorio; sin hallazgos propios más allá de lo ya conocido en `apply_pull` (full-replace, trade-off v1 aceptado) |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 4. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     4       0.40
2. Cohesión / Acoplamiento       10%     2       0.20
3. Capas / Límites               10%     3       0.30
4. Seguridad                     15%     1       0.15
5. Robustez / Resiliencia        15%     1       0.15
6. Consistencia transaccional    10%     3       0.30
7. Observabilidad                10%     3       0.30
8. Configuración / Entornos       5%     2       0.10
9. Pruebas / Calidad             10%     1       0.10
10. Escalabilidad                 5%     3       0.15
────────────────────────────────────────────────────────────
TOTAL                          100%             2.15 / 5.00
SCORE FINAL                                      2.15 / 5.00 🔴
```

**Interpretación:** 🔴 **Riesgo alto — el épico no está listo para cerrarse ni para mergear
`develop`→`main`.** El trabajo de esta sesión (P3 cerrado con 3 bugs reales corregidos, P5
reescrito de 1.40 a funcional) mejoró sustancialmente dos de los cinco slices, pero **P4 —el
que hace que esto sea una "app de escritorio" y no solo un backend de sync— nunca había sido
auditado ni ejecutado, y todo lo que se revisó en él está roto**: la función de cifrado
crashea garantizado, el launcher crashea garantizado, y los dos artefactos de empaquetado
(`.spec`, `.iss`) no compilarían si alguien los invocara con las herramientas reales.

---

## 5. Comparación con las auditorías individuales previas

| Slice | Última auditoría individual | Esta auditoría (integrada) |
|-------|------------------------------|------------------------------|
| P1 | 3.80/5.00 🟡 | Sin cambios, verificado sin drift |
| P2 | 3.70/5.00 🟡 | Sin cambios, verificado sin drift |
| P3 | 3.55/5.00 🟡 (pre-cierre) | Cerrado esta sesión, funcional, pendiente re-score formal |
| P4 | **Nunca auditado** | **1.0-2.0/5.00 🔴 en la mayoría de dimensiones tocadas** — peor que P5 antes de su reescritura |
| P5 | 1.40/5.00 🔴 (pre-reescritura) | Reescrito esta sesión, funcional, pendiente re-score formal |

El promedio simple de los scores individuales conocidos (P1 3.80, P2 3.70, P3 ~3.55, P5 1.40,
P4 sin score previo) ya sugería un épico débil; esta auditoría integrada, al fin darle a P4
el escrutinio que nunca tuvo, confirma que el problema es más profundo de lo que la suma de
auditorías parciales dejaba ver.

---

## 6. Matriz impacto vs esfuerzo

| Hallazgo | Impacto | Esfuerzo | Cuadrante |
|----------|---------|----------|-----------|
| H-P4-001 (`.as_bytes()`) | Crítico | Bajo | 🔥 Quick win |
| H-P4-002 (test no hermético) | Alto | Bajo | 🔥 Quick win |
| H-P4-003 (`__import__` launcher) | Crítico | Bajo | 🔥 Quick win |
| H-P4-004 (`desktop.spec` fabricado) | Crítico | Alto | 🎯 Estratégico — reescribir contra la API real de PyInstaller, y **validarlo en una máquina Windows real** (no hay forma de verificarlo en este entorno Linux) |
| H-P4-005 (`installer.iss` fabricado) | Crítico | Alto | 🎯 Estratégico — mismo problema, requiere Inno Setup real para validar |
| Logging ad-hoc en `keyring.py` | Bajo | Bajo | 📋 Baja prioridad |
| `os.getenv` en vez de `app.settings` | Medio | Bajo | 🔥 Quick win |
| `time.sleep(1)` en `launcher.py` | Bajo | Bajo | 📋 Baja prioridad |

**Nota importante sobre H-P4-004/H-P4-005:** a diferencia de todos los demás hallazgos de este
épico, estos dos **no se pueden verificar por ejecución en este entorno** (Linux, sin
PyInstaller ni Inno Setup). Se puede corregir la sintaxis contra la documentación real de
ambas herramientas, pero la única verificación genuina requiere una build real en Windows —
algo que el propio diseño de P4 ya reconocía como manual/fuera de CI. Cualquier corrección acá
debe tratarse como "mejor esfuerzo, sin garantía", y señalarse explícitamente como pendiente de
validación manual en Windows antes de confiar en que el instalador funciona.

---

## 7. Backlog priorizado de remediación

1. **H-P4-002 + H-P4-001** (mismo test): arreglar el test para que sea hermético
   (`monkeypatch.setenv("DATABASE_URL", ...)`), lo cual expone H-P4-001 inmediatamente; arreglar
   `.as_bytes()` (usar `str(db_path_obj.resolve()).encode("utf-8")` o similar).
2. **H-P4-003**: arreglar el import roto de `launcher.py` (`from app.logger import
   get_logger`, patrón estándar del resto del proyecto).
3. **Config**: `get_db_key()` debe leer `app.settings.DATABASE_URL`, no `os.getenv` directo.
4. **H-P4-004/H-P4-005**: reescribir contra la API real de PyInstaller/Inno Setup — con la
   salvedad explícita de que la validación final requiere una build manual en Windows.
5. Una vez corregido, re-auditar P4 formalmente y P5 (pendiente desde su reescritura), y
   recién ahí decidir si el épico completo está listo para `develop`→`main`.

## 8. Plan 30/60/90

- **Ya:** ítems 1-3 del backlog (verificables en este entorno, sin dependencias externas).
- **30 días:** ítem 4 (packaging), con validación manual en una máquina Windows real antes de
  darlo por cerrado — no se puede firmar como "verde" solo por lectura de código.
- **60-90 días:** re-auditoría formal de P4 y P5, decisión de merge `develop`→`main` del épico
  completo condicionada a que los 5 slices superen individualmente 🟡 sólido (o a que el
  usuario acepte explícitamente el estado actual como suficiente para su caso de uso).

---

*Informe generado por Claude Code (skills: code-review, auditoria-ingenieria).*
*Este informe NO reemplaza los informes individuales de cada slice — los complementa con la
vista integrada que ninguno de ellos, por separado, podía dar.*
