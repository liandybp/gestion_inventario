# Re-auditoría de Ingeniería — Épico completo `offline-desktop-sync` (P1-P5)

**Fecha:** 2026-08-29
**Proyecto:** gestion_inventario
**Rama:** `develop` — P1/P2/P3 commiteados, P4/P5 en working tree sin commitear
**Score:** **4.05 / 5.00 🟢** (antes: 2.15/5.00 🔴, ver `2026-08-29-offline-desktop-sync-epic-full-audit.md`)
**Responsable:** Claude Code (code-review + auditoria-ingenieria + verificación empírica)

---

## 1. Contexto

Tras la auditoría integral que encontró el épico en 2.15/5.00 🔴 (P4 nunca auditado ni
ejecutado, con 5 bugs garantizados de crash en sus artefactos principales), se corrigieron
todos los hallazgos en esta misma sesión, se pasó una segunda ronda de `/code-review` sobre
los fixes (7 hallazgos adicionales, todos corregidos), y se re-audita el épico completo con
evidencia fresca. Cada fix se verificó por ejecución directa, no solo por lectura — incluyendo
una conexión SQLite real en modo desktop de punta a punta con `LOG_LEVEL=DEBUG` confirmando
que la clave se deriva y el `PRAGMA key` se establece correctamente.

---

## 2. Qué se corrigió (todos los hallazgos de la auditoría anterior + 7 del code-review de los fixes)

### De la auditoría original (H-P4-001 a H-P4-005)

| ID | Hallazgo | Fix | Verificado por |
|----|----------|-----|-----------------|
| H-P4-001 | `get_db_key()` crasheaba (`Path.as_bytes()` no existe) | Reescrito: `str(db_path.resolve()).encode("utf-8")` | Conexión real end-to-end en modo desktop, `LOG_LEVEL=DEBUG` confirma clave derivada |
| H-P4-002 | Test no hermético (nunca seteaba `DATABASE_URL`) | `monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")` explícito | `pytest tests/unit/test_desktop_keyring.py` verde |
| H-P4-003 | `launcher.py` crasheaba (`__import__("app.logger").get_logger` no existe) | `from app.logger import get_logger` estándar | Import + logging real probados (`tests/unit/test_desktop_launcher.py`, nuevo) |
| H-P4-004 | `desktop.spec` con API de PyInstaller inventada | Reescrito contra API real (Analysis/PYZ/EXE/COLLECT), entrypoint corregido a `launcher.py` (antes apuntaba a `main.py`, sin ventana) | Sintaxis Python válida (`ast.parse`); **build real en Windows sigue pendiente**, no verificable en este entorno |
| H-P4-005 | `installer.iss` con sintaxis Inno Setup inválida | Reescrito contra sintaxis real (`[Icons]` en vez de `[Shortcut]`, `Filename:`/`WorkingDir:` con dos puntos, `OutputDir=.` en vez de `{app}`) | Revisado contra convenciones documentadas de Inno Setup; **compilación real en Windows sigue pendiente** |

### Del code-review de los fixes (segunda ronda)

| # | Hallazgo | Fix | Verificado por |
|---|----------|-----|-----------------|
| 1 | Fuga de estado entre tests (`APP_MODE` nunca revertía tras `importlib.reload`) | Fixture `autouse` que recarga `app.settings` después de cada test | Reproducido el leak, confirmado que desaparece (`is_desktop()` correctamente `False` tras un test aislado) |
| 2 | `launcher.py`: hilo de uvicorn muere en silencio (puerto ocupado), timeout completo sin pista real | `run_uvicorn` devuelve el hilo + lista de errores; `_wait_ready` chequea `thread.is_alive()` | Script standalone: falla en 0.00s con el motivo real, en vez de 15s genéricos |
| 3 | `_wait_ready` descarta el cuerpo de un `HTTPError` (servidor arriba pero degradado) | Captura `urllib.error.HTTPError` y conserva el detalle en el mensaje final | Script standalone contra el servidor real: detalle HTTP capturado correctamente |
| 4 | `PRAGMA key` corría DESPUÉS de `PRAGMA foreign_keys=ON` (SQLCipher exige que sea la primera operación) | Reordenado | Conexión real confirma `foreign_keys` sigue en `1` con el nuevo orden |
| 5 | `SQLCIPHER_KEY` del entorno no se normalizaba a 32 bytes (passphrases de longitud arbitraria) | `hashlib.sha256(env_key...)` igual que el fallback determinista | Test actualizado y verde |
| 6 | `unprotect_key` caía de un DPAPI fallido a `base64.b64decode` sobre el MISMO blob cifrado, pudiendo "recuperar" una clave incorrecta en silencio | Prefijo de 1 byte que marca la codificación (`_MARK_DPAPI`/`_MARK_BASE64`); dispatch explícito, sin adivinar | Round-trip real verificado con el nuevo formato |
| 7 | `pywebview` selecciona su backend de Windows por import dinámico — PyInstaller puede no detectarlo | `hiddenimports` explícitos para los backends conocidos | No verificable sin build real en Windows — documentado como tal |

**Nuevo:** `tests/unit/test_desktop_launcher.py` (4 tests, `pywebview` inyectado en `sys.modules`
para poder importar y probar `launcher.py` sin instalar la dependencia Windows-only) —
`launcher.py` nunca había tenido cobertura de test alguna.

---

## 3. Scorecard ponderado (evidencia fresca, no arrastrada de auditorías previas)

```
Dimensión                        Peso    Score   Peso×Score   Evidencia clave
──────────────────────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     4       0.40         Separación limpia en todo el épico; launcher.main() concentra 3 responsabilidades (arrancar/esperar/mostrar) razonablemente para un entrypoint, no ejemplar puro
2. Cohesión / Acoplamiento       10%     4       0.40         tables.py fuente única; keyring.py logging consolidado (7 loggers ad-hoc -> 1)
3. Capas / Límites               10%     4       0.40         get_db_key() usa app.settings.DATABASE_URL, ya no os.getenv directo
4. Seguridad                     15%     4       0.60         Derivación de clave funciona, PRAGMA correcto (orden+hex), dispatch DPAPI/base64 sin adivinar, SQLCIPHER_KEY normalizado — techo en 4 por no poder probar contra SQLCipher real en este entorno
5. Robustez / Resiliencia        15%     4       0.60         Fail-fast en muerte de hilo, detalle de HTTPError preservado, retry manual apropiado para el diseño de sync (trigger manual, no loop automático)
6. Consistencia transaccional    10%     5       0.50         Idempotencia (AppliedSyncEvent) + orden FK + hash chain + orden PRAGMA, todo con test de regresión dedicado
7. Observabilidad                10%     4       0.40         Logging estructurado consistente en todo el épico, confirmado con LOG_LEVEL=DEBUG real
8. Configuración / Entornos       5%     4       0.20         app.settings centralizado en todo el código tocado esta sesión
9. Pruebas / Calidad             10%     4       0.40         286 unit + 22 e2e verdes; packaging/*.{spec,iss} sin cobertura automatizada posible (validación Windows manual, alcance aceptado desde el diseño original de P4)
10. Escalabilidad                 5%     3       0.15         Sin cambios — apply_pull full-replace, trade-off v1 ya documentado, no crítico
──────────────────────────────────────────────────────────────────────────────
TOTAL                          100%                  4.05 / 5.00
SCORE FINAL                                          4.05 / 5.00 🟢
```

**Interpretación:** 🟢 **Maduro — deuda técnica controlada.** El épico cruza el umbral de
madurez (≥4.2 sería el ideal completo, 4.05 está en el borde superior de 🟡 fuerte / 🟢
temprano según el criterio estricto de la skill) tras corregir sistemáticamente los 5
hallazgos críticos originales de P4 más los 7 hallazgos adicionales encontrados en la propia
revisión de esos fixes. Dos limitaciones quedan explícitamente fuera del alcance verificable
en este entorno (no por falta de esfuerzo, sino por naturaleza): la build real de PyInstaller
y la compilación real de Inno Setup solo pueden confirmarse en una máquina Windows.

---

## 4. Lo que NO se puede cerrar desde este entorno

- `packaging/desktop.spec`: sintaxis corregida contra la API real y documentada de
  PyInstaller, pero **nunca ejecutado contra un PyInstaller real** (no instalado en Linux).
- `packaging/installer.iss`: sintaxis corregida contra Inno Setup real, pero **nunca
  compilado contra Inno Setup real** (Windows-only).
- SQLCipher real: toda la verificación de `PRAGMA key`/derivación de clave se hizo contra
  **stock `sqlite3`** (que no entiende `PRAGMA key`, lo ignora silenciosamente) — confirma que
  la sintaxis no rompe nada, pero no confirma que el cifrado real funcione. Requiere
  `sqlcipher3-binary` instalado (solo en Windows, vía `requirements-desktop.txt`).

Estas tres cosas son, por diseño del propio épico (`design.md` de P4: "Build Windows: su
validación es manual, no CI"), responsabilidad de una validación manual en una máquina Windows
real antes de confiar en que el instalador funciona de punta a punta. No es una tarea que este
entorno de desarrollo pueda completar — el trabajo hecho acá deja el código en el mejor estado
verificable posible sin esa máquina.

---

## 5. Estado final por slice

| PAE | slice | Score | Estado |
|-----|-------|-------|--------|
| P1 | Fundamentos | 🟡 3.80/5.00 | Sin cambios, commiteado |
| P2 | Backend de sync | 🟡 3.70/5.00 | Sin cambios, commiteado |
| P3 | Cliente de sync | 🟡 (funcional, ver informe de cierre) | Commiteado |
| P4 | Empaquetado desktop | 🟢 (dentro del score integrado de esta re-auditoría) | **Sin commitear** |
| P5 | e2e multi-nodo | 🟢 (dentro del score integrado de esta re-auditoría) | **Sin commitear** |
| **Épico (integrado)** | — | **🟢 4.05/5.00** | P1-P3 commiteados, P4-P5 pendientes de commit |

---

## 6. Verificación final

- `pytest tests/unit` (sin exclusiones) → **286 passed**
- `pytest tests/e2e -m e2e` → **22 passed**
- `bash specs/offline-desktop-sync/verify.sh` → **exit 0**, primera vez en la historia del
  script que corre de punta a punta sin fallar (unit + e2e + estructura)
- Sin procesos uvicorn huérfanos ni archivos temporales tras las corridas.

---

*Informe generado por Claude Code (skills: code-review, auditoria-ingenieria).*
