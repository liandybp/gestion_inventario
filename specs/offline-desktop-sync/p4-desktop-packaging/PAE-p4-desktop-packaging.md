# PAE-P4: offline-desktop-sync — Empaquetado desktop (Windows) + cifrado

<!-- INSTRUCCIONES OPENCODE: leer CLAUDE.md + sección 3; P1-P3 verdes; una tarea a la vez;
     T1 (deps) y T3 (db.py) REQUIEREN aprobación explícita del usuario antes de ejecutar;
     los envelopes [manual-win] NO se validan en CI Linux — solo se crean los archivos. -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` · **Rama de trabajo** | `feature/offline-sync-p4-desktop` |
| **Fecha** | 2026-08-28 |
| **Versión** | v0.4.7 (bump en P1; P4 no re-bumpea) |
| **Slice** | P4 de 5 (`offline-desktop-sync`) |
| **PAE anterior** | P3 — Cliente de sync (verde en dev) |
| **Fase** | Claude Code (Opus 4.8) planifica → OpenCode + Qwen3.6-35B-A3B ejecuta |

### Documentos fuente
`specs/offline-desktop-sync/requirements.md` · `.../p4-desktop-packaging/design.md` · `.../p4-desktop-packaging/tasks.md`

### Skills asignadas
- [ ] `security-architect` — key manager (DPAPI + fallback)
- [ ] `backend-developer` — `PRAGMA key` en db.py, launcher
- [ ] `devops-engineer` — requirements-desktop, PyInstaller spec, Inno Setup
- [ ] `technical-writer` — doc de build
- [ ] `test-automation-engineer` — test del key manager

### ⚠️ Gates de aprobación (antes de ejecutar)
- **T1 (dependencias nuevas):** `pywebview`, `pyinstaller`, `sqlcipher3-binary`, `pywin32` — invariante del proyecto exige aprobación.
- **T3 (`app/db.py`):** invariante "no modificar sin confirmación" — el cambio es aditivo/condicional pero requiere OK explícito.

---

## 1. Objetivo

### 1.1 Qué se construye
El runtime y el empaquetado de escritorio para Windows: un launcher (`app/desktop/launcher.py`)
que arranca uvicorn embebido y abre una ventana **pywebview** contra la misma app; **cifrado en
reposo** de la BD local con **SQLCipher (AES-256)** cuya llave se gestiona con **Windows DPAPI**
(`app/desktop/keyring.py` + `PRAGMA key` condicional en `db.py`); y el pipeline de build
(**PyInstaller** + **Inno Setup**) que produce un instalador `.exe`. Deps de escritorio aisladas
en `requirements-desktop.txt`.

### 1.2 Por qué
P1-P3 dejan la app funcional como nodo desktop, pero corriendo desde el código. P4 la vuelve
**instalable en un PC Windows sin Python** y **protege los datos locales** (PII de clientes +
finanzas) ante robo/copia del equipo.

### 1.3 Valor esperado
- Un instalador `.exe` que deja la app lista con un doble clic, sin dependencias externas.
- BD local ilegible fuera del equipo (SQLCipher + DPAPI).
- Cero impacto en el servidor central (deps aisladas; `db.py` condicional a desktop).

### 1.4 Qué NO se toca
- `auth.py`, `deps.py`, `docker-compose*`, `requirements.txt` (las deps desktop van aparte).
- El comportamiento del server (el cifrado y el key manager solo actúan en modo desktop).
- e2e multi-nodo → **P5**.

---

## 2. Decisiones de diseño

| # | Decisión | Justificación |
|---|----------|---------------|
| D1 | Shell **pywebview** | Decisión del usuario; reutiliza backend+frontend, ligero |
| D2 | **SQLCipher** vía `sqlite+pysqlcipher` + `PRAGMA key` | Cifra todo el archivo, transparente para SQLAlchemy |
| D3 | Llave con **DPAPI** + persistencia local cifrada; fallback dev fuera de Windows | Atada a máquina/usuario; sin fricción; dev en Linux posible |
| D4 | Deps en **`requirements-desktop.txt`** separado | No contaminar server/Docker |
| D5 | `PRAGMA key` en el **listener `connect`** condicional a `is_desktop()`+llave | Mínimo cambio; server intacto |
| D6 | Verificación **honesta**: [unit]/[import]/[manual-win] | Windows/SQLCipher/pywebview no validables en CI Linux |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama
```
★ NUEVO   ✦ MODIFICADO

  Instalador (Inno Setup ★) ──► dist/ (PyInstaller ★ desde desktop.spec ★)
                                     │
  launcher.py ★ ──► uvicorn (hilo) ──► app.main:app (mismo backend)
        │                                    │
        └──► ventana pywebview               └──► db.py ✦ (PRAGMA key si is_desktop + llave)
                                                        │
                                             keyring.py ★ (DPAPI Win / fallback dev)
  requirements-desktop.txt ★ (pywebview, pyinstaller, sqlcipher3-binary, pywin32)
```

### 3.2 Inventario de archivos (scope duro)
| Archivo | Operación |
|---------|-----------|
| `requirements-desktop.txt` | CREATE |
| `app/desktop/__init__.py` | CREATE |
| `app/desktop/keyring.py` | CREATE |
| `app/db.py` | MODIFY ⚠️ |
| `app/desktop/launcher.py` | CREATE |
| `packaging/desktop.spec` | CREATE |
| `packaging/installer.iss` | CREATE |
| `packaging/README-desktop.md` | CREATE |
| `tests/unit/test_desktop_keyring.py` | CREATE |

### 3.3 Dependencias existentes (firmas reales)
Ver `design.md` §2.1 (`db.py` listener `connect`, `app = FastAPI(lifespan)`, `uvicorn.run`,
`cryptography`/`httpx` presentes, SQLCipher/pywebview/pyinstaller/pywin32 ausentes).

### 3.4 Interfaces nuevas
`keyring.get_db_key()`, `launcher.main()`, listener `db.py` con `PRAGMA key`. Ver `design.md`.

---

## 4. Fases de implementación (Task Envelopes)

8 envelopes en `tasks.md`. Batches: **T1-T3 · T4-T6 · T7-T8**.

| # | Envelope | Files | Verif. | Depends |
|---|----------|-------|--------|---------|
| T1 | requirements-desktop | requirements-desktop.txt | manual-win | none ⚠️OK deps |
| T2 | key manager | desktop/__init__.py, keyring.py | unit+manual-win | none |
| T3 | PRAGMA key en db.py | db.py | unit (server) | T2 ⚠️OK db.py |
| T4 | launcher | desktop/launcher.py | import+manual-win | T2 |
| T5 | PyInstaller spec | packaging/desktop.spec | manual-win | T4 |
| T6 | Inno Setup | packaging/installer.iss | manual-win | T5 |
| T7 | doc build | packaging/README-desktop.md | manual-win | T5,T6 |
| T8 | test key manager | test_desktop_keyring.py | unit | T2 |

---

## 5. Plan de tests y verificación

### 5.0 Pre-implementación
```bash
# P1-P3 verdes. Suite unit verde antes de empezar.
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
```

### 5.1 Verificación por envelope
[unit] → comando pytest; [import] → import sin romper server; [manual-win] → build en Windows
(NO se marca verde en CI). Ver cada envelope.

### 5.3 Tests que deben seguir pasando
- [ ] Suite unit completa tras T3 (server intacto: el `PRAGMA key` es no-op sin `is_desktop()`).

### 5.4 Tests e2e
El flujo completo desktop (instalar → operar offline → cifrado → sync) se valida **manualmente en
Windows** (README-desktop) y su e2e automatizado multi-nodo es **P5**. P4 aporta el test unit del
key manager; el resto es build validado en Windows. Excepción explícita y trazada.

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación | Rollback |
|---|--------|-------|---------|------------|----------|
| R1 | `PRAGMA key` rompe el server | Baja | Alto | Condicional a `is_desktop()`+llave; try/except; suite unit lo verifica | git revert db.py |
| R2 | Pérdida del blob DPAPI = datos locales inaccesibles | Media | Alto | Backup outbox (P3) + primer-sync repuebla lectura; documentar en README | re-provisionar nodo |
| R3 | PyInstaller no empaqueta templates/static | Media | Medio | `datas` explícito en el spec; validar en Windows | ajustar spec |
| R4 | Deps desktop contaminan el server | Baja | Medio | `requirements-desktop.txt` separado; Docker usa solo `requirements.txt` | — |
| R5 | sqlcipher3-binary no instala en Linux dev | Media | Bajo | Fallback dev de la llave no exige SQLCipher; SQLCipher solo obligatorio en Windows build | usar SQLite plano en dev |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit
- [ ] **Gates aprobados:** deps (T1) y `db.py` (T3) confirmados por el usuario
- [ ] **[unit] verde:** suite unit + `test_desktop_keyring.py` (en Linux/CI) tras los cambios
- [ ] **Server intacto:** suite verde con `APP_MODE` ausente (PRAGMA key no-op)
- [ ] **[manual-win]:** build del instalador validado en Windows (README-desktop) — fuera de CI
- [ ] **Invariantes:** `requirements.txt`/`docker-compose*`/`auth.py` sin cambios
- [ ] **Scope:** `git diff --name-only develop` == los 9 archivos de 3.2
- [ ] **Limpieza:** sin temporales

### 7.2 Script de verificación (parte automatizable)
```bash
#!/bin/bash
set -e
export SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory:
python3 -m pytest tests/unit -q                       # server intacto
python3 -m pytest tests/unit/test_desktop_keyring.py -v
python3 -c "import app.desktop.launcher as l; assert hasattr(l,'main')"
git diff --name-only develop
echo "=== CI CHECKS PASSED (build Windows: manual, ver README-desktop) ==="
```

### 7.3 Commits esperados
```
build(desktop): requirements-desktop + key manager (DPAPI + fallback)
feat(desktop): SQLCipher PRAGMA key condicional en db.py
feat(desktop): launcher pywebview + uvicorn embebido
build(desktop): PyInstaller spec + Inno Setup + doc de build
test(desktop): key manager fallback
```

### 7.4 Criterios de aceptación (Gherkin)
- [ ] REQ-3: el `.db` desktop no se abre con un lector SQLite estándar sin la llave (manual-win).
- [ ] REQ-4: copiar el `.db` a otro equipo no lo vuelve legible (DPAPI atado, manual-win).
- [ ] REQ-2: el instalador deja la app funcional en un Windows limpio (manual-win).
- [ ] REQ-1: `python -m app.desktop.launcher` abre la ventana con la app (manual-win).

---

> **Fin de P4.** Al cerrar, se genera **P5 — e2e multi-nodo + endurecimiento** vía SDD-Agent.
> P5 valida el sistema completo (dos nodos + central, con conflicto) y cierra el épico.
