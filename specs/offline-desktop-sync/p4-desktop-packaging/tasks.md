# Task Envelopes — P4: Empaquetado desktop + cifrado

- **Slice:** P4 (`offline-desktop-sync`) · **Rama base:** `develop` · **Depende de:** P3 (verde)
- **Insumo:** `design.md` · **REQs:** 2, 3, 4 (+ REQ-1 runtime)
- **Test runner (partes testeables):** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest`
- **Batches:** T1-T3 · T4-T6 · T7-T8

> **Tipos de verificación:** **[unit]** testeable en CI · **[import]** solo import sin romper server · **[manual-win]** requiere Windows (no se marca verde en CI).
> Mapeo: REQ-2→T5,T6,T7 · REQ-3→T3 · REQ-4→T2 · REQ-1→T4 · deps→T1.

---

### T1: `requirements-desktop.txt` (deps aisladas)

**Depends on:** none · **Skill:** devops-engineer · **Agent:** coder
**Files:** `requirements-desktop.txt` (CREATE) · **Pillars:** P1, P5, P6
**Verificación:** [manual-win] (instalación real en Windows)

> ⚠️ **Requiere aprobación de dependencias (invariante del proyecto).** No instalar hasta que
> el usuario apruebe esta lista. Versiones sugeridas (fijar tras validar en Windows).

#### Objective
Declarar las dependencias solo-desktop, separadas de `requirements.txt`.

#### Instructions
**Step 1: Create** `requirements-desktop.txt`:
```
# Dependencias SOLO para el empaquetado de escritorio (Windows).
# NO se instalan en el servidor central ni en la imagen Docker.
# Instalar con: pip install -r requirements.txt -r requirements-desktop.txt
-r requirements.txt

pywebview==5.3.2
pyinstaller==6.11.1
sqlcipher3-binary==0.5.4
pywin32==308 ; sys_platform == "win32"
```
**Edge cases:** `pywin32` solo en Windows (marcador de entorno). El resto es multiplataforma
para poder importar/desarrollar en Linux salvo `sqlcipher3-binary` (que en Linux puede requerir
libsqlcipher; el fallback dev de la llave no exige SQLCipher).

#### Verification
```bash
# [manual-win] En un Windows con Python:
#   pip install -r requirements.txt -r requirements-desktop.txt
#   python -c "import webview, sqlcipher3; print('deps ok')"
```
#### Expected Report
- [ ] `requirements-desktop.txt` creado · (validación de instalación en Windows, manual)

---

### T2: Key manager (`app/desktop/keyring.py`)

**Depends on:** none · **Skill:** security-architect · **Agent:** coder
**Files:** `app/desktop/__init__.py` (CREATE), `app/desktop/keyring.py` (CREATE)
**Pillars:** P1, P4, P5, P6 · **Verificación:** [unit] (fallback) + [manual-win] (DPAPI)

#### Context
```python
# app/settings.py — env(name, default) -> str. Rutas del .db local se pasan por env DESKTOP_DB_PATH.
# Windows: win32crypt.CryptProtectData / CryptUnprotectData (pywin32). Linux dev: env DESKTOP_DB_KEY.
```
#### Objective
Crear `get_db_key()` con DPAPI en Windows y fallback dev fuera de Windows.

#### Instructions
**Step 1: Create** `app/desktop/__init__.py` VACÍO.
**Step 2: Create** `app/desktop/keyring.py`:
```python
from __future__ import annotations

import os
import secrets
import sys
from pathlib import Path

from app.logger import get_logger

_log = get_logger(__name__)


def _key_file() -> Path:
    base = os.getenv("DESKTOP_DB_PATH", "").strip() or os.path.join(os.path.expanduser("~"), ".gestion_inventario")
    return Path(base).with_suffix(".key")


def _windows_get_key() -> str:
    import win32crypt  # type: ignore

    kf = _key_file()
    if kf.exists():
        blob = kf.read_bytes()
        raw = win32crypt.CryptUnprotectData(blob, None, None, None, 0)[1]
        return raw.decode("ascii")
    key = secrets.token_hex(32)
    blob = win32crypt.CryptProtectData(key.encode("ascii"), "gestion_inventario", None, None, None, 0)
    kf.parent.mkdir(parents=True, exist_ok=True)
    kf.write_bytes(blob)
    return key


def _fallback_get_key() -> str | None:
    env_key = os.getenv("DESKTOP_DB_KEY", "").strip()
    if env_key:
        return env_key
    kf = _key_file()
    if kf.exists():
        return kf.read_text(encoding="ascii").strip()
    key = secrets.token_hex(32)
    kf.parent.mkdir(parents=True, exist_ok=True)
    kf.write_text(key, encoding="ascii")
    return key


def get_db_key() -> str | None:
    """Llave hex de SQLCipher. Windows: DPAPI. No-Windows: env/archivo (fallback dev)."""
    try:
        if sys.platform == "win32":
            return _windows_get_key()
        return _fallback_get_key()
    except Exception:  # noqa: BLE001
        _log.error("No se pudo obtener la llave de la BD", exc_info=True)
        return None
```
**Edge cases:** sin `DESKTOP_DB_PATH` → `~/.gestion_inventario.key`; error → `None` (no rompe).

#### Verification
```bash
# [unit]
DESKTOP_DB_KEY=deadbeef python3 -c "from app.desktop.keyring import get_db_key; print(get_db_key())"
# Expected: deadbeef
```
#### Expected Report
- [ ] `app/desktop/__init__.py` + `keyring.py` creados · fallback imprime la llave

---

### T3: `PRAGMA key` en `db.py` (cifrado condicional)

**Depends on:** T2 · **Skill:** backend-developer · **Agent:** refactor
**Files:** `app/db.py` · **Pillars:** P1, P3, P4, P5, P6 · **Verificación:** [unit] (server intacto)

> ⚠️ **Invariante:** `app/db.py` no se modifica sin confirmación. Este cambio es **aditivo y
> condicional** (`is_desktop()` + llave presente); en modo server NO altera nada. Requiere OK.

#### Context
```python
# app/db.py:10 — ACTUAL
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    """Enable foreign key enforcement on SQLite connections."""
    if DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
```
#### Objective
Aplicar `PRAGMA key` (SQLCipher) ANTES de `foreign_keys` cuando el nodo es desktop y hay llave.

#### Instructions
**Step 1:** `Read app/db.py`
**Step 2:** Reemplazar el listener por:
```python
# BEFORE: (el listener actual mostrado en Context)
# AFTER:
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    """SQLCipher key (desktop) + foreign key enforcement on SQLite connections."""
    if DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_connection.cursor()
        try:
            from app.settings import is_desktop
            if is_desktop():
                from app.desktop.keyring import get_db_key
                key = get_db_key()
                if key:
                    # PRAGMA key DEBE ser lo primero, antes de cualquier otro SQL.
                    cursor.execute(f"PRAGMA key = \"x'{key}'\"")
        except Exception:  # noqa: BLE001
            pass
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
```
**Edge cases:** modo server → el `if is_desktop()` es False → idéntico a hoy. Import perezoso de
`desktop.keyring` (no crea dependencia server→desktop en tiempo de import). Error → se ignora y
sigue con `foreign_keys` (no rompe el server).

#### Verification
```bash
# [unit] server intacto: la suite completa debe seguir verde (modo server, sin llave)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
# Expected: PASS
```
#### Expected Report
- [ ] `app/db.py` — listener con `PRAGMA key` condicional · suite unit PASS (server intacto)

---

### T4: Launcher desktop (`app/desktop/launcher.py`)

**Depends on:** T2 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/desktop/launcher.py` (CREATE) · **Pillars:** P1, P5, P6
**Verificación:** [import] + [manual-win]

#### Context
```python
# app/main.py — app = FastAPI(...). uvicorn.run(app, host, port). Migraciones corren en lifespan.
# pywebview: import webview; webview.create_window(title, url); webview.start()
# Puerto libre: socket. Hilo: threading.Thread.
```
#### Objective
Crear el entrypoint desktop: uvicorn en un hilo + ventana pywebview.

#### Instructions
**Step 1: Create** `app/desktop/launcher.py`:
```python
from __future__ import annotations

import os
import socket
import threading
import time


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _default_db_url() -> str:
    base = os.getenv("DESKTOP_DB_PATH", "").strip() or os.path.join(os.path.expanduser("~"), ".gestion_inventario.db")
    return f"sqlite+pysqlcipher:///{base}"


def main() -> None:
    os.environ.setdefault("APP_MODE", "desktop")
    os.environ.setdefault("DATABASE_URL", _default_db_url())
    os.environ.setdefault("SESSION_SECRET", os.getenv("SESSION_SECRET", "desktop-local"))

    port = _free_port()
    url = f"http://127.0.0.1:{port}"

    def _serve() -> None:
        import uvicorn
        uvicorn.run("app.main:app", host="127.0.0.1", port=port, log_level="warning")

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
    time.sleep(1.5)  # dar tiempo a que uvicorn levante

    import webview  # import perezoso: solo en el runtime desktop
    webview.create_window("Gestión Inventario", url, width=1280, height=800)
    webview.start()


if __name__ == "__main__":
    main()
```
**Edge cases:** `APP_MODE`/`DATABASE_URL` se fijan solo si no vienen ya del entorno (permite
override). Import de `webview`/`uvicorn` perezoso (no rompe el server que importa este módulo).

#### Verification
```bash
# [import] el módulo importa sin webview instalado (main() no se ejecuta):
python3 -c "import app.desktop.launcher as l; print(hasattr(l,'main'))"
# Expected: True
# [manual-win] En Windows con deps: python -m app.desktop.launcher → abre la ventana
```
#### Expected Report
- [ ] `app/desktop/launcher.py` creado · import imprime `True`

---

### T5: Spec de PyInstaller (`packaging/desktop.spec`)

**Depends on:** T4 · **Skill:** devops-engineer · **Agent:** coder
**Files:** `packaging/desktop.spec` (CREATE) · **Pillars:** P1, P6 · **Verificación:** [manual-win]

#### Context
```python
# Entry: app/desktop/launcher.py:main. Incluir app/templates y app/static como datos.
# PyInstaller .spec estándar (Analysis/PYZ/EXE/COLLECT).
```
#### Objective
Crear el spec de PyInstaller que empaqueta el launcher con templates/static.

#### Instructions
**Step 1: Create** `packaging/desktop.spec`:
```python
# -*- mode: python ; coding: utf-8 -*-
# Construir con:  pyinstaller packaging/desktop.spec   (en Windows)
from PyInstaller.utils.hooks import collect_submodules

block_cipher = None

a = Analysis(
    ['../app/desktop/launcher.py'],
    pathex=['..'],
    binaries=[],
    datas=[('../app/templates', 'app/templates'), ('../app/static', 'app/static')],
    hiddenimports=collect_submodules('app') + ['uvicorn', 'webview', 'sqlcipher3'],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz, a.scripts, [], exclude_binaries=True,
    name='GestionInventario', console=False,
)
coll = COLLECT(exe, a.binaries, a.datas, name='GestionInventario')
```
**Edge cases:** `datas` incluye templates y static (Jinja/HTMX los necesita en runtime).
`console=False` (app de ventana, sin consola).

#### Verification
```bash
# [manual-win] En Windows:  pyinstaller packaging/desktop.spec
#   → dist/GestionInventario/GestionInventario.exe arranca la app
```
#### Expected Report
- [ ] `packaging/desktop.spec` creado · (build validado en Windows, manual)

---

### T6: Instalador Inno Setup (`packaging/installer.iss`)

**Depends on:** T5 · **Skill:** devops-engineer · **Agent:** coder
**Files:** `packaging/installer.iss` (CREATE) · **Pillars:** P1, P6 · **Verificación:** [manual-win]

#### Context
```
# Inno Setup empaqueta dist/GestionInventario/ en un instalador .exe con acceso directo.
```
#### Objective
Crear el script Inno Setup del instalador Windows.

#### Instructions
**Step 1: Create** `packaging/installer.iss`:
```ini
; Compilar con Inno Setup (Windows): iscc packaging\installer.iss
[Setup]
AppName=Gestion Inventario
AppVersion=0.4.7
DefaultDirName={autopf}\GestionInventario
DefaultGroupName=Gestion Inventario
OutputBaseFilename=GestionInventario-Setup-0.4.7
Compression=lzma
SolidCompression=yes
PrivilegesRequired=lowest

[Files]
Source: "..\dist\GestionInventario\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs

[Icons]
Name: "{group}\Gestion Inventario"; Filename: "{app}\GestionInventario.exe"
Name: "{autodesktop}\Gestion Inventario"; Filename: "{app}\GestionInventario.exe"

[Run]
Filename: "{app}\GestionInventario.exe"; Description: "Iniciar Gestion Inventario"; Flags: nowait postinstall skipifsilent
```
**Edge cases:** `PrivilegesRequired=lowest` (instala en carpeta de usuario, sin admin) para que
DPAPI quede atado al usuario correcto.

#### Verification
```bash
# [manual-win] En Windows con Inno Setup:  iscc packaging\installer.iss
#   → Output\GestionInventario-Setup-0.4.7.exe instala y crea accesos directos
```
#### Expected Report
- [ ] `packaging/installer.iss` creado · (instalador validado en Windows, manual)

---

### T7: Doc de build (`packaging/README-desktop.md`)

**Depends on:** T5, T6 · **Skill:** technical-writer · **Agent:** docs
**Files:** `packaging/README-desktop.md` (CREATE) · **Pillars:** P1 · **Verificación:** [manual-win]

#### Objective
Documentar el proceso de build del instalador desde cero en Windows.

#### Instructions
**Step 1: Create** `packaging/README-desktop.md` con los pasos:
```markdown
# Build del instalador de escritorio (Windows)

Requisitos: Windows 10/11, Python 3.12+, Inno Setup 6.

1. Crear venv e instalar deps (incluye las de escritorio):
   ```
   python -m venv .venv && .venv\Scripts\activate
   pip install -r requirements.txt -r requirements-desktop.txt
   ```
2. Probar el runtime sin empaquetar:
   ```
   python -m app.desktop.launcher
   ```
   (Abre la ventana; la BD local se crea cifrada con SQLCipher + llave DPAPI.)
3. Empaquetar el ejecutable:
   ```
   pyinstaller packaging/desktop.spec
   ```
   Resultado: `dist/GestionInventario/GestionInventario.exe`.
4. Construir el instalador:
   ```
   iscc packaging\installer.iss
   ```
   Resultado: `Output/GestionInventario-Setup-0.4.7.exe`.

## Aprovisionamiento del nodo (primer sync)
Al primer arranque con conexión, el nodo se registra contra el servidor central
(`POST /sync/register`, ver PAE-P2) y guarda `server_url` + `device_token` en `sync_state`.
Después opera offline. La llave de cifrado la gestiona DPAPI (ver PAE-P4 / keyring.py).
```
#### Verification
```bash
# [manual-win] seguir el README en Windows y obtener el instalador.
```
#### Expected Report
- [ ] `packaging/README-desktop.md` creado con los 4 pasos + aprovisionamiento

---

### T8: Test del key manager (fallback)

**Depends on:** T2 · **Skill:** test-automation-engineer · **Agent:** coder
**Files:** `tests/unit/test_desktop_keyring.py` (CREATE) · **Pillars:** P1, P4, P5, P6
**Verificación:** [unit]

#### Context
```python
# app/desktop/keyring.py — get_db_key() ; en no-Windows usa DESKTOP_DB_KEY o archivo local.
# monkeypatch de env vars con pytest (os.environ) o fixture tmp_path para DESKTOP_DB_PATH.
```
#### Objective
Test del fallback dev de `get_db_key` (env var y persistencia en archivo).

#### Instructions
**Step 1: Create** `tests/unit/test_desktop_keyring.py`:
```python
from __future__ import annotations

import sys

import pytest

from app.desktop import keyring


@pytest.mark.skipif(sys.platform == "win32", reason="fallback dev solo fuera de Windows")
def test_env_key_takes_precedence(monkeypatch):
    monkeypatch.setenv("DESKTOP_DB_KEY", "abc123")
    assert keyring.get_db_key() == "abc123"


@pytest.mark.skipif(sys.platform == "win32", reason="fallback dev solo fuera de Windows")
def test_file_key_persists(monkeypatch, tmp_path):
    monkeypatch.delenv("DESKTOP_DB_KEY", raising=False)
    monkeypatch.setenv("DESKTOP_DB_PATH", str(tmp_path / "node.db"))
    k1 = keyring.get_db_key()
    k2 = keyring.get_db_key()
    assert k1 and k1 == k2  # persistida y estable entre llamadas
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_desktop_keyring.py -v
# Expected: 2 passed (en Linux/CI)
```
#### Expected Report
- [ ] `tests/unit/test_desktop_keyring.py` — 2 tests PASS (fuera de Windows)

---

## Checklist de los 7 mandamientos (Fase 3)
- [x] Before/after exactos · ≤800 tokens · ≤2 archivos · dependencias con firmas reales
- [x] Verificación mecánica DONDE aplica; [manual-win] declarado honestamente donde CI no puede validar
- [x] Batches de 3 (T1-T3 · T4-T6 · T7-T8)

> **Aprobación de dependencias (T1) obligatoria antes de ejecutar P4** (invariante del proyecto).
> **`db.py` (T3) requiere confirmación explícita** (invariante). El resto de P4 es aditivo.
> **Version:** el épico va en v0.4.7 (bump en P1). P4 no re-bumpea.
