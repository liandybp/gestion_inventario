# Design — P4: Empaquetado desktop (Windows) + cifrado en reposo

- **Slice:** P4 de 5 (`offline-desktop-sync`)
- **Proyecto:** gestion_inventario · **Rama base:** `develop` · **Versión objetivo:** v0.4.7
- **Fase SDD:** 2 de 4 (Design)
- **REQs cubiertos:** REQ-2 (instalable Windows), REQ-3 (SQLCipher AES-256), REQ-4 (llave vía DPAPI); completa REQ-1 (runtime desktop: pywebview + uvicorn embebido)
- **Depende de:** P3 (comportamiento desktop, `APP_MODE`, cliente sync) — verde
- **Insumo:** `specs/offline-desktop-sync/requirements.md`

> ⚠️ **Verificación honesta:** este slice produce artefactos de plataforma **Windows**
> (instalador, PyInstaller, SQLCipher, pywebview, DPAPI). En el entorno de CI/dev Linux
> **no** se pueden validar con `pytest`. Cada envelope declara su tipo de verificación:
> **[unit]** (testeable en Linux/CI), **[import]** (solo que el módulo importe sin romper el
> server), o **[manual-win]** (requiere un equipo Windows). Los [manual-win] NO se marcan
> "verde" en CI; se validan en el build de escritorio. Esto es explícito, no una omisión.

---

## 2.1 Codebase Grounding

| Archivo | Operación | Rol |
|---------|-----------|-----|
| `requirements-desktop.txt` | CREATE | Deps solo-desktop (aisladas del server/Docker) |
| `app/desktop/__init__.py` | CREATE | Paquete del runtime desktop |
| `app/desktop/keyring.py` | CREATE | `get_db_key()`: DPAPI en Windows + fallback dev |
| `app/db.py` | MODIFY | `PRAGMA key` en el listener `connect` cuando hay llave desktop ⚠️ invariante |
| `app/desktop/launcher.py` | CREATE | uvicorn embebido (hilo) + ventana pywebview |
| `packaging/desktop.spec` | CREATE | Spec de PyInstaller |
| `packaging/installer.iss` | CREATE | Script Inno Setup (instalador Windows) |
| `packaging/README-desktop.md` | CREATE | Cómo construir el instalador (Windows) |
| `tests/unit/test_desktop_keyring.py` | CREATE | Test del fallback del key manager (parte testeable) |
| `app/settings.py` | REFERENCE | `is_desktop()`, `DATABASE_URL` (de P3/actual) |
| `app/main.py` | REFERENCE | `app = FastAPI(lifespan=...)`, `uvicorn.run(...)` en `__main__` |

**Firmas reales verificadas:**
```python
# app/db.py:10 — @event.listens_for(Engine, "connect") _set_sqlite_pragma(dbapi_connection, connection_record):
#   if DATABASE_URL.startswith("sqlite"): cursor.execute("PRAGMA foreign_keys=ON")
# app/db.py:29 — engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, pool_pre_ping=True)  [rama sqlite]
# app/main.py:55 — app = FastAPI(title="Inventario", lifespan=lifespan)  ; :200 if __name__ == "__main__": uvicorn.run(...)
# requirements.txt — cryptography==50.0.0 ya presente ; httpx presente. pysqlcipher/pywebview/pyinstaller/pywin32 NO están.
```

---

## 2.2 Architecture Approach

**Runtime desktop (REQ-1/2).** `app/desktop/launcher.py` es el entrypoint del ejecutable
empaquetado: fija `APP_MODE=desktop` y el `DATABASE_URL` local (SQLCipher), arranca **uvicorn en
un hilo** contra `app.main:app` en `127.0.0.1:<puerto libre>`, y abre una **ventana pywebview**
apuntando a esa URL. El usuario ve la misma app web dentro de una ventana nativa. **PyInstaller**
empaqueta el intérprete + la app + deps en un ejecutable; **Inno Setup** produce el instalador.

**Cifrado en reposo (REQ-3/4).** En modo desktop el `DATABASE_URL` usa el dialecto SQLCipher
(`sqlite+pysqlcipher:///<ruta>/inventario.db`). La llave se aplica con `PRAGMA key` en el
listener `connect` de `db.py` (debe ser la **primera** operación de la conexión, antes de
`foreign_keys`). La llave la provee `app/desktop/keyring.py::get_db_key()`:
- **Windows (REQ-4):** genera una llave aleatoria la primera vez, la protege con **DPAPI**
  (`win32crypt.CryptProtectData`, atada a usuario/máquina) y la persiste cifrada junto al `.db`;
  en arranques siguientes la desprotege (`CryptUnprotectData`). Copiar el `.db` a otro equipo no
  sirve sin poder desproteger ese blob DPAPI.
- **Fallback dev (no-Windows):** deriva la llave de una env var (`DESKTOP_DB_KEY`) o de un archivo
  local — permite correr/depurar el modo desktop en Linux sin DPAPI. **Nunca** se usa en el
  instalador de producción (Windows siempre tiene DPAPI).

**Aislamiento de dependencias.** `pywebview`, `pyinstaller`, `sqlcipher3-binary`, `pywin32` van
en `requirements-desktop.txt` **separado** — el `requirements.txt` del server y la imagen Docker
**no** los incluyen. Así el servidor central no arrastra deps de escritorio.

**Invariante server intacta:** en modo server, `get_db_key()` no se invoca (el listener solo
aplica `PRAGMA key` si `is_desktop()` y hay llave). El comportamiento actual del server no cambia.

---

## 2.3 Data Model
Ninguno nuevo. P4 cambia **cómo** se abre la BD (cifrada), no su esquema. El `.db` local ya lo
crean las migraciones (P1-P3); SQLCipher solo cifra el archivo.

---

## 2.4 API Contracts
Ninguno. P4 es runtime + build, sin endpoints.

---

## 2.5 Security Review

| Check | Respuesta |
|-------|-----------|
| Datos en reposo | **Cifrado AES-256** del `.db` completo (SQLCipher) — datos, índices, journals (REQ-3) |
| Gestión de llave | DPAPI atado a usuario/máquina (REQ-4); la llave nunca se hardcodea en el instalador |
| Robo de equipo | El `.db` copiado a otra máquina es ilegible (no se puede desproteger el blob DPAPI ajeno) |
| Superficie local | uvicorn escucha solo en `127.0.0.1` (no expuesto a la red); pywebview es la única UI |
| Fallback dev | Solo activo fuera de Windows; documentado como no-producción |
| Deps nuevas | Aisladas en `requirements-desktop.txt`; no tocan el server. Requieren aprobación (invariante) |

---

## 2.6 Trade-offs

| # | Decisión | Elegido | Alternativa | Por qué no |
|---|----------|---------|-------------|-----------|
| T1 | Shell desktop | **pywebview** (decisión del usuario) | Tauri / Electron | Cero código en otro lenguaje; reutiliza backend+frontend; ligero (ver brainstorming) |
| T2 | Cifrado | **SQLCipher** (`sqlite+pysqlcipher`) | Cifrado por columnas | Cifra todo el archivo transparente para SQLAlchemy; sin tocar modelos/queries |
| T3 | Llave | **DPAPI + persistencia local cifrada** | Pedir contraseña al abrir | Sin fricción para el operador; atado a la máquina; alternativa (passphrase) es follow-up |
| T4 | Deps | **`requirements-desktop.txt` separado** | Añadir a requirements.txt | No contaminar el server/Docker con deps de escritorio Windows |
| T5 | Aplicar llave | `PRAGMA key` en listener `connect` | Subclase de engine | Mínimo cambio; el listener ya existe; solo se antepone la llave |

---

## 2.7 Migration & Rollback
Sin migración de datos. Rollback = no distribuir el instalador; revertir el cambio de `db.py`
(el `PRAGMA key` es condicional a `is_desktop()` + llave, así que revertir no afecta al server).
**Nota de operación:** una BD ya cifrada solo se abre con su llave; perder el blob DPAPI = perder
el acceso a los datos no sincronizados de ese nodo (mitigado por el backup del outbox de P3 y el
primer-sync-online que repuebla lo de lectura).

---

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría

| Dim | Riesgo | Mitigación → Instruction de envelope |
|-----|--------|--------------------------------------|
| **1. SoC/SRP** | Mezclar gestión de llave, arranque de uvicorn y pywebview en un archivo | Separados: `keyring.py` (llave), `launcher.py` (arranque+ventana); `db.py` solo aplica la llave |
| **3. Capas/Límites** | `db.py` importando código de `desktop/` crea dependencia server→desktop | `db.py` importa `keyring.get_db_key` **de forma perezosa y protegida** (import dentro del listener, try/except) — el server nunca ejecuta esa rama; sin acoplamiento en tiempo de import |
| **5. Robustez/Resiliencia** | `get_db_key()` (DPAPI/FS) puede fallar; el listener no debe romper el server | El listener aplica `PRAGMA key` solo si `is_desktop()` y `get_db_key()` devolvió llave; cualquier error → log + seguir (server) / abortar arranque claro (desktop). try/except explícito |
| **6. Consistencia Transaccional** | `PRAGMA key` debe ir ANTES de cualquier SQL (incl. `foreign_keys`) | El envelope de `db.py` especifica el ORDEN literal: key primero, luego `foreign_keys` |

Sin hallazgos que bloqueen Fase 3.

---

## 2.10 Delegation
Ninguna. `security-architect` sería opcional (cifrado/llaves) pero el patrón (SQLCipher + DPAPI +
llave no hardcodeada + listen-on-localhost) está cubierto en §2.5 y es estándar documentado.

---

## Interfaces nuevas (contratos para Fase 3)
```python
# app/desktop/keyring.py
def get_db_key() -> str | None:
    """Llave hex de SQLCipher. Windows: DPAPI (genera+protege / desprotege). No-Windows:
    DESKTOP_DB_KEY env o archivo local (fallback dev). None si no hay llave (server)."""

# app/desktop/launcher.py
def main() -> None:
    """Fija APP_MODE=desktop + DATABASE_URL local; arranca uvicorn en un hilo; abre pywebview."""

# app/db.py (listener modificado)
#   if is_desktop(): key = get_db_key(); if key: cursor.execute(f"PRAGMA key = \"x'{key}'\"") ANTES de foreign_keys
```
