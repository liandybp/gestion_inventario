# Diseño — App de escritorio offline-first con sincronización bidireccional

- **Fecha:** 2026-08-28
- **Proyecto:** gestion_inventario
- **Rama base:** `develop`
- **Versión objetivo:** v0.4.7 (PATCH por default — ver nota en §9; MINOR/MAJOR solo si el usuario lo marca como hito explícito)
- **Estado:** diseño aprobado (brainstorming) → pendiente PAE (SDD-Agent)
- **Modelo de planificación:** Claude Code (Opus 4.8) planifica y genera el PAE vía SDD-Agent; OpenCode + Qwen3.6-35B-A3B implementa

---

## 1. Objetivo

Ofrecer una **app de escritorio instalable en Windows** que permita a una ubicación
(POS o CENTRAL) **operar sin conexión a internet** y **sincronizar bidireccionalmente**
con la app online actual (`gestion.legatumbp.com`, FastAPI + PostgreSQL) **a petición del
usuario**, cuando haya conexión — que puede tardar días en estar disponible.

**Idea rectora:** no es una app nueva; es **la misma app** corriendo contra una BD SQLite
local en lugar del PostgreSQL central. Se reutiliza el 100% del backend (FastAPI +
SQLAlchemy + servicios) y del frontend (Jinja2 + HTMX). Lo único nuevo es la **capa de
sincronización** (un router en el servidor central + un módulo local) y el **empaquetado
de escritorio** (pywebview + instalador).

**Idea de seguridad rectora:** la BD local es un vector de ataque nuevo (un PC de POS no es
un servidor endurecido). El diseño trata la confidencialidad (cifrado en reposo), la
integridad (que no se manipule el outbox a mano) y la exposición mínima de credenciales
como requisitos de primera clase, no como añadidos.

---

## 2. Topología y modelo de conflictos

**Hub-and-spoke con sync manual bajo demanda.** Cada ubicación es un nodo local-first,
siempre offline-capaz. Los nodos **no hablan entre sí**: sincronizan contra el servidor
central, que es la fuente de verdad canónica. La sync la dispara el usuario con un botón.

**Por qué el riesgo de conflicto es bajo (confirmado con el usuario):**

| Operación | Offline | Conflicto | Razón |
|---|---|---|---|
| **Venta** (POS, su propio stock) | ✅ Escritura | **Ninguno** | Partición natural: cada ubicación descuenta solo su inventario físico |
| **Alta de cliente** | ✅ Escritura | **Único riesgo real** | Catálogo de clientes compartido por negocio; dos POS pueden dar de alta al mismo cliente offline |
| **Compra** | ❌ Online-only | — | Decisión del usuario: la compra se maneja siempre con conexión (competencia de CENTRAL) |
| **Traspaso** | ❌ Online-only | — | Decisión del usuario: requiere conexión en el momento (CENTRAL online, rol owner/admin) |
| **Alta/edición de producto** | ❌ Online-only | — | Se hace en la compra (online-only); nadie lo pidió offline |
| **Edición de cliente existente** | ❌ Online-only (v1) | — | Evita conflicto de edición concurrente sin beneficio claro en v1 |
| **Consultas** (inventario, saldos, utilidades, reportes, dividendos, coach, historial) | ✅ Solo lectura | — | Reflejan la última sync; RBAC por pestaña igual que hoy |
| **Copiloto IA (chat LLM)** | ❌ Online-only | — | Depende de LLM externo; sin conexión la pestaña queda deshabilitada |

**Rangos de ID para SKU autogenerado:** se reserva un **intervalo de 10.000 productos por
negocio** para la numeración automática de SKU. Los negocios existentes reciben su intervalo;
los nuevos negocios reciben el siguiente. Como la compra (donde se dan de alta productos
nuevos) es online-only, solo hay un escritor por negocio generando esos códigos → sin
colisión ni entre negocios ni dentro del mismo. El formato de SKU actual se mantiene.

---

## 3. Arquitectura

```
┌─────────────────────────────────────────┐        ┌──────────────────────────────────┐
│  APP DE ESCRITORIO (Windows, por nodo)   │        │  SERVIDOR CENTRAL (hoy)          │
│                                          │        │  gestion.legatumbp.com           │
│  pywebview → http://127.0.0.1:PORT       │        │  FastAPI + PostgreSQL            │
│    └─ uvicorn embebido (mismo app)       │        │                                  │
│    └─ SQLAlchemy → SQLite LOCAL cifrado  │        │  ┌────────────────────────────┐  │
│       (SQLCipher, AES-256)               │        │  │ sync router (NUEVO)        │  │
│                                          │  push  │  │  POST /sync/push           │  │
│  Alcance escritura: venta + cliente      │ ─────► │  │   ├─ valida versión app     │  │
│  Alcance lectura:  inventario/saldos/... │        │  │   ├─ idempotencia event_id  │  │
│                                          │  pull  │  │   ├─ replay vía services    │  │
│  ┌────────────────────────────────────┐ │ ◄───── │  │   │   existentes            │  │
│  │ sync module local (NUEVO)          │ │        │  │   ├─ dedup clientes         │  │
│  │  ├─ tabla pending_sync_events      │ │        │  │   └─ arma paquete de lectura│  │
│  │  │   (outbox append-only + hash    │ │        │  └────────────────────────────┘  │
│  │  │    encadenado)                  │ │        │                                  │
│  │  ├─ botón "Sincronizar" (UI)       │ │        │  Backup diario de todas las BD  │
│  │  ├─ last_synced_at (banner)        │ │        │  (ya existe)                     │
│  │  └─ backup local rotativo outbox   │ │        └──────────────────────────────────┘
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

**Reutilización:** el `DATABASE_URL` ya soporta SQLite (es el modo dev actual). La app de
escritorio es el mismo `app/` apuntando a un SQLite local cifrado. No se duplica frontend
ni lógica de negocio.

**Empaquetado (decidido): pywebview.** Ventana nativa liviana que apunta a uvicorn embebido,
100% Python, cero código en otro lenguaje, reutiliza backend y frontend tal cual. Instalador
de Windows (PyInstaller + Inno Setup o equivalente). Se descartó Tauri (requiere Rust/sidecar,
sobra para el alcance v1) y Electron (100MB+, 200-400MB RAM). Nota revisable: si la app de
escritorio crece a producto de primera clase, migrar a Tauri con backend sidecar es la ruta
natural.

---

## 4. Mecánica de sincronización

### 4.1 Push (nodo → servidor)

1. El nodo envía los eventos pendientes del outbox (`VentaRegistrada`, `ClienteCreado`),
   cada uno con un **`event_id` estable generado localmente** (no autoincremental) → reintentar
   un push interrumpido es seguro (el servidor descarta los ya aplicados).
2. El servidor **reaplica cada evento a través de los services existentes**
   (`InventoryService.sale()`, alta de cliente, etc.), **no copia filas crudas** → re-valida el
   stock real del servidor en el momento de aplicar, no el que el nodo creía tener hace días.
3. **Dedup de clientes** (único conflicto real):
   - Si el `client_id` entrante ya existe en el servidor:
     - **Coincide teléfono o email** → es el mismo cliente real: se descarta el duplicado y se
       **remapean** las ventas que el nodo asoció localmente al cliente canónico. Regla de
       desempate: **gana el que se registró primero** (por `created_at`).
     - **No coincide** → dos personas distintas con el mismo `client_id` (recordar: `client_id`
       lo teclea el operador a mano, es único global): el servidor agrega un sufijo al entrante
       y lo marca en una **pantalla de conflictos** para revisión manual del admin.
4. El servidor responde: qué eventos se aplicaron (para vaciar el outbox local), qué remapeos
   aplicar (IDs locales → canónicos) y el paquete de lectura (pull).

### 4.2 Pull (servidor → nodo)

El nodo actualiza sus tablas de solo-lectura y `last_synced_at`. Para v1 se trae una **foto
completa del alcance de lectura** del negocio/ubicación del nodo (no delta fino fila-por-fila):
como esos datos no se editan localmente (solo se leen), es mucho más simple y, con el tamaño de
datos real de estos negocios, no debería pesar. (Delta incremental queda como optimización
futura si el volumen lo justifica.)

---

## 5. Seguridad de la BD local

| Amenaza | Mecanismo |
|---|---|
| **Confidencialidad** — que no cualquiera abra/lea la BD | **SQLCipher** (`sqlcipher3-binary`, instala en Windows sin compilar), cifrado **AES-256** de todo el archivo (datos, índices, journals) vía `sqlite+pysqlcipher://` + `PRAGMA key`. Transparente para SQLAlchemy: no se tocan modelos ni queries |
| **Dónde vive la llave** — el punto que realmente importa | Derivada de un secreto ligado a la instalación vía **Windows DPAPI** (atado a máquina/usuario de Windows), no hardcodeada en el instalador. Copiar el `.db` a otra máquina no sirve sin robar también las credenciales de Windows de ese equipo |
| **Integridad** — que no editen el outbox a mano | Outbox (`pending_sync_events`) **append-only con hash encadenado** (cada evento incluye el hash del anterior). El servidor detecta manipulación al recibir el paquete y rechaza/marca los eventos alterados en vez de aceptarlos ciegamente |
| **Credenciales cacheadas** — login offline sin exponer todo | Se replican **solo los hashes PBKDF2 de los usuarios de ese negocio/ubicación**, no la tabla `users` completa. Robar un POS expone solo esas credenciales, no las de todo el sistema |
| **Pérdida de datos no sincronizados** — el backup del server no cubre lo que el nodo aún no subió | **Backup local rotativo automático del outbox** (carpeta local, sin red) + **aviso en el banner** ("llevas X días sin sincronizar") que empuja a sincronizar seguido y reduce la ventana de exposición |

El backup diario del servidor central (ya existente) cubre la copia canónica una vez
sincronizado; el backup local cubre la ventana offline previa a la sync.

---

## 6. Errores y casos borde

- **Sync se corta a la mitad** (cae la conexión durante el push): cada evento tiene `event_id`
  estable e idempotente → reintentar reenvía solo lo no confirmado, sin duplicar.
- **Venta rechazada al re-validar stock** (el stock cambió en el servidor por otra vía mientras
  el POS estaba offline): **no se resuelve solo** — se muestra en la pantalla de conflictos para
  que un humano decida (es dinero real, no un dato cualquiera).
- **Nodo con versión de app vieja** frente al servidor: el paquete de sync incluye la versión de
  la app; si es demasiado vieja para el esquema de eventos, el servidor **rechaza con un mensaje
  claro** en vez de interpretar mal el esquema.

---

## 7. Banner de última sincronización

Banner persistente en el layout base de la app de escritorio, visible **en cada pestaña**,
leyendo `last_synced_at` local. Muestra la fecha/hora de la última sync y, si han pasado
demasiados días, un aviso destacado. Bajo costo de implementación (una lectura local + un
partial en el layout).

---

## 8. Testing

- **Unit:** replay de eventos (venta válida / venta rechazada por stock), dedup de clientes
  (coincide teléfono-email / no coincide / remapeo de ventas), asignación de rangos de ID por
  negocio, hash encadenado del outbox (detección de manipulación).
- **E2E (obligatorio por CLAUDE.md):** **nueva categoría de infraestructura** — simular **dos
  nodos SQLite locales + un servidor central** en el mismo test, cada uno creando datos offline
  (incluido un conflicto de cliente a propósito), sincronizando ambos, y verificando el estado
  final canónico. Los 20 e2e actuales son single-server; esto es una inversión de infraestructura
  de testing nueva, no solo de la feature.

---

## 9. Alcance, no-alcance y notas

**En alcance (v1):**
- App de escritorio pywebview instalable en Windows (multi-negocio/ubicación por instalación)
- Venta y alta de cliente offline; consultas de solo lectura offline (con RBAC por pestaña)
- Sync bidireccional manual (push idempotente + pull foto completa)
- Dedup de clientes (teléfono/email, gana el primero, pantalla de conflictos)
- Rangos de ID de 10k por negocio
- Cifrado SQLCipher + llave DPAPI + outbox con hash encadenado + credenciales acotadas + backup local
- Banner de última sync

**Fuera de alcance (v1):**
- Compras, traspasos y edición de producto/cliente offline (online-only)
- Copiloto IA / cualquier función LLM offline
- Sincronización automática/programada (v1 es manual bajo demanda)
- Delta incremental fino en el pull (v1 trae foto completa)
- Migración de PKs a UUID / unicidad compuesta de SKU (los rangos de 10k lo evitan en v1)

**Nota de versionado:** por la regla dura del proyecto, todo incremento es **PATCH** por
default (v0.4.6 → v0.4.7), aunque esto sea una entrega grande. MINOR/MAJOR solo si el usuario
lo marca explícitamente como hito de release. Al ser una feature de este tamaño, es un candidato
razonable a que el usuario decida marcarla como hito — pero no se asume.

---

## 10. Siguiente paso

Generar el PAE vía **SDD-Agent** (4 fases: requirements → design → task envelopes → PAE
assembly), con tests e2e en el DoD. Este documento es el insumo de las fases 1-2.
