# Requirements — App de escritorio offline-first con sincronización bidireccional

- **Feature slug:** `offline-desktop-sync`
- **Proyecto:** gestion_inventario
- **Rama base:** `develop`
- **Versión objetivo:** v0.4.7 (PATCH)
- **Fase SDD:** 1 de 4 (Requirements)
- **Insumo:** `docs/plans/2026-08-28-app-escritorio-offline-sync-design.md` (brainstorming aprobado)
- **Planifica:** Claude Code (Opus 4.8) · **Implementa:** OpenCode + Qwen3.6-35B-A3B

> ⚠️ **Nota de dimensionamiento (crítica — leer antes de Fase 2):** esta feature es un
> **épico**, no un PAE único. Cubre un sistema distribuido (outbox + replay + dedup),
> empaquetado de escritorio (pywebview/PyInstaller/Inno Setup), cifrado en reposo
> (SQLCipher + DPAPI) y una nueva infraestructura de tests e2e multi-nodo. Los requirements
> de este documento enmarcan **todo** el épico; el diseño (Fase 2), los task envelopes
> (Fase 3) y el PAE (Fase 4) se generan **por slice**, no de una sola vez. Ver §7
> (Descomposición en PAEs) — es un punto de decisión que bloquea Fase 2.

---

## 1. Functional Requirements

Formato EARS + Gherkin. Prioridad MoSCoW por REQ (resumen en §2).

### REQ-1: Ejecutar el mismo backend contra SQLite local en modo escritorio
**Priority:** Must Have
**EARS:** WHEN la app se lanza en modo escritorio, THE SYSTEM SHALL arrancar uvicorn
embebido apuntando a una BD SQLite local en lugar del PostgreSQL central, reutilizando
`app/` sin bifurcar la lógica de negocio.

```gherkin
Scenario: Arranque en modo escritorio
  GIVEN la variable de entorno APP_MODE="desktop"
  AND un DATABASE_URL que apunta a un archivo SQLite local
  WHEN el usuario abre la app de escritorio
  THEN uvicorn arranca embebido en 127.0.0.1 en un puerto local
  AND la ventana pywebview carga http://127.0.0.1:<port>
  AND las migraciones inline (migrations.py) se ejecutan sobre el SQLite local

Scenario: El modo servidor no cambia
  GIVEN APP_MODE ausente o "server"
  WHEN la app arranca en el servidor central
  THEN el comportamiento es idéntico al actual (PostgreSQL, sin pywebview)
```

### REQ-2: Empaquetar la app como instalable de Windows
**Priority:** Must Have
**EARS:** WHEN se construye el release de escritorio, THE SYSTEM SHALL producir un
instalador de Windows que incluya el runtime Python, la app y sus dependencias, sin
requerir instalación manual de Python en el equipo destino.

```gherkin
Scenario: Instalación en un equipo Windows limpio
  GIVEN un PC Windows sin Python instalado
  WHEN el usuario ejecuta el instalador
  THEN la app queda instalada con un acceso directo
  AND al abrirla arranca sin dependencias externas faltantes
```

### REQ-3: Cifrar la BD local en reposo (SQLCipher)
**Priority:** Must Have
**EARS:** WHEN la app de escritorio abre su BD local, THE SYSTEM SHALL usar SQLCipher
(AES-256) de modo que el archivo `.db` sea ilegible sin la llave.

```gherkin
Scenario: El archivo cifrado no se puede abrir sin llave
  GIVEN una BD local creada por la app de escritorio
  WHEN se intenta abrir el archivo .db con un lector SQLite estándar sin la llave
  THEN el archivo no se puede leer (cabecera cifrada)

Scenario: La app abre su propia BD
  GIVEN la app de escritorio con su llave provista por el gestor de llaves
  WHEN arranca
  THEN abre la BD con PRAGMA key y opera normalmente
```

### REQ-4: Gestionar la llave de cifrado ligada a la instalación (Windows DPAPI)
**Priority:** Must Have
**EARS:** WHEN la app necesita la llave de la BD, THE SYSTEM SHALL derivarla de un secreto
protegido con Windows DPAPI (atado a máquina/usuario), NUNCA hardcodeada en el instalador.

```gherkin
Scenario: Copiar la BD a otro equipo no la vuelve legible
  GIVEN una BD local cifrada y su secreto DPAPI en el equipo A
  WHEN se copia solo el archivo .db al equipo B
  THEN el equipo B no puede derivar la llave (DPAPI atado al equipo/usuario A)
```

### REQ-5: Registrar ventas offline (POS, su propio stock)
**Priority:** Must Have
**EARS:** WHEN un operador registra una venta sin conexión, THE SYSTEM SHALL descontar el
stock local de su propia ubicación y encolar un evento `VentaRegistrada` en el outbox.

```gherkin
Scenario: Venta offline descuenta stock local y encola evento
  GIVEN la app de escritorio sin conexión, ubicación POS con stock del producto "SKU000123"
  WHEN el operador registra una venta de 2 unidades de "SKU000123"
  THEN el stock local baja en 2 (FIFO local)
  AND se crea un evento VentaRegistrada en pending_sync_events
  AND el evento tiene un event_id estable y el hash del evento anterior

Scenario: No se puede vender más que el stock local
  GIVEN un POS offline con 1 unidad de "SKU000123"
  WHEN el operador intenta vender 3 unidades
  THEN la venta se rechaza con el mismo mensaje que la app online
```

### REQ-6: Alta de cliente offline
**Priority:** Must Have
**EARS:** WHEN un operador da de alta un cliente sin conexión, THE SYSTEM SHALL crearlo en
la BD local y encolar un evento `ClienteCreado` en el outbox.

```gherkin
Scenario: Alta de cliente offline encola evento
  GIVEN la app de escritorio sin conexión
  WHEN el operador crea un cliente con client_id "C-100", teléfono "600111222"
  THEN el cliente existe en la BD local
  AND se crea un evento ClienteCreado en pending_sync_events con teléfono y email
```

### REQ-7: Consultas de solo lectura offline con RBAC por pestaña
**Priority:** Must Have
**EARS:** WHEN un usuario consulta inventario, saldos, utilidades, reportes, dividendos,
coach o historial sin conexión, THE SYSTEM SHALL mostrar los datos de la última
sincronización, respetando el mismo control de acceso por rol que la app online.

```gherkin
Scenario: Operador no ve pestañas restringidas offline
  GIVEN un usuario con rol "operator" en la app de escritorio offline
  WHEN abre el dashboard
  THEN NO ve las pestañas admin-only (ej. Coach) — igual que online

Scenario: Las lecturas reflejan la última sync
  GIVEN una app de escritorio que sincronizó por última vez hace 2 días
  WHEN un admin abre el reporte de utilidades
  THEN ve los datos vigentes a esa última sincronización
```

### REQ-8: Banner de última sincronización en cada pestaña
**Priority:** Must Have
**EARS:** WHEN cualquier pestaña se renderiza en modo escritorio, THE SYSTEM SHALL mostrar
un banner con la fecha/hora de la última sincronización y un aviso si han pasado demasiados días.

```gherkin
Scenario: Banner visible con fecha de última sync
  GIVEN una app de escritorio con last_synced_at = "2026-08-26 10:00"
  WHEN el usuario abre cualquier pestaña
  THEN ve un banner "Última sincronización: 2026-08-26 10:00"

Scenario: Aviso por sync vencida
  GIVEN una app cuyo last_synced_at tiene más de N días
  WHEN el usuario abre cualquier pestaña
  THEN el banner muestra un aviso destacado de sincronizar
```

### REQ-9: Outbox append-only con hash encadenado
**Priority:** Must Have
**EARS:** WHEN se encola cualquier evento offline, THE SYSTEM SHALL asignarle un `event_id`
estable y un hash que encadena el evento anterior, de modo que una manipulación posterior
del outbox sea detectable.

```gherkin
Scenario: Manipular un evento rompe la cadena
  GIVEN 3 eventos encolados en pending_sync_events con hash encadenado
  WHEN se altera el payload del evento 2 directamente en la BD
  THEN la validación de la cadena falla al recalcular el hash del evento 3
```

### REQ-10: Push idempotente de eventos al servidor central
**Priority:** Must Have
**EARS:** WHEN el usuario presiona "Sincronizar" con conexión, THE SYSTEM SHALL enviar los
eventos pendientes al endpoint de sync central, que los aplica de forma idempotente
(descarta los ya aplicados por `event_id`).

```gherkin
Scenario: Reintento de push no duplica
  GIVEN un push que se cortó tras aplicar el evento "E1" en el servidor
  WHEN el nodo reintenta el push con E1 y E2
  THEN el servidor descarta E1 (ya aplicado) y aplica solo E2
  AND no se duplica ninguna venta ni cliente
```

### REQ-11: Replay de eventos vía los services existentes en el servidor
**Priority:** Must Have
**EARS:** WHEN el servidor central recibe un evento de venta, THE SYSTEM SHALL aplicarlo a
través de los services existentes (`InventoryService.sale`, etc.), re-validando el stock
real del servidor, NO copiando filas crudas.

```gherkin
Scenario: Venta re-validada contra el stock del servidor
  GIVEN un evento VentaRegistrada de un POS
  WHEN el servidor lo reaplica vía InventoryService.sale
  THEN descuenta stock según el FIFO real del servidor
  AND si el stock del servidor no alcanza, el evento se marca como conflicto (no se aplica silenciosamente)
```

### REQ-12: Dedup de clientes por teléfono/email (gana el primero)
**Priority:** Must Have
**EARS:** WHEN dos nodos crean un cliente que coincide en teléfono o email, THE SYSTEM SHALL
conservar el registrado primero (por `created_at`), descartar el duplicado y remapear las
ventas del duplicado al cliente canónico.

```gherkin
Scenario: Mismo cliente en dos POS se fusiona
  GIVEN el POS-A creó cliente tel "600111222" el día 1
  AND el POS-B creó cliente tel "600111222" el día 2
  WHEN ambos sincronizan
  THEN el servidor conserva el del POS-A (created_at menor)
  AND remapea las ventas del cliente del POS-B al cliente del POS-A

Scenario: client_id colisiona pero son personas distintas
  GIVEN dos nodos crearon client_id "C-100" con teléfonos distintos y sin email común
  WHEN ambos sincronizan
  THEN el servidor agrega un sufijo al segundo client_id
  AND lo marca en la pantalla de conflictos para revisión manual
```

### REQ-13: Pull de foto completa del alcance de lectura
**Priority:** Must Have
**EARS:** WHEN termina el push, THE SYSTEM SHALL devolver al nodo una foto completa de los
datos de lectura de su negocio/ubicación y actualizar `last_synced_at`.

```gherkin
Scenario: El nodo actualiza sus datos de lectura tras sincronizar
  GIVEN un push exitoso
  WHEN el servidor responde el paquete de pull
  THEN el nodo reemplaza sus tablas de solo-lectura con la foto recibida
  AND actualiza last_synced_at a la hora de la sync
```

### REQ-14: Pantalla de conflictos para revisión manual
**Priority:** Must Have
**EARS:** WHEN el servidor no puede aplicar un evento automáticamente (venta sin stock,
colisión de client_id sin match), THE SYSTEM SHALL registrarlo en una pantalla de conflictos
para que un admin lo resuelva.

```gherkin
Scenario: Conflicto de venta visible para el admin
  GIVEN una venta rechazada por stock insuficiente en el servidor
  WHEN el admin abre la pantalla de conflictos
  THEN ve la venta pendiente con su detalle (producto, cantidad, nodo, fecha)
```

### REQ-15: Rangos de 10.000 IDs de SKU por negocio
**Priority:** Should Have
**EARS:** WHEN se genera un SKU automático, THE SYSTEM SHALL usar el rango de 10.000
reservado para ese negocio, evitando colisión con el constraint único global de `products.sku`.

```gherkin
Scenario: SKU autogenerado cae en el rango del negocio
  GIVEN el negocio "bazar" con rango base 20000
  WHEN se autogenera un SKU nuevo para bazar
  THEN el número queda dentro de [20000, 29999]
  AND no colisiona con el rango de otro negocio
```

### REQ-16: Rechazo por versión de app incompatible
**Priority:** Should Have
**EARS:** WHEN un nodo con una versión de app demasiado vieja sincroniza, THE SYSTEM SHALL
rechazar la sync con un mensaje claro en vez de interpretar mal el esquema de eventos.

```gherkin
Scenario: Nodo con versión vieja recibe error claro
  GIVEN un nodo con versión de esquema de eventos < mínima soportada
  WHEN sincroniza
  THEN el servidor responde un error explícito "versión incompatible, actualice la app"
```

### REQ-17: Credenciales offline acotadas al negocio/ubicación
**Priority:** Must Have
**EARS:** WHEN se aprovisiona un nodo, THE SYSTEM SHALL replicar solo los hashes PBKDF2 de
los usuarios de ese negocio/ubicación, no la tabla `users` completa.

```gherkin
Scenario: Un nodo solo conoce sus propios usuarios
  GIVEN un nodo aprovisionado para el negocio "bazar"
  WHEN se inspecciona su tabla users local
  THEN solo contiene usuarios asociados a "bazar", no de otros negocios
```

### REQ-18: Backup local rotativo del outbox
**Priority:** Should Have
**EARS:** WHEN hay eventos pendientes sin sincronizar, THE SYSTEM SHALL mantener una copia
local rotativa del outbox para reducir la pérdida ante fallo del equipo antes de sincronizar.

```gherkin
Scenario: Copia local del outbox tras cada venta offline
  GIVEN una venta registrada offline
  WHEN se encola el evento
  THEN existe una copia local rotativa del outbox actualizada (sin requerir red)
```

### REQ-19: Compras, traspasos y funciones LLM deshabilitados offline
**Priority:** Must Have
**EARS:** WHEN no hay conexión, THE SYSTEM SHALL deshabilitar (con mensaje claro) las
pestañas de compra, traspaso y copiloto IA, que requieren el servidor central.

```gherkin
Scenario: Compra bloqueada offline
  GIVEN la app de escritorio sin conexión
  WHEN el usuario abre la pestaña de compras
  THEN ve un mensaje "Requiere conexión" y no puede registrar la compra

Scenario: Copiloto bloqueado offline
  GIVEN la app sin conexión
  WHEN el usuario abre el copiloto
  THEN la pestaña indica que requiere conexión (LLM online-only)
```

### REQ-20: Tests e2e multi-nodo
**Priority:** Must Have
**EARS:** WHEN se valida la feature, THE SYSTEM SHALL incluir tests e2e que simulen dos
nodos SQLite locales + un servidor central sincronizando, incluyendo un conflicto de cliente.

```gherkin
Scenario: Dos nodos sincronizan contra el central con un conflicto
  GIVEN dos nodos que crearon datos offline (uno con un cliente en conflicto)
  WHEN ambos sincronizan contra el servidor central
  THEN el estado final canónico es correcto (ventas aplicadas, cliente deduplicado, conflicto marcado)
```

---

## 2. MoSCoW Prioritization

| Prioridad | REQs |
|-----------|------|
| **Must Have** | REQ-1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 19, 20 |
| **Should Have** | REQ-15, 16, 18 |
| **Could Have** | (ninguno en esta iteración) |
| **Won't Have (v1)** | Sync automática/programada; delta incremental fino en el pull; edición offline de producto/cliente existente; migración de PKs a UUID; unicidad compuesta de SKU; app de escritorio para Mac/Linux |

---

## 3. Non-Functional Requirements

| Categoría | Pregunta | Respuesta |
|-----------|----------|-----------|
| **Performance** | ¿Tiempo de sync aceptable? | El pull trae foto completa del alcance de un negocio/ubicación; con el volumen real de estos negocios debe completar en segundos sobre una conexión intermitente. No hay SLA duro; la sync es manual y no bloquea la operación offline |
| **Security** | ¿Nuevos límites de auth / datos sensibles? | Sí: BD local cifrada (SQLCipher/DPAPI, REQ-3/4), credenciales offline acotadas (REQ-17), integridad del outbox (REQ-9), endpoint de sync autenticado en el servidor. La BD local contiene PII de clientes y datos financieros del negocio |
| **Scalability** | ¿Volumen esperado? | Decenas de nodos como máximo (una instalación por ubicación). El servidor central ya soporta la carga; el sync es puntual, no continuo |
| **Observability** | ¿Nuevos logs/métricas? | Sí: cada sync (push/pull) se registra con `X-Request-ID` (patrón existente); los conflictos quedan trazados en la pantalla de conflictos y en logs |
| **Reliability** | ¿Comportamiento ante error? | Push idempotente y reanudable (REQ-10); eventos no aplicables → conflicto explícito, nunca pérdida silenciosa (REQ-11/14); backup local del outbox (REQ-18) |

---

## 4. Out of Scope (v1)

- Sincronización automática o programada (v1 es manual bajo demanda).
- Delta incremental fila-por-fila en el pull (v1 trae foto completa).
- Edición offline de productos y de clientes ya existentes (solo alta de cliente y venta).
- Compras y traspasos offline (online-only por decisión del usuario).
- Cualquier función de LLM/copiloto offline (online-only).
- Migración de PKs enteras a UUID y unicidad compuesta `(business_id, sku)` (los rangos de 10k lo evitan en v1).
- Empaquetado para macOS/Linux (solo Windows en v1).
- Resolución automática de conflictos de venta (van a revisión manual).

---

## 5. Dependencies

- **Librerías nuevas** (requieren aprobación explícita por invariante del proyecto):
  `pywebview`, `pyinstaller`, `sqlcipher3-binary`, y un binding de Windows DPAPI
  (`pywin32` o `cryptography` sobre DPAPI). Ninguna se añade sin confirmación.
- **Servidor central:** debe exponer el nuevo router de sync (mismo repo, `app/routers/`).
- **Modelo de datos:** nueva tabla `pending_sync_events` (local) + tablas/columnas de sync
  en el servidor (a definir en Fase 2 por slice). Migraciones inline (`migrations.py`, schema_version).
- **Constraint existente crítico:** `products.sku` es `unique=True` **global**
  (`ux_products_sku`) — REQ-15 (rangos de 10k) existe precisamente por esto.
- **Instalador:** herramienta de empaquetado Windows (Inno Setup o equivalente) — fuera del
  runtime Python, parte del pipeline de build de escritorio.

---

## 6. Open Questions

1. **Aprovisionamiento inicial del nodo (bootstrap):** ¿cómo recibe un nodo nuevo su primera
   foto de datos + sus credenciales acotadas? ¿Un "primer sync" con conexión obligatoria al
   instalar, o un archivo de aprovisionamiento generado desde el servidor? — **A resolver en
   el slice de sync-cliente.**
2. **Autenticación del endpoint de sync:** ¿token por nodo (registro de dispositivos) o
   reutiliza las sesiones existentes? Impacta la seguridad del push. — **A resolver en el
   slice de sync-backend.**
3. **Rangos de SKU para negocios existentes:** ¿backfill que asigna un rango base a cada
   negocio actual, o solo aplica a negocios nuevos? El usuario pidió asignar rango también a
   los existentes — **a confirmar el mapeo concreto en el slice de fundamentos.**

> Estas preguntas **no bloquean** la descomposición en PAEs (§7), pero **sí** deben
> resolverse al inicio del slice correspondiente, antes de escribir sus envelopes.

---

## 7. Descomposición en PAEs (punto de decisión — bloquea Fase 2)

El épico se implementa como una **secuencia de PAEs**, cada uno generado por SDD-Agent con
su propio `design.md` + `tasks.md` + `PAE-*.md`. Orden propuesto (cada slice es
verificable e incrementa valor):

| # | Slice (PAE) | Alcance | REQs | Por qué en este orden |
|---|-------------|---------|------|----------------------|
| **P1** | **Fundamentos de sync en el modelo** | Tabla `pending_sync_events` (outbox + hash encadenado), `APP_MODE`, rangos de SKU de 10k, banderas de modo escritorio en settings. Sin desktop todavía — corre en el backend actual con SQLite | 1, 9, 15 | Base que todo lo demás usa; testeable con unit sobre el backend actual |
| **P2** | **Backend de sincronización (servidor central)** | Router `/sync/push` + `/sync/pull`, replay vía services, dedup de clientes, validación de versión, pantalla de conflictos | 10, 11, 12, 13, 14, 16 | El servidor debe saber recibir antes de que exista un cliente que envíe |
| **P3** | **Cliente de sync + escritura offline** | Outbox writer en venta y alta de cliente, botón "Sincronizar", `last_synced_at` + banner, deshabilitar compra/traspaso/LLM offline, credenciales acotadas | 5, 6, 7, 8, 17, 18, 19 | Consume P1+P2; cierra el flujo funcional offline |
| **P4** | **Empaquetado de escritorio + cifrado** | pywebview + uvicorn embebido, PyInstaller + instalador Windows, SQLCipher, llave DPAPI | 2, 3, 4 | Envuelve lo ya funcional; es la capa más específica de plataforma (Windows) |
| **P5** | **e2e multi-nodo + endurecimiento** | Infra de test de dos nodos + central, escenarios de conflicto, cierre de auditoría | 20 (+ verificación de todos) | Valida el sistema completo end-to-end |

**Recomendación:** generar el PAE detallado (design → tasks → PAE) **slice por slice**,
empezando por **P1**, y no avanzar a P2 hasta que P1 esté implementado y verde. Así cada PAE
respeta el presupuesto de envelopes del modelo local (batches de 3-5) y cada uno es
desplegable/testeable por separado.

---

## Estado

**Fase 1 (requirements) completa.** Hay 3 preguntas abiertas (§6) que se resuelven al inicio
de su slice, no ahora. El punto que bloquea Fase 2 es la **decisión de descomposición** (§7):
confirmar el enfoque por slices y cuál se detalla primero.
