# Traspaso — Release 0.5.0 (frontend modular) · Fase 1

## Decisiones tomadas (2026-10-23, sesión de diagnóstico, Opus)

- Diagnóstico completo: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (plan de fases §4).
- **Release único 0.5.0** para fases 0-5 (pedido explícito del usuario, MINOR): varios commits, **ninguno sube
  versión**; `VERSION`/`.bumpversion.cfg`/`CHANGELOG` se bumpean una sola vez al cerrar. Los PAEs de cada fase
  **no** llevan envelope de bump. `CHANGELOG.md` acumula bajo `## 0.5.0 (sin publicar …)`.
- Rama del release: **`feature/frontend-modular`** (desde `develop` `0eebbec`), sin push.
  - `bc07d88` fase 0: XSS de Inicio (`tojson`) + test + informe. Entra en 0.5.0 (decisión del usuario: no
    publicarlo antes como PATCH).
  - `a3b7a23` brief de la fase 1.
- Cada fase sigue el loop `~/obsidian-lab/04-Harness/loop-desarrollo-agentes.md`, en orden (cada una construye
  sobre la anterior).
- Fase 1 añade un **arnés de snapshots del DOM** (`tests/snapshots/ui_snapshot.py`) como red de seguridad para
  todo el release: refactor de plantillas ⇒ DOM equivalente. Decidir en el PAE si se commitea (recomendado: sí,
  ejecutable aparte, fuera de la suite).
- Working tree del repo real: `CLAUDE.md`, `.claude/CLAUDE.md`, `.idea`, `.serena/`,
  `specs/copiloto-ventas-ubicacion-mensual/` tienen cambios **de la sesión del 2026-10-18**, sin commitear y
  ajenos a este release: no mezclarlos en sus commits.

## Estado

- Paso 3 (prototipo) **lanzado**: `cc_local_agent /tmp/proto-fase1 /tmp/proto-fase1/brief.md > /tmp/proto-fase1/r1.json`
  (clon en `/tmp/proto-fase1/repo`, rama `feature/frontend-modular`).

## Siguiente etapa: paso 4 — verificación (sesión nueva, Sonnet)

1. `pgrep -af cc_local` — confirmar que terminó; `jq -r .result /tmp/proto-fase1/r1.json`.
2. Leer **solo** `/tmp/proto-fase1/informe.md` + `git -C /tmp/proto-fase1/repo diff --stat feature/frontend-modular`.
3. Comprobar uno mismo 2-3 hechos críticos:
   - H9 (arnés determinista) y el `diff -r snap-base snap-fase1` — **re-ejecutarlo**, no fiarse del informe.
   - Unit + e2e en verde en el clon (387 / 44).
   - Que no hay ficheros fuera: `find ~ -maxdepth 2 -newer /tmp/proto-fase1/brief.md -not -path '*/.cache/*'`.
   - Prueba de punta a punta en el runtime real: levantar el clon con uvicorn en un puerto libre contra una BD
     SQLite temporal, abrir un modal de edición, guardar y cancelar (cierre por `data-modal-close` y por clic
     en overlay), y abrir login e impresión de traspaso.
4. Si algo no cuadra: seguimiento con `cc_local_agent /tmp/proto-fase1 pregunta.md "$(jq -r .session_id /tmp/proto-fase1/r1.json)"`.
5. Después: paso 5 (PAE vía `SDD-Agent`, fases 1-2 en Opus, 3-4 en Sonnet; generador desde el prototipo +
   simulación por cc_local) → gate de aprobación del usuario.

## Prompt para pegar en la sesión nueva

```
Proyecto ~/PyCharmMiscProject/gestion_inventario, rama feature/frontend-modular. Release 0.5.0 (frontend
modular), fase 1. Lee docs/prototipos/fase1-layouts-macros/traspaso.md y ejecuta el paso 4 (verificación del
prototipo de cc_local en /tmp/proto-fase1) tal como está descrito. No subas versión en ningún commit.
```
