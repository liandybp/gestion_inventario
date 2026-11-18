Proyecto gestion_inventario (/home/liandy/PyCharmMiscProject/gestion_inventario, rama develop, HEAD ae8fda8).

Fases 2b-4a del PAE pdf-documentos-venta-oferta: Envelopes + Assembly (SIN lanzamiento: la 4a va en otra sesión tras la aprobación del usuario).
Las Fases 1-2a ya están hechas por Opus-1 y pasaron cc_design_check (exit 0) y la revisión de Codex (12 hallazgos incorporados).
Lee completos specs/pdf-documentos-venta-oferta/requirements.md y design.md POR FICHERO antes de nada (incluida la tabla "Hallazgos de Codex incorporados" y las sondas de design/sondas/).
No reescribas requirements.md ni design.md. Si encuentras una contradicción con el código, detente y repórtala.
Usa la skill SDD-Agent (Fases 3-4) y su plantilla ~/obsidian-lab/Templates/template-sdd-agent.md.

**Fase 2b — Envelopes + Assembly** (specs/pdf-documentos-venta-oferta/tasks.md + PAE-pdf-documentos-venta-oferta.md):
- Genera los envelopes siguiendo los ficheros y el orden de design.md, en formato Apply: cc_local o Apply: script.
  - cc_local: decisión + before/after completo en prosa, ≤800 tokens (target), máx 2 ficheros. Firmas leídas del código real.
  - script: fichero nuevo o cambio mecánico con bloques pae-edit (DSL en ~/harness-loop/docs/pae-edit-format.md).
- Contrato del Lote 0: el primer envelope del Lote A es Apply: script con "Contrato Lote 0: `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`" (flujo de design.md §2.8: login, Negocios → Editar, moneda GEN, E2E-SKU1 1×303875 + E2E-SKU2 1×187000, tipo P, vencimiento 2099-12-31, PDF con pdfplumber (aserciones de design.md §2.8); segundo test con F sin "Fecha de vencimiento"). Patrón de login y fixture live_server: tests/e2e/test_sale_payment_method_e2e.py y tests/e2e/conftest.py.
- Cambios de la 2ª ronda del usuario ya integrados en requirements.md/design.md (precio unitario + subtotal, iconos SVG de teléfono y email, fecha dd/mm/aaaa, moneda elegida al construir el documento): síguelos tal cual.
- En el lote que toca plantillas, incluye la regeneración de baselines: python3 tests/snapshots/ui_snapshot.py write tests/snapshots/baseline (lección de 0.5.1: el CI cayó por no regenerarlas).
- Lote final: VERSION y .bumpversion.cfg 0.5.1 → 0.5.2 (PATCH, nunca MINOR), CHANGELOG.md, CLAUDE.md raíz (Estado actual). El cambio de .claude/CLAUDE.md va en un envelope marcado "lo aplica Claude Code en la revisión" (cc_local no puede editar .claude/).
- Prosa ASD-STE100 (imperativo, actor explícito, un término por concepto, alcance con ficheros).
- Cada envelope con Files: completos (incluidos tests y cualquier fichero que nombren sus instrucciones).
- La sección ## Ejecución del PAE usa los comandos reales (no inventes):
  - unit: SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest tests/unit -q --override-ini="addopts="
  - snapshots: .venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
  - e2e: SESSION_SECRET=test DATABASE_URL="sqlite+pysqlite:///:memory:" ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest tests/e2e -m e2e -q --override-ini="addopts="
  - Lote 0: el mismo comando e2e limitado a tests/e2e/test_sales_doc_pdf_oferta_e2e.py
  Comprueba en tests/e2e/conftest.py y .gitea/workflows/deploy.yml qué variables necesita de verdad el e2e y si hace falta --free-port.
- Disciplina de sesión: escribe cada lote completo con un solo Write/Edit, nunca con cat > por Bash ni envelope a envelope; no releas tasks.md entero (Grep o Read con offset/limit); valida cada lote con una sola ejecución de cc_pae_check al terminarlo.
- Tramo: todos. Si te acercas al tope de turnos, escribe specs/pdf-documentos-venta-oferta/traspaso-2b.md y para.

Al terminar la 2b, PARA: responde con el resumen del PAE (lotes, nº de envelopes, script vs cc_local, resultado de cc_pae_check) y espera. No crees ramas ni lances cc_pae_loop.
