from __future__ import annotations

# Fuente única de verdad para el conjunto de tablas que viajan en un pull y su
# orden de dependencia FK (padres antes que hijos). Server (SyncService.build_pull,
# app/sync/service.py) y cliente (apply_pull, app/sync/client.py) deben mantenerse
# sincronizados: agregar una tabla nueva a un lado sin el otro debe fallar fuerte
# en vez de romperse en silencio (ver H-CR-3, code-review 2026-08-29).
SYNC_TABLE_ORDER: tuple[str, ...] = (
    "businesses",
    "locations",
    "products",
    "customers",
    "users",
    "sales_documents",
    "inventory_movements",
    "inventory_lots",
    "movement_allocations",
    "sales_document_items",
    "money_extractions",
    "operating_expenses",
    "cash_baselines",
)
