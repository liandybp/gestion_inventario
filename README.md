# Gestión de Inventario (FastAPI + HTMX)

Aplicación web para gestionar inventario, compras, ventas, documentos (facturas/presupuestos) y clientes.

Versión actual: `0.1.6` (ver `VERSION`).

## Características

- **Productos**
  - Alta/edición/búsqueda.
  - Unidad de medida, coste/venta por defecto.
  - Imagen (opcional) y archivos en `app/static/uploads`.
- **Inventario**
  - Movimientos y control de stock (lotes + FIFO).
  - Edición de producto desde la pestaña Inventario.
  - Ajuste de **stock actual** desde el modal de edición (genera movimiento `adjustment` para trazabilidad).
  - Opción **Inventario inicial** al ajustar stock para que ese lote se consuma primero en FIFO.
  - Tabla de inventario con:
    - **Costo**: mínimo histórico de compras.
    - **Precio venta**: precio de venta por defecto.
- **Ventas**
  - Venta por escáner y venta manual.
- **Documentos (Factura / Presupuesto)**
  - Carrito temporal en sesión.
  - Vista previa.
  - Emisión y descarga/impresión en PDF.
  - Edición de documentos emitidos.
  - Eliminación de documentos emitidos.
  - **Importante:** emitir factura/presupuesto **no registra** una venta ni movimiento de inventario.
- **Clientes**
  - Alta/edición/listado.
  - Ficha de cliente con métricas y documentos.
  - Eliminación de cliente (bloqueada si tiene documentos asociados).
- **Autenticación por sesión**
  - Roles `admin` y `operator`.

## Stack

- FastAPI
- SQLAlchemy
- Jinja2
- HTMX
- Base de datos:
  - Por defecto: **SQLite** (archivo `inventario.db`)
  - Producción: **PostgreSQL** (vía `DATABASE_URL`)

## Requisitos

- Python 3.12+

## Instalación y ejecución (local)

1) Crear y activar un entorno virtual.

2) Instalar dependencias:

```bash
pip install -r requirements.txt
```

3) Ejecutar:

```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 10000 --reload
```

La app quedará disponible en:

- `http://127.0.0.1:10000/ui`

## Acceso (usuarios)

En el arranque, la app asegura usuarios por variables de entorno:

- `ADMIN_USERNAME` (por defecto: `admin`)
- `ADMIN_PASSWORD` (por defecto: `admin`)
- `OPERATOR_USERNAME` (por defecto: `operator`)
- `OPERATOR_PASSWORD` (por defecto: `operator`)

## Variables de entorno

- **Base de datos**
  - `DATABASE_URL`
    - Por defecto: `sqlite+pysqlite:///./inventario.db`
    - Ejemplo PostgreSQL: `postgresql+psycopg://user:pass@host:5432/dbname`

- **Sesión**
  - `SESSION_SECRET` (recomendado en producción)
  - `SESSION_HTTPS_ONLY`
    - `"1"` para cookies solo HTTPS

- **Usuarios/roles**
  - `ADMIN_USERNAME`, `ADMIN_PASSWORD`
  - `OPERATOR_USERNAME`, `OPERATOR_PASSWORD`

## Versionado (SemVer) con bump2version

Este proyecto usa tags tipo `vMAJOR.MINOR.PATCH` (ej: `v0.1.0`) y un archivo `VERSION`.

## Changelog

- Ver `CHANGELOG.md`.
- Nota: si ves cambios en `Unreleased`, significa que hay commits posteriores al último tag.

1) Instala dependencias de desarrollo:

```bash
pip install -r requirements-dev.txt
```

2) Incrementa versión (esto crea **commit** y **tag** automáticamente):

```bash
bump2version patch  # 0.1.0 -> 0.1.1
bump2version minor  # 0.1.0 -> 0.2.0
bump2version major  # 0.1.0 -> 1.0.0
```

3) Sube commits y tags a GitHub:

```bash
git push origin main
git push origin --tags
```

## Configuración del negocio (`app/business_config.conf`)

La app usa un archivo `.conf` (INI) para parámetros de negocio:

- `[issuer]`: datos del emisor (nombre, NIF/CIF, dirección, etc.)
- `[currency]`: moneda (`code`, `symbol`)
- `[sales_documents]`:
  - `default_type` (`F` o `P`)
  - `enabled_types` (ej. `F,P`)
  - `invoice_label`, `quote_label`

Edita el archivo:

- `app/business_config.conf`

y reinicia la app para aplicar cambios.

## Ejecución con Docker Compose (PostgreSQL + App + Caddy)

Este repo incluye `docker-compose.yml` y `Dockerfile`.

1) Ajusta variables en `docker-compose.yml` (contraseñas y `SESSION_SECRET`).

2) Levanta servicios:

```bash
docker compose up --build
```

Servicios:

- `db`: PostgreSQL
- `app`: FastAPI en `:10000` (interno)
- `caddy`: expone `80/443` (ver `Caddyfile`)

Uploads persistentes:

- volumen `uploads_data` → `/app/app/static/uploads`

## Notas sobre base de datos y migraciones

- La app ejecuta tareas de inicialización en el arranque (`app/main.py`):
  - `Base.metadata.create_all`
  - Algunos `ALTER TABLE`/índices para compatibilidad (principalmente en SQLite y PostgreSQL).

## Guía rápida de uso

- **Ventas**:
  - Realiza ventas por escáner o manual.
- **Inventario**:
  - Ajusta el **stock actual** desde “Inventario → Edit” (crea un ajuste en el historial).
  - Marca **Inventario inicial** si estás migrando stock existente y quieres que se consuma primero en FIFO.
- **Documentos**:
  - Agrega artículos al carrito temporal.
  - Usa **Vista previa**.
  - Emite **Factura** o **Presupuesto**.
  - Descarga PDF.
  - Edita o elimina documentos emitidos desde “Documentos recientes”.
- **Clientes**:
  - Crea/edita clientes.
  - Consulta su historial y métricas.

## Estructura del proyecto (resumen)

- `app/main.py`: app FastAPI + middleware + startup DB
- `app/db.py`: conexión y sesión DB
- `app/models.py`: modelos SQLAlchemy
- `app/routers/`: routers API/UI
- `app/templates/`: templates Jinja2 (parciales HTMX)
- `app/static/`: assets y uploads

## Licencia

Pendiente (define aquí la licencia si aplica).
