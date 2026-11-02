from __future__ import annotations

import hashlib
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import HTTPException, UploadFile
from fastapi.templating import Jinja2Templates
from jinja2.exceptions import TemplateRuntimeError

from app.settings import DEV_ACTIONS_ENABLED, UPLOAD_MAX_MB
from app.utils import month_range

templates = Jinja2Templates(directory="app/templates")


def _static_asset_version(static_dir: Path | None = None) -> str:
    """Versión de cache-busting derivada del contenido de los assets estáticos.

    Se calcula en CADA render (no se congela en import): ``uvicorn --reload``
    solo vigila ``*.py``, así que editar un ``.css``/``.js`` no reinicia el
    proceso; con un hash por render, el ``?v=`` cambia igual y el navegador/CDN
    invalida la caché. Las rutas parten de ``__file__`` para no depender del CWD.
    """
    digest = hashlib.sha1()
    root = static_dir or Path(__file__).resolve().parent.parent / "static"
    # Todos los .css/.js (también los módulos de js/core, js/components...): el hash cambia
    # si cambia cualquiera, aunque main.js no cambie. La ruta entra en el hash (renombrar
    # un módulo también invalida la caché).
    files = sorted(p for sub in ("css", "js") for p in (root / sub).rglob("*") if p.suffix in (".css", ".js"))
    for path in files:
        try:
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(path.read_bytes())
        except OSError:
            continue
    return digest.hexdigest()[:8]


templates.env.globals["static_version"] = _static_asset_version  # callable: se invoca en cada render


def _template_error(message: str) -> str:
    """Hace fallar el render desde una plantilla (Jinja no tiene `raise`).

    Para parámetros de macros/plantillas que solo admiten unos valores (`kind`, `heading`):
    un valor fuera de la lista debe ser un error visible, no un modal con título vacío o un
    id que el JS no encuentra (H-308).
    """
    raise TemplateRuntimeError(message)


templates.env.globals["template_error"] = _template_error

_DEV_ACTIONS_ENABLED = DEV_ACTIONS_ENABLED

_UPLOAD_DIR = Path("app/static/uploads")


MAX_IMAGE_SIZE_MB = UPLOAD_MAX_MB
MAX_IMAGE_SIZE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024


def save_product_image(image_file: UploadFile) -> str:
    if image_file is None:
        raise HTTPException(status_code=422, detail="image_file is required")
    if not image_file.filename:
        raise HTTPException(status_code=422, detail="image_file is required")

    content_type = (image_file.content_type or "").lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=422, detail="Invalid image type")

    _UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    ext = Path(image_file.filename).suffix.lower()
    if not ext or len(ext) > 10:
        ext = ".img"

    # Validate file size
    image_file.file.seek(0, 2)
    size = image_file.file.tell()
    image_file.file.seek(0)
    if size > MAX_IMAGE_SIZE_BYTES:
        raise HTTPException(
            status_code=422,
            detail=f"La imagen excede el tamaño máximo de {MAX_IMAGE_SIZE_MB} MB",
        )

    filename = f"{uuid4().hex}{ext}"
    out_path = _UPLOAD_DIR / filename
    with out_path.open("wb") as f:
        shutil.copyfileobj(image_file.file, f)

    return f"/static/uploads/{filename}"


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def dt_to_local_input(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M")




def parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    s = s.replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        value = float(s)
    except ValueError:
        return None
    # "nan"/"inf" son floats válidos para Python: se tratan como cualquier texto no numérico.
    return value if math.isfinite(value) else None


def parse_typed_float(value: Optional[str], error_detail: str) -> Optional[float]:
    """Como `parse_optional_float`, pero un valor tecleado inutilizable es un 422.

    `parse_optional_float` devuelve None tanto para un campo vacío como para texto no numérico
    (incluidos "nan"/"inf"); en un precio eso haría que se usara el valor por defecto sin avisar.
    """
    parsed = parse_optional_float(value)
    if parsed is None and (value or "").strip():
        raise HTTPException(status_code=422, detail=error_detail)
    return parsed


def delete_uploaded_image(image_url: Optional[str]) -> None:
    """Borra una imagen subida por `save_product_image` (p. ej. si la operación falló después)."""
    if not image_url or not image_url.startswith("/static/uploads/"):
        return
    try:
        (_UPLOAD_DIR / Path(image_url).name).unlink(missing_ok=True)
    except OSError:
        pass


