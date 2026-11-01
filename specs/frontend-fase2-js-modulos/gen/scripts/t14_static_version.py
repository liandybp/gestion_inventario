P = "app/routers/ui_common.py"
edit(P, "def _static_asset_version() -> str:", "def _static_asset_version(static_dir: Path | None = None) -> str:")
edit(P, "solo vigila ``*.py``, así que editar ``app.css``/``app.js`` no reinicia el",
     "solo vigila ``*.py``, así que editar un ``.css``/``.js`` no reinicia el")
edit(P, """    digest = hashlib.sha1()
    static_dir = Path(__file__).resolve().parent.parent / "static"
    for rel in ("css/app.css", "js/app.js", "js/htmx.min.js", "js/chart.umd.min.js", "js/JsBarcode.all.min.js"):
        try:
            digest.update((static_dir / rel).read_bytes())
        except OSError:
            continue
    return digest.hexdigest()[:8]""", """    digest = hashlib.sha1()
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
    return digest.hexdigest()[:8]""")
T = "tests/unit/test_ui_regressions.py"
edit(T, "import hashlib\nfrom pathlib import Path\n\n", "")
edit(T, '''def test_static_asset_version_derives_from_file_content():
    """H-060: el hash sale del contenido real de los assets (no una constante)."""
    static_dir = Path(ui_common.__file__).resolve().parent.parent / "static"
    css = (static_dir / "css/app.css").read_bytes()
    js = (static_dir / "js/app.js").read_bytes()
    htmx = (static_dir / "js/htmx.min.js").read_bytes()
    chart = (static_dir / "js/chart.umd.min.js").read_bytes()
    barcode = (static_dir / "js/JsBarcode.all.min.js").read_bytes()
    expected = hashlib.sha1(css + js + htmx + chart + barcode).hexdigest()[:8]
    assert ui_common._static_asset_version() == expected
''', '''def test_static_asset_version_changes_with_any_nested_module(tmp_path):
    """H-060 + fase 2: el hash cambia si cambia CUALQUIER .css/.js, también un módulo anidado
    (los que importa main.js no llevan ``?v=`` propio)."""
    (tmp_path / "css").mkdir()
    (tmp_path / "css" / "app.css").write_text("body{}")
    (tmp_path / "js" / "features").mkdir(parents=True)
    (tmp_path / "js" / "main.js").write_text("import './features/x.js';")
    module = tmp_path / "js" / "features" / "x.js"
    module.write_text("export const a = 1;")
    before = ui_common._static_asset_version(tmp_path)
    assert before == ui_common._static_asset_version(tmp_path)
    module.write_text("export const a = 2;")
    assert ui_common._static_asset_version(tmp_path) != before
    module.rename(tmp_path / "js" / "features" / "y.js")
    assert ui_common._static_asset_version(tmp_path) != before
''')
