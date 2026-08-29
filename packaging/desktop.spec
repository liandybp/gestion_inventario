# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec para la aplicación desktop.
# Build (en Windows, ver packaging/README-desktop.md):
#   pyinstaller --spec-file packaging/desktop.spec
#
# H-P4-004 (auditoría 2026-08-29, docs/auditorias/2026-08-29-offline-desktop-sync-epic-full-audit.md):
# la versión original usaba parámetros y atributos que no existen en la API real de
# PyInstaller (options={"pyexe": {...}}, a.purecludedata, una segunda Analysis() con kwargs
# inventados, EXE(coll=..., atrf=..., one_file=...)) y apuntaba a app/main.py (el servidor
# headless, sin ventana) en vez de app/desktop/launcher.py (el entrypoint real que abre
# pywebview) — nunca hubiera compilado. Reescrito contra la API real, documentada, de
# PyInstaller (Analysis/PYZ/EXE/COLLECT en modo onedir). Sigue sin poder validarse en este
# entorno Linux (PyInstaller no está instalado aquí) — requiere una build real en Windows.

a = Analysis(
    ['app/desktop/launcher.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('app/templates', 'app/templates'),
        ('app/static', 'app/static'),
    ],
    hiddenimports=[
        'fastapi',
        'uvicorn',
        'sqlalchemy',
        'jinja2',
        'httpx',
        'pywebview',
        # pywebview selecciona su backend de GUI en Windows (edgechromium/mshtml/cef)
        # vía import dinámico en tiempo de ejecución, algo que el análisis estático de
        # PyInstaller no detecta por sí solo — sin esto, el .exe puede compilar pero
        # fallar en runtime con "no suitable web engine found" (code-review 2026-08-29,
        # gotcha documentado de PyInstaller+pywebview). Confirmar contra el backend
        # real disponible en la máquina de build.
        'webview.platforms.edgechromium',
        'webview.platforms.mshtml',
        'webview.platforms.winforms',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'unittest',
        'doctest',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='gestion_inventario_desktop',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name='gestion_inventario_desktop',
)
