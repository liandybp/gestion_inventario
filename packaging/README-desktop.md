# Manual de Build Desktop (Windows)

Este documento describe cómo generar el instalador `.exe` para la aplicación
de escritorio. **Sólo se ejecuta en Windows** - los pasos de CI en Linux no
validan estos comandos.

## Requisitos previos

Instalar las dependencias de desktop:

```bash
pip install -r requirements-desktop.txt
```

Las dependencias incluyen:
- `pywebview` - para la ventana GUI
- `pyinstaller` - para empaquetar el ejecutable
- `sqlcipher3-binary` - binario de SQLCipher para cifrado
- `pywin32` - para DPAPI (protección de clave)

## Pasos de build

### 1. Generar el ejecutable con PyInstaller

```bash
pyinstaller --spec-file packaging/desktop.spec
```

Esto crea la carpeta `dist/` con el ejecutable y todos los recursos incluidos.

### 2. Generar el instalador con Inno Setup

Abrir `packaging/installer.iss` en Inno Setup y compilar. Esto genera
`gestion_inventario_setup.exe` en la carpeta `packaging/`.

### 3. Validación manual (Checklist REQ-2/REQ-3/REQ-4)

- [ ] **REQ-2**: Ejecutar el instalador en una máquina Windows limpia
- [ ] **REQ-3**: Verificar que el `.db` cifrado no se abra con SQLite Std sin la llave
- [ ] **REQ-4**: Copiar el `.db` a otro equipo - debe ser ilegible sin la clave DPAPI
- [ ] La aplicación arranca y el dashboard cargado correctamente
- [ ] Ventas/altas de cliente funcionan en modo offline (encolan eventos)
- [ ] Botón "Sincronizar" funciona y push/pull con servidor central

### 4. Estructura resultante

```
dist/
  gestion_inventario_desktop.exe  (ejecutable PyInstaller)
  (recursos incluidos: templates, static, app/desktop)

packaging/
  desktop.spec          (especificación PyInstaller)
  installer.iss         (script Inno Setup)
  README-desktop.md     (este documento)
  icons/                (iconos de la aplicación)
```

## Modo desarrollo (sin empaquetar)

Para probar el modo desktop sin build:

```bash
APP_MODE=desktop python3 -m app.desktop.launcher
```

Esto inicia uvicorn en hilo separado y abre la ventana pywebview contra
http://127.0.0.1:10000.

## Modo server (invariante)

El modo server (`APP_MODE=server`) permanece intacto. No se empaquetan
recursos desktop y la aplicación funciona como hoy en el servidor central.

## Solución de problemas

- Si `sqlcipher3-binary` no se encuentra, verifica la instalación:
  `pip install sqlcipher3-binary`
- Si pywebview falla, asegúrate de tener los backends necesarios:
  - Windows: `pywebview[qt]` o `pywebview[gtk]`
  - macOS: `pywebview[cocoa]`
- Si DPAPI falla (no Windows), el keyring usa fallback base64