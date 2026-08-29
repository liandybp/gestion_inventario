; Inno Setup installer para Inventario Desktop
; Genera el instalador .exe que instala la aplicación en Windows.
; Compilar (Inno Setup IDE o ISCC.exe) DESPUÉS de generar dist/ con PyInstaller
; (ver packaging/README-desktop.md).
;
; H-P4-005 (auditoría 2026-08-29, docs/auditorias/2026-08-29-offline-desktop-sync-epic-full-audit.md):
; la versión original usaba sintaxis inválida de Inno Setup en casi todas las secciones —
; {app} (constante de tiempo de INSTALACIÓN) usada en OutputDir/SourceDir (que Inno Setup
; resuelve en tiempo de COMPILACIÓN), una directiva Filesource que no existe, una sección
; [Shortcut] que no existe (la real es [Icons]), Cmd=/WorkingDir= con sintaxis de [Setup] en
; una sección que requiere Name:/Filename: con dos puntos, {cm} como constante de directorio
; (no existe), y File: en vez de Filename: en [Run] — no hubiera compilado. Reescrito contra
; la sintaxis real y documentada de Inno Setup. Sigue sin poder validarse en este entorno
; Linux (Inno Setup no está instalado aquí) — requiere una compilación real en Windows.

[Setup]
AppName=Inventario Gestión Empresarial
AppVersion=0.4.8
AppPublisher=Legatum BP
AppSupportURL=https://gitea.legatumbp.com/liandybp/gestion_inventario
DefaultDirName={localappdata}\Inventario
DefaultGroupName=Inventario
UninstallDisplayName=Inventario Gestión Empresarial
OutputDir=.
OutputBaseFilename=gestion_inventario_setup
; SetupIconFile requiere packaging/icons/app.ico — no existe todavía en el repo, no
; inventamos el asset acá. Descomentar cuando exista:
; SetupIconFile=icons\app.ico
PrivilegesRequired=lowest
DisableProgramGroupPage=yes

; Copia el contenido de dist/gestion_inventario_desktop/ (generado por PyInstaller,
; ver packaging/desktop.spec) hacia el directorio de instalación.
[Files]
Source: "..\dist\gestion_inventario_desktop\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs
Source: "..\README.md"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Inventario Desktop"; Filename: "{app}\gestion_inventario_desktop.exe"; WorkingDir: "{app}"
Name: "{autodesktop}\Inventario Desktop"; Filename: "{app}\gestion_inventario_desktop.exe"; WorkingDir: "{app}"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Crear un acceso directo en el escritorio"; GroupDescription: "Accesos directos adicionales"

; Inno Setup genera y registra el desinstalador automáticamente (unins000.exe) a partir
; de [Setup]/[Files]/[Icons] — no hace falta declarar [Registry]/[UninstallDelete] a mano.

[Run]
Filename: "{app}\gestion_inventario_desktop.exe"; Description: "Iniciar Inventario"; Flags: nowait postinstall skipifsilent
