@echo off
echo ============================================================
echo  Leads Engine - Build .exe
echo ============================================================
echo.

:: Active le venv du projet
call venv\Scripts\activate.bat

:: Installe PyInstaller dans le venv
pip install pyinstaller

:: Nettoie les builds précédents
if exist dist\LeadsEngine rmdir /s /q dist\LeadsEngine
if exist build rmdir /s /q build

:: Build
pyinstaller ^
    --name "LeadsEngine" ^
    --onedir ^
    --noconsole ^
    --noconfirm ^
    --add-data "app.py;." ^
    --add-data "config.py;." ^
    --add-data "export.py;." ^
    --add-data "agents;agents" ^
    --add-data "core;core" ^
    --add-data "services;services" ^
    --collect-all streamlit ^
    --add-data "venv\Lib\site-packages\streamlit-1.55.0.dist-info;streamlit-1.55.0.dist-info" ^
    --collect-all altair ^
    --collect-all pandas ^
    --collect-all openpyxl ^
    --collect-all httpx ^
    --collect-all bs4 ^
    --collect-all lxml ^
    --collect-all dotenv ^
    --collect-all nest_asyncio ^
    --collect-all dns ^
    --collect-all serpapi ^
    --collect-all pydantic ^
    --hidden-import "streamlit.web.cli" ^
    --hidden-import "streamlit.web.bootstrap" ^
    --hidden-import "streamlit.runtime" ^
    --hidden-import "lxml.etree" ^
    --hidden-import "dns.resolver" ^
    --hidden-import "sqlite3" ^
    --hidden-import "email.mime.text" ^
    --hidden-import "email.mime.multipart" ^
    --hidden-import "email.mime.base" ^
    run.py

:: Copie le .env dans le dossier de distribution (clés API)
if exist .env copy .env dist\LeadsEngine\.env

:: Copie version.txt dans le dossier de distribution
if exist version.txt copy version.txt dist\LeadsEngine\version.txt

:: Crée le ZIP de release (sans données utilisateur)
echo.
echo Creation du ZIP de release...
powershell -Command ^
  "$tmp = 'dist\_release_tmp\LeadsEngine'; " ^
  "New-Item -ItemType Directory -Path $tmp -Force | Out-Null; " ^
  "Copy-Item 'dist\LeadsEngine\LeadsEngine.exe' $tmp; " ^
  "Copy-Item 'dist\LeadsEngine\version.txt' $tmp; " ^
  "Copy-Item 'dist\LeadsEngine\.env' $tmp; " ^
  "Copy-Item 'dist\LeadsEngine\_internal' \"$tmp\_internal\" -Recurse; " ^
  "if (Test-Path 'dist\LeadsEngine.zip') { Remove-Item 'dist\LeadsEngine.zip' }; " ^
  "Compress-Archive -Path 'dist\_release_tmp\LeadsEngine' -DestinationPath 'dist\LeadsEngine.zip'; " ^
  "Remove-Item 'dist\_release_tmp' -Recurse -Force"

echo.
echo ============================================================
echo  Build termine : dist\LeadsEngine\
echo  ZIP de release : dist\LeadsEngine.zip
echo  Lance LeadsEngine.exe pour demarrer l'application.
echo ============================================================
pause
