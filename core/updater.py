"""
core/updater.py — Auto-update depuis GitHub Releases.
Vérifie si une nouvelle version existe, propose le téléchargement
et remplace les fichiers via un script batch.
"""

import os
import sys
import subprocess
from pathlib import Path

import httpx

from core.logger import get_logger

log = get_logger("updater")

GITHUB_REPO = "lbt13/leads-engine"
API_LATEST = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_version(v: str) -> tuple:
    """'10.0.0' → (10, 0, 0)"""
    try:
        return tuple(int(x) for x in v.strip().lstrip("vV").split("."))
    except Exception:
        return (0, 0, 0)


def get_local_version(root: Path) -> str:
    vf = root / "version.txt"
    if vf.exists():
        return vf.read_text(encoding="utf-8").strip()
    return "0.0.0"


# ── Check ────────────────────────────────────────────────────────────────────

def check_update(root: Path) -> dict | None:
    """
    Interroge GitHub Releases pour voir si une version plus récente existe.
    Retourne un dict {version, download_url, size, changelog} ou None si à jour.
    """
    try:
        r = httpx.get(API_LATEST, timeout=10, follow_redirects=True)
        if r.status_code != 200:
            log.debug("GitHub API → %s", r.status_code)
            return None

        data = r.json()
        remote = data.get("tag_name", "").lstrip("vV")
        local = get_local_version(root)

        if _parse_version(remote) <= _parse_version(local):
            return None

        download_url = None
        size = 0
        for asset in data.get("assets", []):
            if asset["name"].lower().endswith(".zip"):
                download_url = asset["browser_download_url"]
                size = asset.get("size", 0)
                break

        if not download_url:
            log.warning("Release %s trouvée mais aucun asset .zip", remote)
            return None

        return {
            "version": remote,
            "download_url": download_url,
            "size": size,
            "changelog": data.get("body", ""),
        }
    except Exception as e:
        log.debug("Update check échoué : %s", e)
        return None


# ── Download & Install ───────────────────────────────────────────────────────

def download_and_install(root: Path, update_info: dict,
                         progress_callback=None) -> bool:
    """
    Télécharge le ZIP et prépare le script batch de mise à jour.
    Ne lance PAS le script — appeler launch_update_and_quit() ensuite.
    """
    tmp_zip = root / "_update.zip"
    new_version = update_info["version"]

    try:
        with httpx.stream("GET", update_info["download_url"],
                          timeout=300, follow_redirects=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            with open(tmp_zip, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=65_536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback and total:
                        progress_callback(downloaded / total)

        # Script batch de mise à jour
        bat = root / "_update.bat"
        bat.write_text(
            f'@echo off\r\n'
            f'chcp 65001 >nul\r\n'
            f'title Leads Engine — Mise a jour\r\n'
            f'color 0A\r\n'
            f'echo.\r\n'
            f'echo  ============================================================\r\n'
            f'echo   Leads Engine — Mise a jour vers v{new_version}\r\n'
            f'echo  ============================================================\r\n'
            f'echo.\r\n'
            f'\r\n'
            f'echo  [1/5] Fermeture de l\'application...\r\n'
            f'taskkill /IM LeadsEngine.exe /F >nul 2>&1\r\n'
            f'timeout /t 3 /nobreak >nul\r\n'
            f'\r\n'
            f'echo  [2/5] Sauvegarde des donnees utilisateur...\r\n'
            f'if exist ".env" copy /Y ".env" "_update_env.bak" >nul\r\n'
            f'if exist "user_config.json" copy /Y "user_config.json" "_update_cfg.bak" >nul\r\n'
            f'if exist "leads.db" copy /Y "leads.db" "_update_db.bak" >nul\r\n'
            f'\r\n'
            f'echo  [3/5] Extraction de la mise a jour...\r\n'
            f'powershell -Command "Expand-Archive -Path \'_update.zip\' -DestinationPath \'_update_tmp\' -Force"\r\n'
            f'if exist "_internal" rmdir /s /q "_internal"\r\n'
            f'xcopy /E /Y /Q "_update_tmp\\LeadsEngine\\*" "." >nul\r\n'
            f'\r\n'
            f'echo  [4/5] Restauration des donnees utilisateur...\r\n'
            f'if exist "_update_env.bak" copy /Y "_update_env.bak" ".env" >nul\r\n'
            f'if exist "_update_cfg.bak" copy /Y "_update_cfg.bak" "user_config.json" >nul\r\n'
            f'if exist "_update_db.bak" copy /Y "_update_db.bak" "leads.db" >nul\r\n'
            f'\r\n'
            f'echo  [5/5] Nettoyage...\r\n'
            f'del "_update_env.bak" 2>nul\r\n'
            f'del "_update_cfg.bak" 2>nul\r\n'
            f'del "_update_db.bak" 2>nul\r\n'
            f'rmdir /s /q "_update_tmp" 2>nul\r\n'
            f'del "_update.zip" 2>nul\r\n'
            f'\r\n'
            f'echo.\r\n'
            f'echo  ============================================================\r\n'
            f'echo   Mise a jour v{new_version} terminee !\r\n'
            f'echo  ============================================================\r\n'
            f'echo.\r\n'
            f'echo  Redemarrage dans 3 secondes...\r\n'
            f'timeout /t 3 /nobreak >nul\r\n'
            f'\r\n'
            f'start "" "LeadsEngine.exe"\r\n'
            f'(goto) 2>nul & del "%~f0"\r\n',
            encoding="utf-8",
        )

        log.info("Mise à jour v%s téléchargée, script prêt", new_version)
        return True

    except Exception as e:
        log.error("Échec téléchargement/installation : %s", e, exc_info=True)
        if tmp_zip.exists():
            tmp_zip.unlink()
        return False


def launch_update_and_quit(root: Path):
    """Lance le script .bat de mise à jour et quitte tout proprement."""
    bat = root / "_update.bat"
    if not bat.exists():
        return

    # Lance le .bat dans une console visible
    subprocess.Popen(
        ["cmd", "/c", str(bat)],
        cwd=str(root),
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )

    # Force la fermeture de tout : Streamlit + navigateur + process Python
    # taskkill sur notre propre PID ferme tout le sous-arbre de processus
    pid = os.getpid()
    subprocess.Popen(
        ["taskkill", "/F", "/PID", str(pid), "/T"],
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
