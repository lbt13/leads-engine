"""
core/updater.py — Auto-update depuis GitHub Releases.
Vérifie au lancement si une nouvelle version existe, propose le
téléchargement et remplace les fichiers via un script batch.
"""

import os
import sys
import subprocess
from pathlib import Path

import httpx

from core.logger import get_logger

log = get_logger("updater")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_version(v: str) -> tuple:
    """'8.0.0' → (8, 0, 0)"""
    try:
        return tuple(int(x) for x in v.strip().lstrip("v").split("."))
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
    Retourne un dict {version, asset_url, size} ou None.
    Ne fait rien en mode développement.
    """
    if not getattr(sys, "frozen", False):
        return None

    repo = os.environ.get("GITHUB_REPO", "")
    token = os.environ.get("GITHUB_TOKEN", "")
    if not repo:
        return None

    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        r = httpx.get(
            f"https://api.github.com/repos/{repo}/releases/latest",
            headers=headers,
            timeout=10,
        )
        if r.status_code != 200:
            log.debug(f"GitHub API → {r.status_code}")
            return None

        data = r.json()
        remote = data.get("tag_name", "").lstrip("v")
        local = get_local_version(root)

        if _parse_version(remote) <= _parse_version(local):
            return None

        for asset in data.get("assets", []):
            if asset["name"].endswith(".zip"):
                return {
                    "version": remote,
                    "asset_url": asset["url"],
                    "browser_url": asset["browser_download_url"],
                    "size": asset["size"],
                }

        log.warning("Release trouvée mais aucun asset .zip")
        return None
    except Exception as e:
        log.debug(f"Update check échoué : {e}")
        return None


# ── Download & Install ───────────────────────────────────────────────────────

def download_and_install(root: Path, update_info: dict,
                         progress_callback=None) -> bool:
    """
    Télécharge le ZIP, écrit un script batch qui remplace les fichiers
    après fermeture de l'app, puis relance l'exe.
    """
    token = os.environ.get("GITHUB_TOKEN", "")

    headers = {"Accept": "application/octet-stream"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    tmp_zip = root / "_update.zip"
    new_version = update_info["version"]

    try:
        with httpx.stream("GET", update_info["asset_url"],
                          headers=headers, timeout=300,
                          follow_redirects=True) as r:
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
            f'echo.\r\n'
            f'echo  Mise a jour Leads Engine v{new_version}...\r\n'
            f'echo.\r\n'
            f'\r\n'
            f':: Attend que l\'exe se ferme\r\n'
            f'taskkill /IM LeadsEngine.exe /F >nul 2>&1\r\n'
            f'timeout /t 3 /nobreak >nul\r\n'
            f'\r\n'
            f':: Extrait le ZIP\r\n'
            f'powershell -Command "Expand-Archive -Path \'_update.zip\' -DestinationPath \'_update_tmp\' -Force"\r\n'
            f'\r\n'
            f':: Supprime l\'ancien _internal\r\n'
            f'if exist "_internal" rmdir /s /q "_internal"\r\n'
            f'\r\n'
            f':: Copie les nouveaux fichiers\r\n'
            f'xcopy /E /Y /Q "_update_tmp\\LeadsEngine\\*" "." >nul\r\n'
            f'\r\n'
            f':: Nettoyage\r\n'
            f'rmdir /s /q "_update_tmp" 2>nul\r\n'
            f'del "_update.zip" 2>nul\r\n'
            f'\r\n'
            f'echo.\r\n'
            f'echo  Mise a jour terminee ! Redemarrage...\r\n'
            f'timeout /t 2 /nobreak >nul\r\n'
            f'\r\n'
            f':: Relance l\'application\r\n'
            f'start "" "LeadsEngine.exe"\r\n'
            f'\r\n'
            f':: Supprime ce script\r\n'
            f'(goto) 2>nul & del "%~f0"\r\n',
            encoding="utf-8",
        )

        # Lance le batch en arrière-plan
        subprocess.Popen(
            ["cmd", "/c", str(bat)],
            cwd=str(root),
            creationflags=subprocess.CREATE_NO_WINDOW,
        )

        log.info(f"Script de mise à jour lancé pour v{new_version}")
        return True

    except Exception as e:
        log.error(f"Échec téléchargement/installation : {e}", exc_info=True)
        if tmp_zip.exists():
            tmp_zip.unlink()
        return False
