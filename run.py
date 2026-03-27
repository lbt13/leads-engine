"""
run.py — Point d'entrée de l'application Leads Engine.
Fonctionne en développement (python run.py) et en .exe (PyInstaller).
"""

import sys
import os
import threading
import webbrowser
import time


def resource_path(relative: str) -> str:
    """Chemin absolu vers une ressource — fonctionne en dev et en .exe PyInstaller."""
    base = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))
    return os.path.join(base, relative)


def _open_browser():
    time.sleep(2.5)
    webbrowser.open("http://localhost:8501")


if __name__ == "__main__":
    if getattr(sys, "frozen", False):
        # ── Mode exe (PyInstaller) ──────────────────────────────────────────────
        # Données dans le dossier racine de LeadsEngine (à côté de l'exe).
        _data_root = os.path.dirname(sys.executable)
        _browsers  = os.path.join(_data_root, "playwright_browsers")
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = _browsers
    else:
        # ── Mode dev (python run.py) ────────────────────────────────────────────
        _data_root = os.path.abspath(os.path.dirname(__file__))

    # Transmis à app.py — fiable même quand Streamlit réexécute le script.
    os.environ["LEADS_ENGINE_ROOT"] = _data_root

    os.environ["STREAMLIT_SERVER_HEADLESS"]            = "true"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"]   = "none"

    threading.Thread(target=_open_browser, daemon=True).start()

    from streamlit.web import cli as stcli
    sys.argv = [
        "streamlit", "run", resource_path("app.py"),
        "--server.headless=true",
        "--global.developmentMode=false",
        "--browser.gatherUsageStats=false",
        "--server.fileWatcherType=none",
    ]
    sys.exit(stcli.main())
