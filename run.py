"""
run.py — Point d'entrée de l'application Leads Engine.
Fonctionne en développement (python run.py) et en .exe (PyInstaller).
Ouvre l'app dans une fenêtre native (Edge/Chrome --app) au lieu du navigateur.
"""

import sys
import os
import socket
import threading
import subprocess
import time


def _kill_port(port: int):
    """Si le port est déjà occupé, tue le processus qui le bloque."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(("127.0.0.1", port))
        sock.close()
        if result == 0:  # Port occupé
            out = subprocess.check_output(
                ["cmd", "/c", f"netstat -ano | findstr :{port}"],
                text=True, stderr=subprocess.DEVNULL,
            )
            for line in out.strip().splitlines():
                parts = line.split()
                if "LISTENING" in parts:
                    pid = parts[-1]
                    subprocess.run(
                        ["taskkill", "/F", "/PID", pid],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
            time.sleep(1)
    except Exception:
        pass


def resource_path(relative: str) -> str:
    """Chemin absolu vers une ressource — fonctionne en dev et en .exe PyInstaller."""
    base = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))
    return os.path.join(base, relative)


def _open_app_window():
    """Ouvre Streamlit dans une fenêtre native via Edge ou Chrome en mode --app."""
    time.sleep(3)
    url = "http://localhost:8501"

    # Chemins possibles pour Edge et Chrome sur Windows
    browsers = [
        os.path.expandvars(r"%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%LocalAppData%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]

    for browser_path in browsers:
        if os.path.isfile(browser_path):
            try:
                subprocess.Popen([
                    browser_path,
                    f"--app={url}",
                    "--new-window",
                    "--window-size=1400,900",
                ])
                return
            except Exception:
                continue

    # Fallback : ouvrir dans le navigateur par défaut
    import webbrowser
    webbrowser.open(url)


if __name__ == "__main__":
    if getattr(sys, "frozen", False):
        # ── Mode exe (PyInstaller) ──────────────────────────────────────────────
        _data_root = os.path.dirname(sys.executable)
    else:
        # ── Mode dev (python run.py) ────────────────────────────────────────────
        _data_root = os.path.abspath(os.path.dirname(__file__))

    os.environ["LEADS_ENGINE_ROOT"] = _data_root

    os.environ["STREAMLIT_SERVER_HEADLESS"]            = "true"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"]   = "none"
    os.environ["STREAMLIT_BROWSER_SERVER_ADDRESS"]     = "localhost"

    _kill_port(8501)

    try:
        threading.Thread(target=_open_app_window, daemon=True).start()

        from streamlit.web import cli as stcli
        sys.argv = [
            "streamlit", "run", resource_path("app.py"),
            "--server.headless=true",
            "--global.developmentMode=false",
            "--browser.gatherUsageStats=false",
            "--server.fileWatcherType=none",
            "--server.port=8501",
        ]
        sys.exit(stcli.main())
    except Exception as _e:
        import traceback
        _crash = os.path.join(_data_root, "crash.log")
        with open(_crash, "w", encoding="utf-8") as _f:
            _f.write(f"Leads Engine crash — {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            traceback.print_exc(file=_f)
        print("\n\n=== ERREUR AU LANCEMENT ===")
        traceback.print_exc()
        print(f"\nDétails sauvegardés dans : {_crash}")
        input("\nAppuie sur Entrée pour fermer...")
        sys.exit(1)
