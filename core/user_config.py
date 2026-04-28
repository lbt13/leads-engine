"""
core/user_config.py — Configuration utilisateur persistante.
Stockée dans user_config.json à côté de leads.db.
"""

import json
from pathlib import Path


_DEFAULT = {
    "crm": "default",         # default | hubspot | pipedrive | salesforce | notion | monday | airtable
    "setup_done": False,       # True une fois que l'écran de config a été validé
}

_config_cache: dict | None = None


def _path() -> Path:
    import os
    root = Path(os.environ.get("LEADS_ENGINE_ROOT", str(Path(__file__).parent.parent)))
    return root / "user_config.json"


def load() -> dict:
    global _config_cache
    if _config_cache is not None:
        return _config_cache
    p = _path()
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            _config_cache = {**_DEFAULT, **data}
            return _config_cache
        except Exception:
            pass
    _config_cache = dict(_DEFAULT)
    return _config_cache


def save(data: dict):
    global _config_cache
    current = load()
    current.update(data)
    _config_cache = current
    _path().write_text(json.dumps(current, indent=2, ensure_ascii=False), encoding="utf-8")


def get(key: str, default=None):
    return load().get(key, default)


def set_crm(crm: str):
    save({"crm": crm, "setup_done": True})
