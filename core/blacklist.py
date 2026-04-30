"""
core/blacklist.py — Liste d'opposition (droit d'opposition RGPD).
Stocke les emails/domaines des personnes ayant demandé à ne plus être contactées.
"""

import json
from pathlib import Path


def _path() -> Path:
    import os
    root = Path(os.environ.get("LEADS_ENGINE_ROOT", str(Path(__file__).parent.parent)))
    return root / "blacklist.json"


def _load() -> dict:
    p = _path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"emails": [], "domains": [], "companies": []}


def _save(data: dict):
    _path().write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def is_blacklisted(email: str) -> bool:
    if not email:
        return False
    email = email.strip().lower()
    data = _load()
    if email in [e.lower() for e in data.get("emails", [])]:
        return True
    domain = email.split("@")[-1] if "@" in email else ""
    if domain and domain in [d.lower() for d in data.get("domains", [])]:
        return True
    return False


def is_company_blacklisted(company_name: str) -> bool:
    if not company_name:
        return False
    company_name = company_name.strip().lower()
    data = _load()
    return company_name in [c.lower() for c in data.get("companies", [])]


def add_email(email: str):
    email = email.strip().lower()
    if not email:
        return
    data = _load()
    if email not in [e.lower() for e in data["emails"]]:
        data["emails"].append(email)
        _save(data)


def add_domain(domain: str):
    domain = domain.strip().lower()
    if not domain:
        return
    data = _load()
    if domain not in [d.lower() for d in data["domains"]]:
        data["domains"].append(domain)
        _save(data)


def add_company(company_name: str):
    company_name = company_name.strip().lower()
    if not company_name:
        return
    data = _load()
    if company_name not in [c.lower() for c in data["companies"]]:
        data["companies"].append(company_name)
        _save(data)


def remove_email(email: str):
    email = email.strip().lower()
    data = _load()
    data["emails"] = [e for e in data["emails"] if e.lower() != email]
    _save(data)


def remove_domain(domain: str):
    domain = domain.strip().lower()
    data = _load()
    data["domains"] = [d for d in data["domains"] if d.lower() != domain]
    _save(data)


def remove_company(company_name: str):
    company_name = company_name.strip().lower()
    data = _load()
    data["companies"] = [c for c in data["companies"] if c.lower() != company_name]
    _save(data)


def list_all() -> dict:
    return _load()


def count() -> int:
    data = _load()
    return len(data.get("emails", [])) + len(data.get("domains", [])) + len(data.get("companies", []))
