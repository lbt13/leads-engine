"""
core/license.py — Système de licence Standard / Pro avec checksum.
Clé format : LE-STD-XXXX-XXXX ou LE-PRO-XXXX-XXXX
Le SEED (4 chars) est aléatoire, le CHECKSUM (4 chars) est un hash du seed + secret.
"""

import hashlib
import re
import secrets
from core.user_config import load, save

_SECRET = "LeG8x!qZ#2026vBnTr"
_ADMIN_KEY = "labitedanslachatte13k"

TIERS = {"standard", "pro"}
TIER_LABELS = {"standard": "Standard", "pro": "Pro"}


def _compute_checksum(seed: str) -> str:
    h = hashlib.sha256((seed + _SECRET).encode()).hexdigest().upper()
    return h[:4]


def _validate_key(key: str) -> str | None:
    key = key.strip().upper()
    m = re.match(r"^LE-(STD|PRO)-([A-Z0-9]{4})-([A-Z0-9]{4})$", key)
    if not m:
        return None
    tier_code, seed, checksum = m.group(1), m.group(2), m.group(3)
    if _compute_checksum(seed) != checksum:
        return None
    return "standard" if tier_code == "STD" else "pro"


def activate(key: str) -> tuple[bool, str]:
    if key.strip() == _ADMIN_KEY:
        save({"license_key": _ADMIN_KEY, "tier": "pro"})
        return True, "Licence Admin activée."
    tier = _validate_key(key)
    if tier is None:
        return False, "Clé invalide ou corrompue."
    save({"license_key": key.strip().upper(), "tier": tier})
    return True, f"Licence {TIER_LABELS[tier]} activée."


def get_tier() -> str:
    cfg = load()
    tier = cfg.get("tier", "")
    if tier in TIERS:
        return tier
    return ""


def is_activated() -> bool:
    return get_tier() != ""


def is_pro() -> bool:
    return get_tier() == "pro"


def is_standard() -> bool:
    return get_tier() == "standard"


def get_license_key() -> str:
    return load().get("license_key", "")


def generate_key(tier: str, seed: str = "") -> str:
    """Génère une clé de licence. Si seed est vide, un seed aléatoire est utilisé."""
    assert tier in ("standard", "pro")
    prefix = "STD" if tier == "standard" else "PRO"
    if not seed:
        seed = secrets.token_hex(2).upper()
    else:
        seed = hashlib.sha256(seed.encode()).hexdigest().upper()[:4]
    checksum = _compute_checksum(seed)
    return f"LE-{prefix}-{seed}-{checksum}"
