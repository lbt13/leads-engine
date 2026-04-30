"""
core/license.py — Validation de licence Standard / Pro.
Clé format : LE-STD-XXXX-XXXX ou LE-PRO-XXXX-XXXX
"""

import hashlib
import re
from base64 import b64decode
from core.user_config import load, save

_K = [b"TGVHOHg=", b"IXFaIzI=", b"MDI2dkI=", b"blRy"]
_SECRET = b"".join(b64decode(k) for k in _K).decode()

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
