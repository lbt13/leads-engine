"""
core/caller.py — Appels sortants via Twilio ou Telnyx (call bridge).
Le provider appelle d'abord l'utilisateur, puis connecte au lead.
"""

from core.user_config import load as load_user_config
from core.blacklist import is_blacklisted


def _clean_phone(raw: str) -> str:
    cleaned = raw.replace(" ", "").replace(".", "").replace("-", "")
    if not cleaned.startswith("+"):
        if cleaned.startswith("0"):
            cleaned = "+33" + cleaned[1:]
        else:
            cleaned = "+33" + cleaned
    return cleaned


# ── Twilio ──────────────────────────────────────────────────────────────

def is_twilio_configured() -> bool:
    cfg = load_user_config()
    return bool(
        cfg.get("twilio_account_sid", "").strip()
        and cfg.get("twilio_auth_token", "").strip()
        and cfg.get("twilio_phone_number", "").strip()
        and cfg.get("user_phone", "").strip()
    )


def make_call_twilio(to: str) -> tuple[bool, str]:
    if is_blacklisted(to):
        return False, "Numéro sur liste d'opposition — appel non lancé"

    cfg = load_user_config()
    account_sid = cfg.get("twilio_account_sid", "").strip()
    auth_token = cfg.get("twilio_auth_token", "").strip()
    twilio_number = cfg.get("twilio_phone_number", "").strip()
    user_phone = cfg.get("user_phone", "").strip()

    if not all([account_sid, auth_token, twilio_number, user_phone]):
        return False, "Twilio non configuré — va dans Configuration"

    try:
        from twilio.rest import Client
    except ImportError:
        return False, "Module twilio non installé — pip install twilio"

    to_clean = _clean_phone(to)
    user_clean = _clean_phone(user_phone)

    try:
        client = Client(account_sid, auth_token)
        twiml = f'<Response><Dial callerId="{twilio_number}">{to_clean}</Dial></Response>'
        call = client.calls.create(
            to=user_clean,
            from_=twilio_number,
            twiml=twiml,
        )
        return True, f"Appel lancé — ton téléphone va sonner (SID: {call.sid})"
    except Exception as e:
        err = str(e)
        if "authenticate" in err.lower() or "credentials" in err.lower():
            return False, "Échec d'authentification Twilio — vérifie ton Account SID et Auth Token"
        if "not a valid phone number" in err.lower():
            return False, f"Numéro invalide — vérifie le format (+33...)"
        return False, f"Erreur Twilio : {err}"


# ── Telnyx ──────────────────────────────────────────────────────────────

def is_telnyx_configured() -> bool:
    cfg = load_user_config()
    return bool(
        cfg.get("telnyx_api_key", "").strip()
        and cfg.get("telnyx_connection_id", "").strip()
        and cfg.get("telnyx_phone_number", "").strip()
        and cfg.get("user_phone", "").strip()
    )


def make_call_telnyx(to: str) -> tuple[bool, str]:
    if is_blacklisted(to):
        return False, "Numéro sur liste d'opposition — appel non lancé"

    cfg = load_user_config()
    api_key = cfg.get("telnyx_api_key", "").strip()
    connection_id = cfg.get("telnyx_connection_id", "").strip()
    telnyx_number = cfg.get("telnyx_phone_number", "").strip()
    user_phone = cfg.get("user_phone", "").strip()

    if not all([api_key, connection_id, telnyx_number, user_phone]):
        return False, "Telnyx non configuré — va dans Configuration"

    try:
        import telnyx
    except ImportError:
        return False, "Module telnyx non installé — pip install telnyx"

    telnyx.api_key = api_key

    to_clean = _clean_phone(to)
    user_clean = _clean_phone(user_phone)

    try:
        call = telnyx.Call.create(
            connection_id=connection_id,
            to=user_clean,
            from_=telnyx_number,
        )
        call.speak(
            payload=f"Connexion avec {to_clean} en cours.",
            language="fr-FR",
            voice="female",
        )
        call.transfer(to=to_clean)
        return True, f"Appel lancé — ton téléphone va sonner (ID: {call.call_control_id})"
    except Exception as e:
        err = str(e)
        if "authent" in err.lower() or "credentials" in err.lower() or "api_key" in err.lower():
            return False, "Échec d'authentification Telnyx — vérifie ta clé API"
        if "not a valid phone number" in err.lower() or "invalid" in err.lower():
            return False, f"Numéro invalide — vérifie le format (+33...)"
        return False, f"Erreur Telnyx : {err}"
