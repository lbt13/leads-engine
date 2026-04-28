"""
core/crm_push.py — Push direct de leads vers les CRM via API.
Supporte : HubSpot, Pipedrive, Salesforce.
"""

import os
import httpx
import pandas as pd
from urllib.parse import urlencode, unquote
from core.user_config import load as load_user_config, save as save_user_config


# ── HubSpot OAuth config ─────────────────────────────────────────────���──────
HUBSPOT_CLIENT_ID = "51ca37af-24ed-4727-9608-cfb58462e719"
HUBSPOT_CLIENT_SECRET = "0857cd81-cbb6-4b78-a7e1-d282ebe165a9"
HUBSPOT_REDIRECT_URI = "https://example.com/callback"
HUBSPOT_SCOPES = "oauth crm.objects.contacts.write crm.objects.contacts.read"


def hubspot_auth_url() -> str:
    params = {
        "client_id": HUBSPOT_CLIENT_ID,
        "redirect_uri": HUBSPOT_REDIRECT_URI,
        "scope": HUBSPOT_SCOPES,
        "response_type": "code",
    }
    return f"https://app.hubspot.com/oauth/authorize?{urlencode(params)}"


def hubspot_exchange_code(code: str) -> tuple[bool, str]:
    code = unquote(code.strip())
    try:
        r = httpx.post(
            "https://api.hubapi.com/oauth/v1/token",
            data={
                "grant_type": "authorization_code",
                "client_id": HUBSPOT_CLIENT_ID,
                "client_secret": HUBSPOT_CLIENT_SECRET,
                "redirect_uri": HUBSPOT_REDIRECT_URI,
                "code": code,
            },
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            save_user_config({
                "hubspot_token": data["access_token"],
                "hubspot_refresh_token": data.get("refresh_token", ""),
            })
            return True, "Connexion HubSpot réussie !"
        return False, f"Erreur {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def _hubspot_refresh() -> str | None:
    cfg = load_user_config()
    refresh = cfg.get("hubspot_refresh_token", "")
    if not refresh:
        return None
    try:
        r = httpx.post(
            "https://api.hubapi.com/oauth/v1/token",
            data={
                "grant_type": "refresh_token",
                "client_id": HUBSPOT_CLIENT_ID,
                "client_secret": HUBSPOT_CLIENT_SECRET,
                "refresh_token": refresh,
            },
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            save_user_config({
                "hubspot_token": data["access_token"],
                "hubspot_refresh_token": data.get("refresh_token", refresh),
            })
            return data["access_token"]
    except Exception:
        pass
    return None


def _get_hubspot_token() -> str:
    cfg = load_user_config()
    token = cfg.get("hubspot_token", "")
    if not token:
        return ""
    try:
        r = httpx.get(
            "https://api.hubapi.com/crm/v3/objects/contacts?limit=1",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if r.status_code == 401:
            return _hubspot_refresh() or ""
    except Exception:
        pass
    return token


# ── Salesforce OAuth config ──────────────────────────────────────────────────
SF_CLIENT_ID = os.environ.get("SF_CLIENT_ID", "")
SF_CLIENT_SECRET = os.environ.get("SF_CLIENT_SECRET", "")
SF_REDIRECT_URI = os.environ.get("SF_REDIRECT_URI", "https://example.com/callback")
SF_SCOPES = "api refresh_token"


def salesforce_auth_url() -> str:
    params = {
        "client_id": SF_CLIENT_ID,
        "redirect_uri": SF_REDIRECT_URI,
        "response_type": "code",
        "scope": SF_SCOPES,
    }
    return f"https://login.salesforce.com/services/oauth2/authorize?{urlencode(params)}"


def salesforce_exchange_code(code: str) -> tuple[bool, str]:
    code = unquote(code.strip())
    try:
        r = httpx.post(
            "https://login.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "client_id": SF_CLIENT_ID,
                "client_secret": SF_CLIENT_SECRET,
                "redirect_uri": SF_REDIRECT_URI,
                "code": code,
            },
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            save_user_config({
                "sf_access_token": data["access_token"],
                "sf_refresh_token": data.get("refresh_token", ""),
                "sf_instance_url": data["instance_url"],
            })
            return True, "Connexion Salesforce réussie !"
        return False, f"Erreur {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def _salesforce_refresh() -> tuple[str, str] | None:
    cfg = load_user_config()
    refresh = cfg.get("sf_refresh_token", "")
    if not refresh:
        return None
    try:
        r = httpx.post(
            "https://login.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "client_id": SF_CLIENT_ID,
                "client_secret": SF_CLIENT_SECRET,
                "refresh_token": refresh,
            },
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            new_token = data["access_token"]
            instance = data.get("instance_url", cfg.get("sf_instance_url", ""))
            save_user_config({
                "sf_access_token": new_token,
                "sf_instance_url": instance,
            })
            return new_token, instance
    except Exception:
        pass
    return None


def _get_salesforce_token() -> tuple[str, str]:
    cfg = load_user_config()
    token = cfg.get("sf_access_token", "")
    instance = cfg.get("sf_instance_url", "")
    if not token or not instance:
        return "", ""
    try:
        r = httpx.get(
            f"{instance}/services/data/v59.0/limits",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if r.status_code == 401:
            result = _salesforce_refresh()
            if result:
                return result
            return "", ""
    except Exception:
        pass
    return token, instance


PUSH_CAPABLE_CRMS = {
    "hubspot": {
        "label": "HubSpot",
        "auth_type": "oauth",
        "auth_label": "Connexion OAuth",
        "guide": (
            "1. Clique sur 'Connecter HubSpot' ci-dessous\n"
            "2. Autorise l'accès sur la page HubSpot\n"
            "3. Copie le code affiché sur la page de redirection\n"
            "4. Colle-le dans le champ ci-dessous"
        ),
        "config_key": "hubspot_token",
    },
    "pipedrive": {
        "label": "Pipedrive",
        "auth_type": "api_key",
        "auth_label": "Clé API",
        "guide": (
            "1. Connecte-toi à Pipedrive\n"
            "2. Clique sur ton avatar (en bas à gauche) → Préférences personnelles\n"
            "3. Onglet 'API' (ou va sur app.pipedrive.com/settings/api)\n"
            "4. Copie ta clé API personnelle\n"
            "5. Colle-la ci-dessous"
        ),
        "config_key": "pipedrive_api_key",
    },
    "salesforce": {
        "label": "Salesforce",
        "auth_type": "oauth",
        "auth_label": "Connexion OAuth",
        "guide": (
            "1. Clique sur 'Connecter Salesforce' ci-dessous\n"
            "2. Connecte-toi à Salesforce et autorise l'accès\n"
            "3. Copie le code affiché dans la barre d'adresse (après `code=`)\n"
            "4. Colle-le dans le champ ci-dessous"
        ),
        "config_key": "sf_access_token",
    },
}


def _get_crm_creds(crm_key: str) -> dict:
    cfg = load_user_config()
    info = PUSH_CAPABLE_CRMS[crm_key]
    creds = {info["config_key"]: cfg.get(info["config_key"], "")}
    for f in info.get("extra_fields", []):
        creds[f] = cfg.get(f, "")
    return creds


def is_connected(crm_key: str) -> bool:
    if crm_key not in PUSH_CAPABLE_CRMS:
        return False
    creds = _get_crm_creds(crm_key)
    main_key = PUSH_CAPABLE_CRMS[crm_key]["config_key"]
    return bool(creds.get(main_key, "").strip())


def test_connection(crm_key: str) -> tuple[bool, str]:
    if crm_key == "hubspot":
        return _test_hubspot()
    if crm_key == "pipedrive":
        return _test_pipedrive()
    if crm_key == "salesforce":
        return _test_salesforce()
    return False, "CRM non supporté pour le push API"


def push_leads(crm_key: str, df: pd.DataFrame, progress_cb=None) -> tuple[int, int, list[str]]:
    """
    Push les leads du DataFrame vers le CRM.
    Retourne (succès, échecs, liste_erreurs).
    """
    if crm_key == "hubspot":
        return _push_hubspot(df, progress_cb)
    if crm_key == "pipedrive":
        return _push_pipedrive(df, progress_cb)
    if crm_key == "salesforce":
        return _push_salesforce(df, progress_cb)
    return 0, 0, ["CRM non supporté"]


# ── HubSpot ──────────────────────────────────────────────────────────────────

def _test_hubspot() -> tuple[bool, str]:
    token = _get_hubspot_token()
    if not token:
        return False, "Token manquant ou expiré — reconnecte HubSpot"
    try:
        r = httpx.get(
            "https://api.hubapi.com/crm/v3/objects/contacts?limit=1",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if r.status_code == 200:
            return True, "Connexion OK"
        return False, f"Erreur {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def _build_hubspot_notes(row: pd.Series) -> str:
    extra = [
        ("google_rating", "Note Google"), ("review_count", "Avis Google"),
        ("siren", "SIREN"), ("siret", "SIRET"), ("legal_form", "Forme juridique"),
        ("creation_date", "Date création"), ("cms", "CMS"), ("hosting", "Hébergeur"),
        ("pagespeed_mobile", "Vitesse mobile"), ("seo_score", "Score SEO"),
        ("seo_weaknesses", "Faiblesses SEO"), ("naf_label", "Activité NAF"),
        ("domain_age", "Âge domaine"), ("has_google_ads", "Google Ads"),
    ]
    parts = []
    for col, label in extra:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() not in ("", "0", "None"):
            if col == "has_google_ads":
                val = "Oui" if str(val) in ("1", "True", "true") else "Non"
            parts.append(f"{label}: {val}")
    return " | ".join(parts)


def _hubspot_existing(token: str) -> tuple[set[str], set[str]]:
    """Récupère emails et noms d'entreprises existants dans HubSpot."""
    emails = set()
    companies = set()
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.hubapi.com/crm/v3/objects/contacts?limit=100&properties=email,company"
    while url:
        try:
            r = httpx.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                break
            data = r.json()
            for contact in data.get("results", []):
                props = contact.get("properties", {})
                email = (props.get("email") or "").lower().strip()
                company = (props.get("company") or "").lower().strip()
                if email:
                    emails.add(email)
                if company:
                    companies.add(company)
            url = data.get("paging", {}).get("next", {}).get("link")
        except Exception:
            break
    return emails, companies


def _push_hubspot(df: pd.DataFrame, progress_cb=None) -> tuple[int, int, list[str]]:
    token = _get_hubspot_token()
    if not token:
        return 0, len(df), ["Token HubSpot expiré — reconnecte dans Configuration"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    ok, fail, skipped, errors = 0, 0, 0, []
    total = len(df)

    existing_emails, existing_companies = _hubspot_existing(token)

    for i, (_, row) in enumerate(df.iterrows()):
        email = str(row.get("email", "")).lower().strip() if pd.notna(row.get("email")) else ""
        company = str(row.get("company_name", "")).lower().strip() if pd.notna(row.get("company_name")) else ""
        if (email and email in existing_emails) or (not email and company and company in existing_companies):
            skipped += 1
            if progress_cb:
                progress_cb((i + 1) / total)
            continue

        props = {}
        if pd.notna(row.get("company_name")):
            props["company"] = str(row["company_name"])
        if pd.notna(row.get("owner_name")):
            names = str(row["owner_name"]).split(maxsplit=1)
            props["firstname"] = names[0]
            if len(names) > 1:
                props["lastname"] = names[1]
            else:
                props["lastname"] = names[0]
        if email:
            props["email"] = email
        if pd.notna(row.get("phone")):
            props["phone"] = str(row["phone"])
        if pd.notna(row.get("website_url")):
            props["website"] = str(row["website_url"])
        if pd.notna(row.get("address")):
            props["address"] = str(row["address"])
        if pd.notna(row.get("city")):
            props["city"] = str(row["city"])
        if pd.notna(row.get("siege_cp")):
            props["zip"] = str(row["siege_cp"])
        if pd.notna(row.get("sector")):
            props["industry"] = str(row["sector"])
        if pd.notna(row.get("owner_role")):
            props["jobtitle"] = str(row["owner_role"])

        notes = _build_hubspot_notes(row)
        if notes:
            props["message"] = notes

        try:
            r = httpx.post(
                "https://api.hubapi.com/crm/v3/objects/contacts",
                headers=headers, json={"properties": props}, timeout=15,
            )
            if r.status_code in (200, 201):
                ok += 1
                if email:
                    existing_emails.add(email)
                if company:
                    existing_companies.add(company)
            elif r.status_code == 409:
                skipped += 1
            else:
                fail += 1
                errors.append(f"{row.get('company_name', '?')}: {r.status_code} {r.text[:100]}")
        except Exception as e:
            fail += 1
            errors.append(f"{row.get('company_name', '?')}: {e}")

        if progress_cb:
            progress_cb((i + 1) / total)

    if skipped > 0:
        errors.insert(0, f"{skipped} leads ignorés (déjà présents dans le CRM)")
    return ok, fail, errors


# ── Pipedrive ────────────────────────────────────────────────────────────────

def _test_pipedrive() -> tuple[bool, str]:
    creds = _get_crm_creds("pipedrive")
    key = creds.get("pipedrive_api_key", "")
    if not key:
        return False, "Clé API manquante"
    try:
        r = httpx.get(
            f"https://api.pipedrive.com/v1/users/me?api_token={key}",
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                name = data.get("data", {}).get("name", "")
                return True, f"Connexion OK — {name}"
        return False, f"Erreur {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def _build_notes_text(row: pd.Series) -> str:
    extra = [
        ("google_rating", "Note Google"), ("review_count", "Avis Google"),
        ("sector", "Secteur"), ("siren", "SIREN"), ("siret", "SIRET"),
        ("employee_range", "Effectif"), ("creation_date", "Date création"),
        ("cms", "CMS"), ("seo_score", "Score SEO"), ("seo_weaknesses", "Faiblesses SEO"),
        ("naf_label", "Activité NAF"), ("domain_age", "Âge domaine"),
        ("has_google_ads", "Google Ads"),
    ]
    parts = []
    for col, label in extra:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() not in ("", "0", "None"):
            if col == "has_google_ads":
                val = "Oui" if str(val) in ("1", "True", "true") else "Non"
            parts.append(f"{label}: {val}")
    return " | ".join(parts)


def _pipedrive_existing(key: str) -> tuple[set[str], set[str]]:
    """Récupère les emails et noms d'orgs existants dans Pipedrive."""
    emails = set()
    orgs = set()
    base = "https://api.pipedrive.com/v1"
    start = 0
    while True:
        try:
            r = httpx.get(f"{base}/persons?api_token={key}&start={start}&limit=100", timeout=15)
            if r.status_code != 200:
                break
            data = r.json()
            for p in data.get("data") or []:
                for e in p.get("email", []):
                    val = e.get("value", "").lower().strip()
                    if val:
                        emails.add(val)
            pagination = data.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break
            start = pagination.get("next_start", start + 100)
        except Exception:
            break
    start = 0
    while True:
        try:
            r = httpx.get(f"{base}/organizations?api_token={key}&start={start}&limit=100", timeout=15)
            if r.status_code != 200:
                break
            data = r.json()
            for o in data.get("data") or []:
                name = (o.get("name") or "").lower().strip()
                if name:
                    orgs.add(name)
            pagination = data.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break
            start = pagination.get("next_start", start + 100)
        except Exception:
            break
    return emails, orgs


def _push_pipedrive(df: pd.DataFrame, progress_cb=None) -> tuple[int, int, list[str]]:
    creds = _get_crm_creds("pipedrive")
    key = creds["pipedrive_api_key"]
    base = "https://api.pipedrive.com/v1"
    ok, fail, skipped, errors = 0, 0, 0, []
    total = len(df)

    existing_emails, existing_orgs = _pipedrive_existing(key)

    for i, (_, row) in enumerate(df.iterrows()):
        email = str(row.get("email", "")).lower().strip() if pd.notna(row.get("email")) else ""
        company = str(row.get("company_name", "")).lower().strip() if pd.notna(row.get("company_name")) else ""

        if (email and email in existing_emails) or (not email and company and company in existing_orgs):
            skipped += 1
            if progress_cb:
                progress_cb((i + 1) / total)
            continue

        org_data = {}
        if pd.notna(row.get("company_name")):
            org_data["name"] = str(row["company_name"])
        if pd.notna(row.get("address")):
            org_data["address"] = str(row["address"])

        org_id = None
        if org_data.get("name"):
            if company not in existing_orgs:
                try:
                    r = httpx.post(
                        f"{base}/organizations?api_token={key}",
                        json=org_data, timeout=15,
                    )
                    if r.status_code in (200, 201) and r.json().get("success"):
                        org_id = r.json()["data"]["id"]
                        existing_orgs.add(company)
                except Exception:
                    pass

        person = {}
        if pd.notna(row.get("owner_name")) and str(row["owner_name"]).strip():
            person["name"] = str(row["owner_name"])
        elif pd.notna(row.get("company_name")) and str(row["company_name"]).strip():
            person["name"] = str(row["company_name"])
        else:
            person["name"] = "Inconnu"
        if email:
            person["email"] = [{"value": email, "primary": True}]
        if pd.notna(row.get("phone")):
            person["phone"] = [{"value": str(row["phone"]), "primary": True}]
        if org_id:
            person["org_id"] = org_id

        notes = _build_notes_text(row)

        try:
            r = httpx.post(
                f"{base}/persons?api_token={key}",
                json=person, timeout=15,
            )
            if r.status_code in (200, 201) and r.json().get("success"):
                ok += 1
                person_id = r.json()["data"]["id"]
                if email:
                    existing_emails.add(email)
                if notes:
                    httpx.post(
                        f"{base}/notes?api_token={key}",
                        json={"content": notes, "person_id": person_id},
                        timeout=15,
                    )
            else:
                fail += 1
                errors.append(f"{row.get('company_name', '?')}: {r.status_code} {r.text[:100]}")
        except Exception as e:
            fail += 1
            errors.append(f"{row.get('company_name', '?')}: {e}")

        if progress_cb:
            progress_cb((i + 1) / total)

    if skipped > 0:
        errors.insert(0, f"{skipped} leads ignorés (déjà présents dans le CRM)")
    return ok, fail, errors


# ── Salesforce ───────────────────────────────────────────────────────────────

def _test_salesforce() -> tuple[bool, str]:
    token, instance = _get_salesforce_token()
    if not token:
        return False, "Token manquant ou expiré — reconnecte Salesforce"
    try:
        r = httpx.get(
            f"{instance}/services/data/v59.0/limits",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if r.status_code == 200:
            return True, f"Connexion OK — {instance.replace('https://', '')}"
        return False, f"Erreur {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def _salesforce_existing(token: str, instance: str) -> tuple[set[str], set[str]]:
    """Récupère emails et noms d'entreprises existants dans Salesforce."""
    emails = set()
    companies = set()
    headers = {"Authorization": f"Bearer {token}"}
    query = "SELECT Email, Company FROM Lead WHERE IsDeleted = false LIMIT 2000"
    try:
        r = httpx.get(
            f"{instance}/services/data/v59.0/query",
            params={"q": query},
            headers=headers,
            timeout=30,
        )
        if r.status_code == 200:
            for record in r.json().get("records", []):
                email = (record.get("Email") or "").lower().strip()
                company = (record.get("Company") or "").lower().strip()
                if email:
                    emails.add(email)
                if company:
                    companies.add(company)
    except Exception:
        pass
    return emails, companies


def _push_salesforce(df: pd.DataFrame, progress_cb=None) -> tuple[int, int, list[str]]:
    token, instance = _get_salesforce_token()
    if not token:
        return 0, len(df), ["Token Salesforce expiré — reconnecte dans Configuration"]

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    ok, fail, skipped, errors = 0, 0, 0, []
    total = len(df)

    existing_emails, existing_companies = _salesforce_existing(token, instance)

    for i, (_, row) in enumerate(df.iterrows()):
        email = str(row.get("email", "")).lower().strip() if pd.notna(row.get("email")) else ""
        company = str(row.get("company_name", "")).lower().strip() if pd.notna(row.get("company_name")) else ""

        if (email and email in existing_emails) or (not email and company and company in existing_companies):
            skipped += 1
            if progress_cb:
                progress_cb((i + 1) / total)
            continue

        lead = {}
        if pd.notna(row.get("company_name")):
            lead["Company"] = str(row["company_name"])
        if pd.notna(row.get("owner_name")):
            lead["LastName"] = str(row["owner_name"])
        if pd.notna(row.get("email")):
            lead["Email"] = str(row["email"])
        if pd.notna(row.get("phone")):
            lead["Phone"] = str(row["phone"])
        if pd.notna(row.get("website_url")):
            lead["Website"] = str(row["website_url"])
        if pd.notna(row.get("address")):
            lead["Street"] = str(row["address"])
        if pd.notna(row.get("city")):
            lead["City"] = str(row["city"])
        if pd.notna(row.get("siege_cp")):
            lead["PostalCode"] = str(row["siege_cp"])
        if pd.notna(row.get("sector")):
            lead["Industry"] = str(row["sector"])
        if pd.notna(row.get("owner_role")):
            lead["Title"] = str(row["owner_role"])

        notes = _build_notes_text(row)
        if notes:
            lead["Description"] = notes

        if not lead.get("LastName"):
            lead["LastName"] = lead.get("Company", "Inconnu")
        if not lead.get("Company"):
            lead["Company"] = "Inconnu"

        try:
            r = httpx.post(
                f"{instance}/services/data/v59.0/sobjects/Lead",
                headers=headers, json=lead, timeout=15,
            )
            if r.status_code in (200, 201):
                ok += 1
                if email:
                    existing_emails.add(email)
                if company:
                    existing_companies.add(company)
            else:
                fail += 1
                errors.append(f"{row.get('company_name', '?')}: {r.status_code} {r.text[:100]}")
        except Exception as e:
            fail += 1
            errors.append(f"{row.get('company_name', '?')}: {e}")

        if progress_cb:
            progress_cb((i + 1) / total)

    if skipped > 0:
        errors.insert(0, f"{skipped} leads ignorés (déjà présents dans le CRM)")
    return ok, fail, errors
