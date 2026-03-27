"""
core/crm_filter.py
Chargement des fichiers CRM et filtrage des doublons inter-sessions.
"""

import re
from pathlib import Path
from difflib import SequenceMatcher

import pandas as pd

from core.logger import get_logger
log = get_logger(__name__)

import os as _os
CRM_DIR = Path(_os.environ.get("LEADS_ENGINE_ROOT", str(Path(__file__).parent.parent))) / "crm"

# Variantes possibles des colonnes nom + ville dans les exports de l'outil
NAME_COLS     = ["Entreprise", "company_name", "nom", "name"]
CITY_COLS     = ["Ville", "city", "ville", "Ville siège"]
SIREN_COLS    = ["SIREN", "siren"]
WEBSITE_COLS  = ["Site web", "Site", "website_url", "website", "site", "URL", "url"]
SECTOR_COLS   = ["Secteur", "sector", "secteur"]
OWNER_COLS    = ["Dirigeant", "owner_name", "dirigeant"]
ROLE_COLS     = ["Rôle", "Role", "owner_role", "rôle", "role", "R\u00f4le"]
PHONE_COLS    = ["Téléphone", "Telephone", "phone", "tel", "T\u00e9l\u00e9phone"]
EMAIL_COLS    = ["Email", "email"]
ADDRESS_COLS  = ["Adresse", "address", "adresse"]
RATING_COLS   = ["Note Google", "google_rating", "note"]
REVIEWS_COLS  = ["Avis", "review_count", "avis"]
LEGAL_COLS    = ["Forme juridique", "legal_form"]
EMPLOYEE_COLS = ["Effectif", "employee_range", "effectif"]
CMS_COLS      = ["CMS", "cms"]
HOSTING_COLS  = ["Hébergeur", "Hebergeur", "hosting", "H\u00e9bergeur"]
PAGESPEED_COLS= ["Vitesse mobile", "pagespeed_mobile"]
SEO_SCORE_COLS= ["Score SEO /10", "seo_score"]
SEO_WEAK_COLS = ["Faiblesses SEO", "seo_weaknesses"]
CREATION_COLS = ["Création", "creation_date", "creation", "Cr\u00e9ation"]
ETAT_COLS     = ["État", "etat", "\u00c9tat"]


def _normalize(text: str) -> str:
    if not text:
        return ""
    text = str(text).lower().strip()
    for s in [" sarl", " sas", " eurl", " sa ", " sci ", " sasu"]:
        text = text.replace(s, " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _find_col(df: pd.DataFrame, candidates: list) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def load_crm() -> list[dict]:
    """
    Charge tous les fichiers xlsx/csv du dossier crm/.
    Retourne une liste de dicts {name_n, city_n, source_file}.
    """
    if not CRM_DIR.exists():
        return []

    entries = []
    files = list(CRM_DIR.glob("*.xlsx")) + list(CRM_DIR.glob("*.csv"))

    for f in files:
        try:
            df = pd.read_excel(f) if f.suffix == ".xlsx" else pd.read_csv(f)
            name_col  = _find_col(df, NAME_COLS)
            city_col  = _find_col(df, CITY_COLS)
            siren_col = _find_col(df, SIREN_COLS)

            if not name_col:
                log.warning("CRM %s — colonne nom introuvable, ignoré", f.name)
                continue

            for _, row in df.iterrows():
                name  = str(row.get(name_col)  or "").strip()
                city  = str(row.get(city_col)  or "").strip() if city_col  else ""
                siren = str(row.get(siren_col) or "").strip() if siren_col else ""
                siren = re.sub(r"\D", "", siren)  # garde uniquement les chiffres
                if name:
                    entries.append({
                        "name_n":      _normalize(name),
                        "city_n":      _normalize(city),
                        "siren":       siren if len(siren) == 9 else "",
                        "source_file": f.name,
                    })

            log.info("CRM chargé : %s (%d entrées)", f.name, len(df))
        except Exception as e:
            log.warning("Erreur lecture CRM %s : %s", f.name, e)

    return entries


def is_in_crm(company_name: str, city: str, siren: str = "", crm: list[dict] = None, threshold: float = 0.85) -> bool:
    """
    Retourne True si l'entreprise est déjà présente dans le CRM.
    Priorité : SIREN exact → fallback nom + ville (similarité 85%).
    """
    if not crm:
        return False

    siren_clean = re.sub(r"\D", "", siren or "")
    name_n = _normalize(company_name)
    city_n = _normalize(city)

    for entry in crm:
        # 1. Match SIREN exact (identifiant légal unique — fiabilité maximale)
        if siren_clean and len(siren_clean) == 9 and entry.get("siren") == siren_clean:
            return True

        # 2. Fallback : similarité nom + ville
        if city_n and entry["city_n"] and city_n != entry["city_n"]:
            continue
        ratio = SequenceMatcher(None, name_n, entry["name_n"]).ratio()
        if ratio >= threshold:
            return True

    return False


def filter_against_crm(leads: list, crm: list[dict]) -> tuple[list, int]:
    """
    Filtre une liste de leads bruts contre le CRM.
    Retourne (leads_filtered, nb_exclus).
    """
    if not crm:
        return leads, 0

    filtered, excluded = [], 0
    for lead in leads:
        name  = lead.get("company_name") or ""
        city  = lead.get("city")         or ""
        siren = lead.get("siren")        or ""
        if is_in_crm(name, city, siren, crm):
            log.info("  CRM doublon exclu : %s (%s)", name[:40], city)
            excluded += 1
        else:
            filtered.append(lead)

    return filtered, excluded


def _val(row, col):
    """Retourne la valeur d'une colonne ou None si absente/vide."""
    if col is None:
        return None
    v = row.get(col)
    if v is None or (isinstance(v, float) and v != v):  # NaN
        return None
    return str(v).strip() or None


def parse_crm_file(filepath: Path) -> list[dict]:
    """
    Parse un fichier CRM depuis le disque.
    Retourne une liste de dicts avec tous les champs disponibles.
    """
    try:
        df = pd.read_excel(filepath) if filepath.suffix == ".xlsx" else pd.read_csv(filepath)
    except Exception as e:
        log.warning("parse_crm_file %s : %s", filepath.name, e)
        return []

    # Détection de toutes les colonnes utiles
    cols = {
        "name":       _find_col(df, NAME_COLS),
        "city":       _find_col(df, CITY_COLS),
        "siren":      _find_col(df, SIREN_COLS),
        "website":    _find_col(df, WEBSITE_COLS),
        "sector":     _find_col(df, SECTOR_COLS),
        "owner":      _find_col(df, OWNER_COLS),
        "role":       _find_col(df, ROLE_COLS),
        "phone":      _find_col(df, PHONE_COLS),
        "email":      _find_col(df, EMAIL_COLS),
        "address":    _find_col(df, ADDRESS_COLS),
        "rating":     _find_col(df, RATING_COLS),
        "reviews":    _find_col(df, REVIEWS_COLS),
        "legal":      _find_col(df, LEGAL_COLS),
        "employee":   _find_col(df, EMPLOYEE_COLS),
        "cms":        _find_col(df, CMS_COLS),
        "hosting":    _find_col(df, HOSTING_COLS),
        "pagespeed":  _find_col(df, PAGESPEED_COLS),
        "seo_score":  _find_col(df, SEO_SCORE_COLS),
        "seo_weak":   _find_col(df, SEO_WEAK_COLS),
        "creation":   _find_col(df, CREATION_COLS),
        "etat":       _find_col(df, ETAT_COLS),
    }

    if not cols["name"]:
        return []

    rows = []
    for _, row in df.iterrows():
        name = _val(row, cols["name"])
        if not name:
            continue
        rows.append({
            "name":      name,
            "city":      _val(row, cols["city"])     or "",
            "siren":     _val(row, cols["siren"])    or "",
            "website":   _val(row, cols["website"]),
            "sector":    _val(row, cols["sector"])   or "",
            "owner":     _val(row, cols["owner"]),
            "role":      _val(row, cols["role"]),
            "phone":     _val(row, cols["phone"]),
            "email":     _val(row, cols["email"]),
            "address":   _val(row, cols["address"]),
            "rating":    _val(row, cols["rating"]),
            "reviews":   _val(row, cols["reviews"]),
            "legal":     _val(row, cols["legal"]),
            "employee":  _val(row, cols["employee"]),
            "cms":       _val(row, cols["cms"]),
            "hosting":   _val(row, cols["hosting"]),
            "pagespeed": _val(row, cols["pagespeed"]),
            "seo_score": _val(row, cols["seo_score"]),
            "seo_weak":  _val(row, cols["seo_weak"]),
            "creation":  _val(row, cols["creation"]),
            "etat":      _val(row, cols["etat"]),
        })
    return rows


def compare_against_crm(file_obj, filename: str) -> dict:
    """
    Compare un fichier contre la base CRM existante sans l'y ajouter.
    Retourne {total, doublons: [{name, city, siren}], nouveaux: [{name, city, siren}]}
    ou {error: str} en cas de problème de lecture.
    """
    try:
        df = pd.read_excel(file_obj) if filename.endswith(".xlsx") else pd.read_csv(file_obj)
    except Exception as e:
        return {"error": str(e), "total": 0, "doublons": [], "nouveaux": []}

    name_col  = _find_col(df, NAME_COLS)
    city_col  = _find_col(df, CITY_COLS)
    siren_col = _find_col(df, SIREN_COLS)

    if not name_col:
        return {
            "error": "Colonne nom introuvable — attendu : Entreprise, company_name, nom ou name",
            "total": 0, "doublons": [], "nouveaux": [],
        }

    crm = load_crm()
    doublons, nouveaux = [], []

    for _, row in df.iterrows():
        name  = str(row.get(name_col)  or "").strip()
        city  = str(row.get(city_col)  or "").strip() if city_col  else ""
        siren = str(row.get(siren_col) or "").strip() if siren_col else ""
        if not name:
            continue
        entry = {"Entreprise": name, "Ville": city, "SIREN": siren}
        if is_in_crm(name, city, siren, crm):
            doublons.append(entry)
        else:
            nouveaux.append(entry)

    return {
        "total":    len(doublons) + len(nouveaux),
        "doublons": doublons,
        "nouveaux": nouveaux,
    }


def crm_stats() -> dict:
    """Stats rapides sur le contenu du dossier CRM."""
    if not CRM_DIR.exists():
        return {"fichiers": 0, "entreprises": 0, "liste": []}

    files = list(CRM_DIR.glob("*.xlsx")) + list(CRM_DIR.glob("*.csv"))
    total = 0
    liste = []
    for f in files:
        try:
            df = pd.read_excel(f) if f.suffix == ".xlsx" else pd.read_csv(f)
            n = len(df)
            total += n
            liste.append({"nom": f.name, "nb": n, "path": str(f)})
        except Exception:
            log.warning("Lecture CRM stats echouee pour '%s'", f.name, exc_info=True)
            liste.append({"nom": f.name, "nb": 0, "path": str(f)})

    return {"fichiers": len(files), "entreprises": total, "liste": liste}
