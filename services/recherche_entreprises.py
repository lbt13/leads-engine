"""
Recherche d'entreprises par secteur + localisation via l'API gouvernementale
recherche-entreprises.api.gouv.fr (gratuite, sans clé, licence ouverte Etalab).

Remplace le scraping Pages Jaunes comme seconde source de leads.
"""
import sys
import time
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

import httpx

API_SEARCH = "https://recherche-entreprises.api.gouv.fr/search"

# ── Table de correspondance secteur courant → codes NAF ────────────────────
# Chaque entrée mappe un mot-clé métier vers les codes NAF les plus pertinents.
# Source : nomenclature NAF Rév. 2 (INSEE).
NAF_MAP = {
    # BTP / Second œuvre
    "plombier":         ["43.22A"],
    "plomberie":        ["43.22A"],
    "chauffagiste":     ["43.22A", "43.22B"],
    "électricien":      ["43.21A"],
    "electricien":      ["43.21A"],
    "électricité":      ["43.21A"],
    "electricite":      ["43.21A"],
    "peintre":          ["43.34A"],
    "peinture":         ["43.34A"],
    "carreleur":        ["43.33Z"],
    "carrelage":        ["43.33Z"],
    "maçon":            ["43.99A"],
    "macon":            ["43.99A"],
    "maçonnerie":       ["43.99A"],
    "maconnerie":       ["43.99A"],
    "couvreur":         ["43.91A"],
    "couverture":       ["43.91A"],
    "charpentier":      ["43.91B"],
    "menuisier":        ["43.32A"],
    "menuiserie":       ["43.32A"],
    "serrurier":        ["43.32B"],
    "serrurerie":       ["43.32B"],
    "vitrier":          ["43.34B"],
    "isolation":        ["43.29A"],
    "isolateur":        ["43.29A"],
    "climatisation":    ["43.22B"],
    "climaticien":      ["43.22B"],
    "terrassement":     ["43.12A"],
    "terrassier":       ["43.12A"],
    "démolition":       ["43.11Z"],
    "demolition":       ["43.11Z"],
    "construction":     ["41.20A", "41.20B"],
    "bâtiment":         ["41.20A", "41.20B"],
    "batiment":         ["41.20A", "41.20B"],
    "rénovation":       ["43.39Z"],
    "renovation":       ["43.39Z"],
    "ravalement":       ["43.34A"],
    "façadier":         ["43.34A"],
    "facadier":         ["43.34A"],
    "parqueteur":       ["43.33Z"],
    "plâtrier":         ["43.31Z"],
    "platrier":         ["43.31Z"],
    "plaquiste":        ["43.31Z"],

    # Espaces verts / Paysage
    "paysagiste":       ["81.30Z"],
    "jardinier":        ["81.30Z"],
    "entretien jardin": ["81.30Z"],
    "élagage":          ["02.10Z"],
    "elagage":          ["02.10Z"],

    # Automobile
    "garagiste":        ["45.20A"],
    "garage":           ["45.20A"],
    "mécanique auto":   ["45.20A"],
    "mecanique auto":   ["45.20A"],
    "carrossier":       ["45.20B"],
    "carrosserie":      ["45.20B"],
    "contrôle technique": ["71.20A"],
    "controle technique": ["71.20A"],

    # Alimentaire / Restauration
    "boulanger":        ["10.71C"],
    "boulangerie":      ["10.71C"],
    "pâtissier":        ["10.71D"],
    "patissier":        ["10.71D"],
    "pâtisserie":       ["10.71D"],
    "patisserie":       ["10.71D"],
    "restaurant":       ["56.10A"],
    "restauration":     ["56.10A"],
    "traiteur":         ["56.21Z"],
    "boucher":          ["10.11Z"],
    "boucherie":        ["47.22Z"],
    "charcutier":       ["10.13A"],
    "charcuterie":      ["10.13A"],
    "pizzeria":         ["56.10A"],
    "kebab":            ["56.10A"],
    "fast food":        ["56.10C"],
    "snack":            ["56.10C"],
    "café":             ["56.30Z"],
    "cafe":             ["56.30Z"],
    "bar":              ["56.30Z"],

    # Santé / Bien-être
    "dentiste":         ["86.23Z"],
    "médecin":          ["86.21Z"],
    "medecin":          ["86.21Z"],
    "pharmacie":        ["47.73Z"],
    "kinésithérapeute": ["86.90A"],
    "kinesitherapeute": ["86.90A"],
    "kiné":             ["86.90A"],
    "kine":             ["86.90A"],
    "ostéopathe":       ["86.90A"],
    "osteopathe":       ["86.90A"],
    "opticien":         ["47.78A"],
    "coiffeur":         ["96.02A"],
    "coiffure":         ["96.02A"],
    "esthéticienne":    ["96.02B"],
    "estheticienne":    ["96.02B"],
    "institut beauté":  ["96.02B"],
    "institut beaute":  ["96.02B"],

    # Immobilier
    "agent immobilier": ["68.31Z"],
    "agence immobilière": ["68.31Z"],
    "agence immobiliere": ["68.31Z"],
    "immobilier":       ["68.31Z"],

    # Commerce
    "fleuriste":        ["47.76Z"],
    "pressing":         ["96.01A"],
    "nettoyage":        ["81.21Z"],
    "déménageur":       ["49.42Z"],
    "demenageur":       ["49.42Z"],
    "déménagement":     ["49.42Z"],
    "demenagement":     ["49.42Z"],

    # Services aux entreprises
    "comptable":        ["69.20Z"],
    "expert-comptable": ["69.20Z"],
    "expert comptable": ["69.20Z"],
    "avocat":           ["69.10Z"],
    "notaire":          ["69.10Z"],
    "architecte":       ["71.11Z"],
    "géomètre":         ["71.12A"],
    "geometre":         ["71.12A"],

    # Informatique / Web
    "informatique":     ["62.01Z", "62.02A"],
    "développeur":      ["62.01Z"],
    "developpeur":      ["62.01Z"],
    "agence web":       ["62.01Z"],
    "webdesign":        ["62.01Z"],
    "infogérance":      ["62.03Z"],
    "infogerance":      ["62.03Z"],
    "dépannage informatique": ["95.11Z"],
    "depannage informatique": ["95.11Z"],

    # Transport
    "taxi":             ["49.32Z"],
    "vtc":              ["49.32Z"],
    "transport routier": ["49.41A", "49.41B"],
    "coursier":         ["53.20Z"],
    "livraison":        ["53.20Z"],

    # Divers
    "photographe":      ["74.20Z"],
    "photographie":     ["74.20Z"],
    "imprimerie":       ["18.12Z"],
    "formation":        ["85.59A"],
    "auto-école":       ["85.53Z"],
    "auto ecole":       ["85.53Z"],
    "sécurité":         ["80.10Z"],
    "securite":         ["80.10Z"],
    "gardiennage":      ["80.10Z"],
}


def _resolve_naf_codes(sector: str) -> list[str]:
    """Résout un mot-clé secteur en codes NAF. Cherche par correspondance exacte puis partielle."""
    key = sector.lower().strip()

    # Match exact
    if key in NAF_MAP:
        return NAF_MAP[key]

    # Match partiel : le mot-clé est contenu dans une clé de la table
    for naf_key, codes in NAF_MAP.items():
        if key in naf_key or naf_key in key:
            return codes

    return []


def _get_code_postal(city_name: str) -> str | None:
    """Récupère le code postal principal d'une commune via geo.api.gouv.fr."""
    try:
        r = httpx.get(
            "https://geo.api.gouv.fr/communes",
            params={"nom": city_name, "fields": "codesPostaux,nom", "limit": 5},
            timeout=10,
        )
        r.raise_for_status()
        results = r.json()
        if not results:
            return None
        # Préférer la correspondance exacte
        for c in results:
            if c["nom"].lower() == city_name.lower():
                cps = c.get("codesPostaux", [])
                return cps[0] if cps else None
        cps = results[0].get("codesPostaux", [])
        return cps[0] if cps else None
    except Exception as e:
        log.warning("Code postal introuvable pour '%s' : %s", city_name, e)
        return None


def search_entreprises(sector: str, city: str, max_results: int = 25) -> list[dict]:
    """
    Recherche des entreprises actives par secteur + ville.
    Retourne une liste de dicts au même format que les résultats Google Maps / Pages Jaunes.
    """
    naf_codes = _resolve_naf_codes(sector)

    results = []
    page = 1
    per_page = 25

    while len(results) < max_results:
        params = {
            "q":                     sector if not naf_codes else "",
            "etat_administratif":    "A",    # Entreprises actives uniquement
            "per_page":              per_page,
            "page":                  page,
        }

        # Filtrer par code NAF si on en a trouvé
        if naf_codes:
            params["activite_principale"] = ",".join(naf_codes)

        # Localisation : code postal pour la précision
        cp = _get_code_postal(city)
        if cp:
            params["code_postal"] = cp
        else:
            # Fallback : recherche textuelle avec la ville dans q
            params["q"] = f"{sector} {city}" if not naf_codes else ""
            if naf_codes:
                # On ne peut pas filtrer par ville sans code postal,
                # on ajoute le département si possible
                pass

        try:
            r = httpx.get(API_SEARCH, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error("API Recherche Entreprises erreur : %s", e)
            break

        entreprises = data.get("results", [])
        if not entreprises:
            break

        total = data.get("total_results", 0)
        log.info("API Entreprises p%d : %d résultats (total %d)", page, len(entreprises), total)

        for ent in entreprises:
            if len(results) >= max_results:
                break
            parsed = _parse_entreprise(ent, city)
            if parsed:
                results.append(parsed)

        if page * per_page >= total:
            break

        page += 1
        time.sleep(0.15)  # Respecte le rate limit (7 req/s)

    log.info("API Entreprises : %d leads pour '%s @ %s'", len(results), sector, city)
    return results


def _parse_entreprise(ent: dict, search_city: str) -> dict | None:
    """Convertit une entreprise de l'API en dict au format standard du scraper."""
    nom = ent.get("nom_complet") or ent.get("nom_raison_sociale") or ""
    if not nom:
        return None

    # Adresse depuis le siège ou l'établissement correspondant
    siege = ent.get("siege", {})
    matching = ent.get("matching_etablissements", [])

    # Préférer l'établissement qui match la recherche (si différent du siège)
    etab = matching[0] if matching else siege

    adresse_parts = []
    for field in ["numero_voie", "type_voie", "libelle_voie"]:
        val = etab.get(field)
        if val:
            adresse_parts.append(str(val))
    adresse_ligne = " ".join(adresse_parts)

    cp = etab.get("code_postal", "")
    ville = etab.get("libelle_commune", "")
    address = f"{adresse_ligne}, {cp} {ville}".strip(", ")

    # Dirigeants
    dirigeants = ent.get("dirigeants", [])
    owner_name = None
    owner_role = None
    all_owners = None
    if dirigeants:
        principal = dirigeants[0]
        prenom = (principal.get("prenom") or "").strip().title()
        nom_dir = (principal.get("nom") or "").strip().upper()
        owner_name = f"{prenom} {nom_dir}".strip() or None
        owner_role = (principal.get("qualite") or "").strip().capitalize() or None
        if len(dirigeants) > 1:
            all_owners = " | ".join(
                f"{(d.get('prenom') or '').strip().title()} {(d.get('nom') or '').strip().upper()} ({(d.get('qualite') or '').strip()})"
                for d in dirigeants[:5]
            )

    # Données financières
    finances = ent.get("finances", {})
    ca = finances.get("ca") if finances else None

    # Forme juridique
    nature_jur = ent.get("nature_juridique", "")
    legal_forms = {
        "5710": "SAS", "5499": "SARL", "1000": "Entrepreneur individuel",
        "5720": "SASU", "5306": "EURL", "6540": "SA", "9220": "Association",
        "5498": "SARL unipersonnelle", "6599": "SCI",
    }

    # Tranche effectif
    tranches = {
        "00": "0 salarié", "01": "1-2", "02": "3-5", "03": "6-9",
        "11": "10-19", "12": "20-49", "21": "50-99", "22": "100-199",
        "31": "200-249", "32": "250-499", "41": "500-999",
    }
    tranche_code = ent.get("tranche_effectif_salarie", "") or ""

    return {
        "company_name":    nom,
        "address":         address,
        "city":            ville or search_city,
        "phone":           None,    # L'API ne fournit pas de téléphone
        "google_rating":   None,
        "review_count":    None,
        "website_url":     None,    # L'API ne fournit pas de site web
        "source":          "registre_national",
        # Données enrichies directement disponibles
        "siren":           ent.get("siren"),
        "siret":           etab.get("siret"),
        "legal_form":      legal_forms.get(nature_jur, nature_jur),
        "legal_form_code": nature_jur,
        "naf_code":        ent.get("activite_principale"),
        "naf_label":       ent.get("libelle_activite_principale"),
        "employee_range":  tranches.get(tranche_code, tranche_code),
        "employee_code":   tranche_code,
        "creation_date":   ent.get("date_creation"),
        "etat":            "Active" if ent.get("etat_administratif") == "A" else "Cessée",
        "owner_name":      owner_name,
        "owner_role":      owner_role,
        "all_owners":      all_owners,
        "siege_adresse":   f"{siege.get('numero_voie', '')} {siege.get('type_voie', '')} {siege.get('libelle_voie', '')}".strip(),
        "siege_cp":        siege.get("code_postal"),
        "siege_ville":     siege.get("libelle_commune"),
        "nb_etablissements": ent.get("nombre_etablissements"),
        "nb_etab_ouverts":   ent.get("nombre_etablissements_ouverts"),
        "date_mise_a_jour":  ent.get("date_mise_a_jour"),
        "section_activite":  ent.get("section_activite_principale"),
    }
