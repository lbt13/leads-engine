import sys
import time
import re
from pathlib import Path
from difflib import SequenceMatcher

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

import httpx

# API officielle gouvernementale — gratuite, sans clé, données SIRENE + Infogreffe
API_RECHERCHE = "https://recherche-entreprises.api.gouv.fr/search"
API_DETAILS   = "https://recherche-entreprises.api.gouv.fr/id/{siren}"

_postal_cache: dict[str, str | None] = {}


def _city_to_postal(city: str) -> str | None:
    """Convertit un nom de ville en code postal via geo.api.gouv.fr (avec cache)."""
    key = city.strip().lower()
    if key in _postal_cache:
        return _postal_cache[key]
    # Si la ville est déjà un code postal (5 chiffres), le retourner tel quel
    clean = re.sub(r"\D", "", city)
    if len(clean) == 5:
        _postal_cache[key] = clean
        return clean
    try:
        r = httpx.get(
            "https://geo.api.gouv.fr/communes",
            params={"nom": city.strip(), "fields": "codesPostaux,nom", "limit": 5},
            timeout=8,
        )
        r.raise_for_status()
        results = r.json()
        if results:
            for c in results:
                if c["nom"].lower() == key:
                    cp = c.get("codesPostaux", [""])[0]
                    _postal_cache[key] = cp
                    return cp
            cp = results[0].get("codesPostaux", [""])[0]
            _postal_cache[key] = cp
            return cp
    except Exception:
        log.warning("Conversion ville→code postal echouee pour '%s'", city, exc_info=True)
    _postal_cache[key] = None
    return None


def find_dirigeant(company_name: str, city: str = "") -> dict:
    """
    Recherche le dirigeant officiel d'une entreprise dans le registre national.

    Stratégie en 3 passes :
      1. Recherche par nom + ville → score de similarité → meilleur match
      2. Si pas de dirigeant trouvé → recherche par nom seul (plus large)
      3. Si SIREN trouvé → appel détails pour dirigeants complets

    Retourne un dict avec :
        owner_name   : prénom + nom du dirigeant principal
        owner_role   : gérant / président / directeur général / etc.
        siren        : numéro SIREN (9 chiffres)
        siret        : numéro SIRET (14 chiffres) — établissement principal
        legal_form   : forme juridique (SARL, SAS, EI, etc.)
        naf_code     : code NAF / APE
        naf_label    : libellé de l'activité
        employee_range: tranche d'effectif
        creation_date: date de création
        found        : bool — correspondance trouvée
    """
    result = _empty()

    # ── Passe 1 : recherche nom + ville ──────────────────────────────────────
    candidates = _search(company_name, city)

    # ── Passe 2 : si aucun résultat, recherche nom seul ─────────────────────
    if not candidates:
        log.debug("Aucun résultat avec ville, retry sans ville : %s", company_name)
        candidates = _search(company_name, "")

    if not candidates:
        log.debug("Aucune entreprise trouvée pour : %s", company_name)
        return result

    # ── Sélection du meilleur match par similarité de nom ───────────────────
    best = _best_match(company_name, city, candidates)
    if not best:
        return result

    result["found"]        = True
    result["siren"]        = best.get("siren", "")
    result["siret"]        = _get_siret_principal(best)
    result["legal_form"]   = _get_legal_form(best)
    result["naf_code"]     = best.get("activite_principale", "")
    result["naf_label"]    = best.get("libelle_activite_principale", "")
    result["creation_date"]= best.get("date_creation", "")
    result["employee_range"]= _get_tranche_effectif(best)

    # ── Extraction des dirigeants ────────────────────────────────────────────
    dirigeants = best.get("dirigeants", [])

    # Si pas de dirigeants dans la recherche, appel détails avec le SIREN
    if not dirigeants and result["siren"]:
        time.sleep(0.3)
        dirigeants = _fetch_dirigeants(result["siren"])

    if dirigeants:
        principal = _select_principal(dirigeants)
        if principal:
            prenom = principal.get("prenom", "").strip().title()
            nom    = principal.get("nom", "").strip().upper()
            result["owner_name"] = f"{prenom} {nom}".strip() or None
            result["owner_role"] = _clean_role(principal.get("qualite", ""))

    time.sleep(0.2)  # respecte le rate limit de l'API
    return result


def _search(name: str, city: str) -> list:
    """Appelle l'API de recherche et retourne la liste des entreprises."""
    params = {
        "q":        name,
        "page":     1,
        "per_page": 5,
    }
    if city:
        # L'API attend un code postal ou code commune, pas un nom de ville.
        # On résout le code postal via geo.api.gouv.fr.
        cp = _city_to_postal(city)
        if cp:
            params["code_postal"] = cp

    try:
        r = httpx.get(API_RECHERCHE, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])
    except Exception as e:
        log.warning("API entreprises erreur : %s", e)
        return []


def _fetch_dirigeants(siren: str) -> list:
    """Appelle l'endpoint détails pour récupérer les dirigeants complets."""
    try:
        r = httpx.get(API_DETAILS.format(siren=siren), timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("dirigeants", [])
    except Exception as e:
        log.warning("Détails SIREN %s : %s", siren, e)
        return []


def _best_match(name: str, city: str, candidates: list) -> dict | None:
    """
    Sélectionne l'entreprise candidate dont le nom est le plus proche.
    Seuil minimum de similarité : 0.45 (assez souple pour les noms abrégés).
    """
    name_n = _normalize(name)
    city_n = _normalize(city)
    best_score = 0.0
    best = None

    for c in candidates:
        # Nom de l'entreprise dans l'API
        nom_api = c.get("nom_complet") or c.get("nom_raison_sociale") or ""
        score = SequenceMatcher(None, name_n, _normalize(nom_api)).ratio()

        # Bonus si la ville correspond
        matching_city = any(
            city_n in _normalize(siege.get("libelle_commune", ""))
            for siege in [c.get("siege", {})]
            if city_n
        )
        if matching_city:
            score += 0.15

        if score > best_score:
            best_score = score
            best = c

    if best_score < 0.45:
        log.debug(
            "Meilleur match trop faible (%.2f) pour '%s' — ignoré", best_score, name
        )
        return None

    log.debug("Match trouvé : '%s' (score %.2f)", best.get("nom_complet", ""), best_score)
    return best


def _select_principal(dirigeants: list) -> dict | None:
    """
    Parmi la liste des dirigeants, retourne le plus pertinent.
    Priorité : Gérant > Président > Directeur Général > premier de la liste.
    """
    priority = ["gérant", "gérante", "président", "présidente",
                "directeur général", "directrice générale", "associé gérant"]

    for role_cible in priority:
        for d in dirigeants:
            role = (d.get("qualite") or "").lower()
            if role_cible in role:
                return d

    # Aucun rôle prioritaire → premier dirigeant de la liste
    return dirigeants[0] if dirigeants else None


# ── Helpers de nettoyage ──────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_role(role: str) -> str:
    if not role:
        return ""
    return role.strip().capitalize()


def _get_siret_principal(company: dict) -> str:
    siege = company.get("siege", {})
    return siege.get("siret", "")


def _get_legal_form(company: dict) -> str:
    forme = company.get("nature_juridique", "")
    libelles = {
        "5710": "SAS", "5499": "SARL", "1000": "Entrepreneur individuel",
        "5720": "SASU", "5306": "EURL", "6540": "SA", "9220": "Association",
    }
    return libelles.get(forme, forme)


def _get_tranche_effectif(company: dict) -> str:
    tranches = {
        "00": "0 salarié", "01": "1-2", "02": "3-5", "03": "6-9",
        "11": "10-19",    "12": "20-49", "21": "50-99", "22": "100-199",
        "31": "200-249",  "32": "250-499",
    }
    code = company.get("tranche_effectif_salarie", "") or ""
    return tranches.get(code, code)


def _empty() -> dict:
    return {
        "found":         False,
        "owner_name":    None,
        "owner_role":    None,
        "siren":         None,
        "siret":         None,
        "legal_form":    None,
        "naf_code":      None,
        "naf_label":     None,
        "employee_range": None,
        "creation_date": None,
    }


# ── Test autonome ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    tests = [
        ("Plomberie Dupont", "Marseille"),
        ("Électricité Martin", "Aix-en-Provence"),
        ("Bouygues Construction", "Paris"),
    ]

    for name, city in tests:
        print(f"\n--- {name} ({city}) ---")
        r = find_dirigeant(name, city)
        if r["found"]:
            print(f"  Dirigeant  : {r['owner_name']} ({r['owner_role']})")
            print(f"  SIREN      : {r['siren']}")
            print(f"  Forme      : {r['legal_form']}")
            print(f"  Effectif   : {r['employee_range']}")
            print(f"  Créée le   : {r['creation_date']}")
        else:
            print("  Non trouvé dans le registre national")
