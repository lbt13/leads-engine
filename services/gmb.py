import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

import httpx
from config import config
ENDPOINT = "https://serpapi.com/search.json"


def get_place_details(place_id: str) -> dict:
    """
    Appelle SerpAPI Google Maps Place Details pour un place_id donne.
    Retourne un dict avec gmb_confirmed, owner_name, gmb_url, gmb_category.
    Consomme 1 credit SerpAPI par appel.
    """
    if not place_id:
        return _empty()

    params = {
        "engine":   "google_maps",
        "type":     "place",
        "place_id": place_id,
        "hl":       "fr",
        "gl":       "fr",
        "api_key":  config.serpapi_key,
    }

    try:
        r = httpx.get(ENDPOINT, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning("GMB place_details erreur pour %s : %s", place_id, e)
        return _empty()

    place = data.get("place_results", {})
    if not place:
        return _empty()

    # Nom du propriétaire — plusieurs emplacements possibles dans la réponse
    owner = (
        place.get("owner", {}).get("name")
        or _find_owner_in_reviews(place)
        or None
    )

    # URL Google Maps publique de la fiche
    gmb_url = place.get("url") or place.get("website") or None

    # Catégorie principale
    category = None
    cats = place.get("type") or place.get("types")
    if isinstance(cats, list) and cats:
        category = cats[0]
    elif isinstance(cats, str):
        category = cats

    # Téléphone (si pas déjà récupéré par le scraper)
    phone = place.get("phone") or None

    # Adresse complète
    address = place.get("address") or None

    time.sleep(config.delay_serpapi_s)

    return {
        "gmb_confirmed": True,
        "owner_name":    owner,
        "gmb_url":       gmb_url,
        "gmb_category":  category,
        "gmb_phone":     phone,
        "gmb_address":   address,
    }


def enrich_from_maps_result(raw: dict) -> dict:
    """
    Extrait les infos GMB directement depuis un résultat Google Maps brut
    (sans appel supplémentaire — 0 crédit extra).
    Moins complet que get_place_details mais gratuit.
    """
    if not raw:
        return _empty()

    # Le résultat Maps contient déjà owner dans certains cas
    owner = (
        raw.get("owner", {}).get("name") if isinstance(raw.get("owner"), dict)
        else raw.get("owner")
        or None
    )

    return {
        "gmb_confirmed": bool(raw.get("place_id")),
        "owner_name":    owner,
        "gmb_url":       raw.get("url") or raw.get("maps_url") or None,
        "gmb_category":  raw.get("type") or None,
        "gmb_phone":     raw.get("phone") or None,
        "gmb_address":   raw.get("address") or None,
    }


def _find_owner_in_reviews(place: dict) -> str | None:
    """
    Parfois le nom du propriétaire apparaît dans les réponses aux avis.
    On cherche la signature "Réponse du propriétaire".
    """
    reviews = place.get("reviews", [])
    for review in reviews[:5]:
        response = review.get("response", {})
        author = response.get("author_name") or response.get("author")
        if author:
            return author
    return None


def _empty() -> dict:
    return {
        "gmb_confirmed": False,
        "owner_name":    None,
        "gmb_url":       None,
        "gmb_category":  None,
        "gmb_phone":     None,
        "gmb_address":   None,
    }
