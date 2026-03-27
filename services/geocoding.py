"""
Géocodage et recherche de communes voisines via geo.api.gouv.fr (gratuit, sans clé API).
"""
import math
import httpx

from core.logger import get_logger
log = get_logger(__name__)

_BASE = "https://geo.api.gouv.fr"


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance en km entre deux points GPS."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_city_coordinates(city_name: str) -> dict | None:
    """Retourne {nom, code, departement, lat, lon, population} pour une commune."""
    try:
        r = httpx.get(
            f"{_BASE}/communes",
            params={"nom": city_name, "fields": "nom,code,centre,population,codeDepartement", "limit": 5},
            timeout=10,
        )
        r.raise_for_status()
        results = r.json()
        if not results:
            return None
        best = results[0]
        for c in results:
            if c["nom"].lower() == city_name.lower():
                best = c
                break
        centre = best.get("centre", {}).get("coordinates", [])
        if len(centre) < 2:
            return None
        return {
            "nom": best["nom"],
            "code": best["code"],
            "departement": best.get("codeDepartement", ""),
            "lat": centre[1],
            "lon": centre[0],
            "population": best.get("population", 0),
        }
    except Exception as e:
        log.warning("Géocodage échoué pour '%s' : %s", city_name, e)
        return None


def _get_dept_communes(dept_code: str) -> list[dict]:
    """Récupère toutes les communes d'un département avec leurs coordonnées."""
    try:
        r = httpx.get(
            f"{_BASE}/departements/{dept_code}/communes",
            params={"fields": "nom,code,centre,population"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("Erreur récupération communes dept %s : %s", dept_code, e)
        return []


def _get_neighboring_depts(dept_code: str) -> list[str]:
    """Retourne les codes des départements limitrophes (table statique pour la France métro)."""
    neighbors = {
        "01": ["38", "39", "69", "71", "73", "74"],
        "02": ["08", "51", "59", "60", "77", "80"],
        "03": ["18", "23", "42", "58", "63", "71"],
        "04": ["05", "06", "26", "83", "84"],
        "05": ["04", "26", "38", "73"],
        "06": ["04", "83"],
        "07": ["26", "30", "38", "42", "43", "48"],
        "08": ["02", "51", "55"],
        "09": ["11", "31", "66"],
        "10": ["21", "51", "52", "77", "89"],
        "11": ["09", "31", "34", "66", "81"],
        "12": ["15", "30", "34", "46", "48", "81", "82"],
        "13": ["30", "83", "84"],
        "14": ["27", "50", "61", "76"],
        "15": ["03", "12", "19", "43", "46", "48", "63"],
        "16": ["17", "24", "79", "86", "87"],
        "17": ["16", "33", "79", "85"],
        "18": ["03", "36", "41", "45", "58"],
        "19": ["15", "23", "24", "46", "63", "87"],
        "21": ["10", "39", "52", "58", "70", "71", "89"],
        "22": ["29", "35", "56"],
        "23": ["03", "19", "36", "63", "87"],
        "24": ["16", "19", "33", "46", "47", "87"],
        "25": ["39", "70", "90"],
        "26": ["04", "05", "07", "38", "84"],
        "27": ["14", "28", "60", "76", "78", "95"],
        "28": ["27", "41", "45", "72", "78", "91"],
        "29": ["22", "56"],
        "30": ["07", "12", "13", "34", "48", "84"],
        "31": ["09", "11", "32", "65", "81", "82"],
        "32": ["31", "40", "47", "64", "65", "82"],
        "33": ["17", "24", "40", "47"],
        "34": ["11", "12", "30", "81"],
        "35": ["22", "44", "49", "50", "53", "56"],
        "36": ["18", "23", "37", "41", "86", "87"],
        "37": ["36", "41", "49", "72", "86"],
        "38": ["01", "05", "07", "26", "42", "69", "73"],
        "39": ["01", "21", "25", "71"],
        "40": ["32", "33", "47", "64"],
        "41": ["18", "28", "36", "37", "45", "72"],
        "42": ["03", "07", "38", "43", "63", "69", "71"],
        "43": ["07", "15", "42", "48", "63"],
        "44": ["35", "49", "56", "85"],
        "45": ["18", "28", "41", "77", "89", "91"],
        "46": ["12", "15", "19", "24", "47", "82"],
        "47": ["24", "32", "33", "40", "46", "82"],
        "48": ["07", "12", "15", "30", "43"],
        "49": ["35", "37", "44", "53", "72", "79", "85", "86"],
        "50": ["14", "35", "53"],
        "51": ["02", "08", "10", "52", "55", "77"],
        "52": ["10", "21", "51", "55", "70", "88"],
        "53": ["35", "49", "50", "61", "72"],
        "54": ["55", "57", "67", "88"],
        "55": ["08", "51", "52", "54", "88"],
        "56": ["22", "29", "35", "44"],
        "57": ["54", "67"],
        "58": ["03", "18", "21", "45", "71", "89"],
        "59": ["02", "62", "80"],
        "60": ["02", "27", "76", "77", "80", "95"],
        "61": ["14", "27", "28", "35", "50", "53", "72"],
        "62": ["59", "76", "80"],
        "63": ["03", "15", "19", "23", "42", "43"],
        "64": ["32", "40", "65"],
        "65": ["31", "32", "64"],
        "66": ["09", "11"],
        "67": ["54", "57", "68", "88"],
        "68": ["67", "88", "90"],
        "69": ["01", "38", "42", "71"],
        "70": ["21", "25", "52", "88", "90"],
        "71": ["01", "03", "21", "39", "42", "58", "69"],
        "72": ["28", "37", "41", "49", "53", "61"],
        "73": ["01", "05", "38", "74"],
        "74": ["01", "73"],
        "76": ["14", "27", "60", "62", "80"],
        "77": ["02", "10", "45", "51", "60", "89", "91", "93", "94", "95"],
        "78": ["27", "28", "91", "92", "95"],
        "79": ["16", "17", "49", "85", "86"],
        "80": ["02", "59", "60", "62", "76"],
        "81": ["11", "12", "31", "34", "82"],
        "82": ["12", "31", "32", "46", "47", "81"],
        "83": ["04", "06", "13", "84"],
        "84": ["04", "13", "26", "30", "83"],
        "85": ["17", "44", "49", "79"],
        "86": ["16", "36", "37", "49", "79", "87"],
        "87": ["16", "19", "23", "24", "36", "86"],
        "88": ["52", "54", "55", "67", "68", "70", "90"],
        "89": ["10", "21", "45", "58", "77"],
        "90": ["25", "68", "70", "88"],
        "91": ["28", "45", "77", "78", "92", "94"],
        "92": ["75", "78", "91", "93", "94", "95"],
        "93": ["75", "77", "92", "94", "95"],
        "94": ["75", "77", "91", "92", "93"],
        "95": ["27", "60", "77", "78", "92", "93"],
        "75": ["92", "93", "94"],
        "2A": ["2B"],
        "2B": ["2A"],
    }
    return neighbors.get(dept_code, [])


def find_expansion_cities(city_name: str, max_cities: int = 10, max_radius_km: int = 50) -> list[dict]:
    """
    À partir d'un nom de ville, retourne les communes les plus proches
    triées par distance, en cherchant dans le département et ses voisins.
    """
    coords = get_city_coordinates(city_name)
    if not coords:
        log.warning("Impossible de géolocaliser '%s' — pas d'extension possible", city_name)
        return []

    lat, lon = coords["lat"], coords["lon"]
    dept = coords["departement"]

    # Récupérer communes du département + départements limitrophes
    all_communes = _get_dept_communes(dept)
    for neighbor_dept in _get_neighboring_depts(dept):
        all_communes.extend(_get_dept_communes(neighbor_dept))

    # Calculer distance et filtrer
    cities = []
    seen_codes = {coords["code"]}  # Exclure la ville d'origine
    for c in all_communes:
        code = c.get("code", "")
        if code in seen_codes:
            continue
        seen_codes.add(code)
        centre = c.get("centre", {}).get("coordinates", [])
        if len(centre) < 2:
            continue
        dist = _haversine(lat, lon, centre[1], centre[0])
        if dist > max_radius_km:
            continue
        cities.append({
            "nom": c["nom"],
            "code": code,
            "distance_km": round(dist, 1),
            "population": c.get("population", 0),
        })

    # Trier : distance croissante, puis population décroissante (à distance égale)
    cities.sort(key=lambda x: (x["distance_km"], -x["population"]))
    log.info("Extension '%s' : %d communes trouvées dans un rayon de %d km", city_name, len(cities), max_radius_km)
    return cities[:max_cities]
