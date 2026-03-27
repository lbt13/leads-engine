import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

import httpx
from config import config
ENDPOINT = "https://serpapi.com/search.json"


def search_google_maps(query: str, location: str, max_results: int = None):
    max_results = max_results or config.max_results_per_query
    start, fetched = 0, 0
    while fetched < max_results:
        params = {
            "engine":  "google_maps",
            "q":       f"{query} {location}",
            "type":    "search",
            "hl":      "fr",
            "gl":      "fr",
            "start":   start,
            "api_key": config.serpapi_key,
        }
        try:
            r = httpx.get(ENDPOINT, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error("SerpAPI erreur : %s", e)
            break
        results = data.get("local_results", [])
        if not results:
            break
        for item in results:
            if fetched >= max_results:
                return
            yield item
            fetched += 1
        if len(results) < 20:
            break
        start += 20
        time.sleep(config.delay_serpapi_s)


def parse_maps_result(raw: dict):
    name = raw.get("title", "").strip()
    if not name:
        return None
    website = (
        raw.get("website")
        or raw.get("links", {}).get("website")
        or ""
    ).strip().rstrip("/")
    address = raw.get("address", "")
    city = address.split(",")[-1].strip() if address else ""
    return {
        "company_name":  name,
        "address":       address,
        "city":          city,
        "phone":         raw.get("phone", ""),
        "google_rating": raw.get("rating"),
        "review_count":  raw.get("reviews"),
        "website_url":   website or None,
        "source":        "google_maps",
    }
