"""
services/pagespeed.py
Score PageSpeed via l'API Google PageSpeed Insights.
Gratuit sans clé : 25 000 requetes/jour.
Avec cle Google (gratuite) : 400 req/minute.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.logger import get_logger
log = get_logger(__name__)

import httpx
from config import config

ENDPOINT  = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
MAX_RETRY = 3
BACKOFF   = [2, 5, 10]  # secondes entre chaque tentative


def get_pagespeed(url: str) -> dict:
    """
    Retourne les scores PageSpeed mobile et desktop.
    Retry automatique avec backoff exponentiel sur les 429.
    Retourne {"mobile": None, "desktop": None} en cas d'echec définitif.
    """
    result = {"mobile": None, "desktop": None}

    for strategy in ("mobile", "desktop"):
        params = {
            "url":      url,
            "strategy": strategy,
            "category": "performance",
        }
        if config.pagespeed_key:
            params["key"] = config.pagespeed_key

        for attempt in range(MAX_RETRY):
            try:
                r = httpx.get(ENDPOINT, params=params, timeout=30)

                if r.status_code == 429:
                    wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]
                    log.warning("PageSpeed 429 (%s) — retry dans %ds", strategy, wait)
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                data  = r.json()
                score = (data.get("lighthouseResult", {})
                             .get("categories", {})
                             .get("performance", {})
                             .get("score"))
                if score is not None:
                    result[strategy] = int(score * 100)
                    log.debug("PageSpeed %s %s : %d/100", strategy, url, result[strategy])
                break

            except httpx.TimeoutException:
                log.warning("PageSpeed timeout (%s) : %s", strategy, url)
                break
            except Exception as e:
                log.warning("PageSpeed erreur (%s) : %s", strategy, e)
                break

    return result
